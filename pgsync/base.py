"""PGSync Base."""

import logging
import os
import random
import threading
import time
import typing as t
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql  # noqa
from sqlalchemy.orm import sessionmaker

from .constants import (
    BUILTIN_SCHEMAS,
    DEFAULT_SCHEMA,
    DELETE,
    LOGICAL_SLOT_PREFIX,
    LOGICAL_SLOT_SUFFIX,
    MATERIALIZED_VIEW,
    PLUGIN,
    TG_OPS,
    TRIGGER_FUNC,
    UPDATE,
)
from .exc import (
    LogicalSlotParseError,
    ReplicationSlotError,
    TableNotFoundError,
)
from .settings import (
    IS_MYSQL_COMPAT,
    MYSQL_DATABASE,
    PG_DATABASE,
    PG_HOST_RO,
    PG_PASSWORD_RO,
    PG_PORT_RO,
    PG_SSLMODE,
    PG_SSLROOTCERT,
    PG_URL_RO,
    PG_USER_RO,
    QUERY_CHUNK_SIZE,
    SQLALCHEMY_MAX_OVERFLOW,
    SQLALCHEMY_POOL_PRE_PING,
    SQLALCHEMY_POOL_RECYCLE,
    SQLALCHEMY_POOL_SIZE,
    SQLALCHEMY_POOL_TIMEOUT,
    SQLALCHEMY_USE_NULLPOOL,
    STREAM_RESULTS,
)
from .trigger import CREATE_TRIGGER_TEMPLATE
from .urls import get_database_url
from .utils import compiled_query, qname
from .view import create_view, DropView, is_view, RefreshView

try:
    import citext  # noqa
except ImportError:
    pass

try:
    import geoalchemy2  # noqa
except ImportError:
    pass

logger = logging.getLogger(__name__)

SSL_MODES = (
    "allow",
    "disable",
    "prefer",
    "require",
    "verify-ca",
    "verify-full",
)


class Payload(object):
    """
    Represents a payload object that contains information about a database change event.

    Attributes:
        tg_op (str): The type of operation that triggered the event (e.g. INSERT, UPDATE, DELETE).
        table (str): The name of the table that was affected by the event.
        schema (str): The name of the schema that contains the table.
        old (dict): The old values of the row that was affected by the event (for UPDATE and DELETE operations).
        new (dict): The new values of the row that was affected by the event (for INSERT and UPDATE operations).
        xmin (int): The transaction ID of the event.
        indices (List[str]): The indices of the affected rows (for UPDATE and DELETE operations).
    """

    __slots__ = ("tg_op", "table", "schema", "old", "new", "xmin", "indices")

    def __init__(
        self,
        tg_op: t.Optional[str] = None,
        table: t.Optional[str] = None,
        schema: t.Optional[str] = None,
        old: t.Optional[t.Dict[str, t.Any]] = None,
        new: t.Optional[t.Dict[str, t.Any]] = None,
        xmin: t.Optional[int] = None,
        indices: t.Optional[t.List[str]] = None,
    ):
        self.tg_op: t.Optional[str] = tg_op
        self.table: t.Optional[str] = table
        self.schema: t.Optional[str] = schema
        self.old: t.Dict[str, t.Any] = old or {}
        self.new: t.Dict[str, t.Any] = new or {}
        self.xmin: t.Optional[int] = xmin
        self.indices: t.List[str] = indices

    @property
    def data(self) -> dict:
        """Extract the payload data from the payload."""
        if self.tg_op == DELETE and self.old:
            return self.old
        return self.new

    def foreign_key_constraint(self, model) -> dict:
        """
        {
            'public.customer': {  referred table with a fully qualified name
                'local': 'customer_id',
                'remote': 'id',
                'value': 1
            },
            'public.group': {  referred table with a fully qualified name
                'local': 'group_id',
                'remote': 'id',
                'value': 1
            }
        }
        """
        constraints: dict = {}
        for foreign_key in model.foreign_keys:
            referred_table: str = str(foreign_key.constraint.referred_table)
            constraints.setdefault(referred_table, {})
            if foreign_key.constraint.column_keys:
                if foreign_key.constraint.column_keys[0] in self.data:
                    constraints[referred_table] = {
                        "local": foreign_key.constraint.column_keys[0],
                        "remote": foreign_key.column.name,
                        "value": self.data[
                            foreign_key.constraint.column_keys[0]
                        ],
                    }
        return constraints


class TupleIdentifierType(sa.types.UserDefinedType):
    cache_ok: bool = True

    def get_col_spec(self, **kwargs) -> str:
        return "TID"

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process


class Base(object):
    _thread_local = threading.local()

    INT_TYPES = (
        "bigint",
        "bigserial",
        "int",
        "int2",
        "int4",
        "int8",
        "integer",
        "serial",
        "serial2",
        "serial4",
        "serial8",
        "smallint",
        "smallserial",
    )
    FLOAT_TYPES = (
        "double precision",
        "float4",
        "float8",
        "real",
    )
    CHAR_TYPES = (
        "char",
        "character",
        "character varying",
        "text",
        "uuid",
        "varchar",
    )

    def __init__(
        self, database: str, verbose: bool = False, *args, **kwargs
    ) -> None:
        """Initialize the base class constructor."""
        self.__engine: sa.engine.Engine = _pg_engine(
            database, echo=False, **kwargs
        )
        self.__engine_ro: t.Optional[sa.engine.Engine] = None
        if (
            PG_USER_RO
            or PG_HOST_RO
            or PG_PASSWORD_RO
            or PG_PORT_RO
            or PG_URL_RO
        ):
            kwargs.update(
                {
                    "user": PG_USER_RO,
                    "host": PG_HOST_RO,
                    "password": PG_PASSWORD_RO,
                    "port": PG_PORT_RO,
                    "url": PG_URL_RO,
                }
            )
            self.__engine_ro: sa.engine.Engine = _pg_engine(
                database, echo=False, **kwargs
            )
        self.__schemas: t.Optional[dict] = None
        # models is a dict of f'{schema}.{table}'
        self.__models: dict = {}
        self.__metadata: dict = {}
        self.__indices: dict = {}
        self.__views: dict = {}
        self.__materialized_views: dict = {}
        self.__tables: dict = {}
        self.__columns: dict = {}
        self.verbose: bool = verbose
        self._conn = None
        self._session = None

    def connect(self) -> None:
        """Connect to database."""
        try:
            conn = self.engine.connect()
            conn.close()
        except Exception as e:
            logger.exception(f"Cannot connect to database: {e}")
            raise

    def pg_settings(self, column: str) -> t.Optional[str]:
        try:
            return self.fetchone(
                sa.select(
                    sa.text("setting"),
                )
                .select_from(sa.text("pg_settings"))
                .where(sa.column("name") == column),
                label="pg_settings",
            )[0]
        except (TypeError, IndexError):
            return None

    @property
    def is_mysql_compat(self) -> bool:
        """
        True when running against a MySQL-family backend (MySQL or MariaDB),
        regardless of the specific driver in use.
        """
        return IS_MYSQL_COMPAT

    def _can_create_replication_slot(self, slot_name: str) -> None:
        """Check if the given user can create and destroy replication slots."""
        with self.advisory_lock(
            slot_name, max_retries=None, retry_interval=0.1
        ):
            if self.replication_slots(slot_name):
                logger.exception(
                    f"Replication slot {slot_name} already exists"
                )
                self.drop_replication_slot(slot_name)

            try:
                self.create_replication_slot(slot_name)

            except Exception as e:
                logger.exception(f"{e}")
                raise ReplicationSlotError(
                    f'PG_USER "{self.engine.url.username}" needs to be '
                    f"superuser or have permission to read, create and destroy "
                    f"replication slots to perform this action.\n{e}"
                )
            else:
                self.drop_replication_slot(slot_name)

    # Tables...
    def models(self, table: str, schema: str) -> sa.sql.Alias:
        """Get an SQLAlchemy model representation from a table.

        Args:
            table (str): The tablename
            schema (str): The database schema

        Returns:
            The SQLAlchemy aliased model representation

        """
        name: str = f"{schema}.{table}"
        if name not in self.__models:
            if schema not in self.__metadata:
                metadata = sa.MetaData(schema=schema)
                metadata.reflect(self.engine, views=True)
                self.__metadata[schema] = metadata
            metadata = self.__metadata[schema]
            if name not in metadata.tables:
                raise TableNotFoundError(
                    f'Table "{name}" not found in registry'
                )
            model = metadata.tables[name]
            model.append_column(sa.Column("xmin", sa.BigInteger))
            model.append_column(sa.Column("ctid"), TupleIdentifierType)
            # support SQLAlchemy/Postgres 14 which somehow now reflects
            # the oid column
            if "oid" not in [column.name for column in model.columns]:
                model.append_column(
                    sa.Column("oid", sa.dialects.postgresql.OID)
                )
            model = model.alias()
            setattr(
                model,
                "primary_keys",
                sorted([primary_key.key for primary_key in model.primary_key]),
            )
            self.__models[f"{model.original}"] = model

        return self.__models[name]

    @property
    def conn(self):
        return self._conn

    @property
    def database(self) -> str:
        """str: Get the database name."""
        return self.engine.url.database

    @property
    def session(self) -> sessionmaker:
        if self._session is None:
            Session = sessionmaker(bind=self.engine, autoflush=True)
            self._session = Session()
        return self._session

    def close_session(self) -> None:
        """Close the cached session and reset it."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    @property
    def engine(self) -> sa.engine.Engine:
        """Get the database engine."""
        if getattr(self._thread_local, "read_only", False):
            return self.__engine_ro
        return self.__engine

    @property
    def schemas(self) -> dict:
        """Get the database schema names."""
        if self.__schemas is None:
            self.__schemas = sa.inspect(self.engine).get_schema_names()
            for schema in BUILTIN_SCHEMAS:
                if schema in self.__schemas:
                    self.__schemas.remove(schema)
        return self.__schemas

    def views(self, schema: str) -> list:
        """Get all materialized and non-materialized views."""
        return self._views(schema) + self._materialized_views(schema)

    def _views(self, schema: str) -> list:
        """Get all non-materialized views."""
        if schema not in self.__views:
            self.__views[schema] = []
            for table in sa.inspect(self.engine).get_view_names(schema):
                # TODO: figure out why we need is_view here when SQLAlchemy
                # already reflects views
                if is_view(self.engine, schema, table, materialized=False):
                    self.__views[schema].append(table)
        return self.__views[schema]

    def _materialized_views(self, schema: str) -> list:
        """Get all materialized views."""
        if self.is_mysql_compat:
            return []

        if schema not in self.__materialized_views:
            self.__materialized_views[schema] = []
            for table in sa.inspect(self.engine).get_materialized_view_names(
                schema
            ):
                # TODO: figure out why we need is_view here when sqlalchemy
                # already reflects views
                if is_view(self.engine, schema, table, materialized=True):
                    self.__materialized_views[schema].append(table)
        return self.__materialized_views[schema]

    def indices(self, table: str, schema: str) -> list:
        """Get the database table indexes."""
        if (table, schema) not in self.__indices:
            indexes = sa.inspect(self.engine).get_indexes(table, schema=schema)
            self.__indices[(table, schema)] = sorted(
                indexes, key=lambda d: d["name"]
            )
        return self.__indices[(table, schema)]

    def tables(self, schema: str) -> list:
        """Get the table names for current schema."""
        if schema not in self.__tables:
            self.__tables[schema] = sorted(
                sa.inspect(self.engine).get_table_names(schema)
            )
        return self.__tables[schema]

    def columns(self, schema: str, table: str) -> list:
        """Get the column names for a table/view."""
        if (table, schema) not in self.__columns:
            columns = sa.inspect(self.engine).get_columns(table, schema=schema)
            self.__columns[(table, schema)] = sorted(
                [column["name"] for column in columns]
            )
        return self.__columns[(table, schema)]

    def truncate_table(self, table: str, schema: str = DEFAULT_SCHEMA) -> None:
        """Truncate a table.

        Note:
            we need to quote table names that can be reserved sql statements
            like user

        Args:
            table (str): The tablename
            schema (str): The database schema

        """
        table_name: str = qname(self.engine, schema, table)
        logger.debug(f"Truncating table: {table_name}")
        if self.is_mysql_compat:
            self.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
        else:
            self.execute(sa.text(f"TRUNCATE TABLE {table_name} CASCADE"))
        logger.debug(f"Truncated table: {table_name}")

    def truncate_tables(
        self, tables: t.List[str], schema: str = DEFAULT_SCHEMA
    ) -> None:
        """Truncate all tables."""
        logger.debug(f"Truncating tables: {tables}")
        for table in tables:
            self.truncate_table(table, schema=schema)
        logger.debug(f"Truncated tables: {tables}")

    def truncate_schema(self, schema: str) -> None:
        """Truncate all tables in a schema."""
        logger.debug(f"Truncating schema: {schema}")
        self.truncate_tables(self.tables(schema), schema=schema)
        logger.debug(f"Truncated schema: {schema}")

    def truncate_schemas(self) -> None:
        """Truncate all tables in a database."""
        for schema in self.schemas:
            self.truncate_schema(schema)

    # Replication slots...
    def replication_slots(
        self,
        slot_name: str,
        plugin: str = PLUGIN,
        slot_type: str = "logical",
    ) -> t.List[str]:
        """List replication slots.

        SELECT * FROM PG_REPLICATION_SLOTS
        """
        return self.fetchall(
            sa.select("*")
            .select_from(sa.text("PG_REPLICATION_SLOTS"))
            .where(
                sa.and_(
                    *[
                        sa.column("slot_name") == slot_name,
                        sa.column("slot_type") == slot_type,
                        sa.column("plugin") == plugin,
                    ]
                )
            ),
            label="replication_slots",
        )

    def create_replication_slot(self, slot_name: str) -> None:
        """Create a replication slot.

        TODO:
        - Only create the replication slot if it does not exist
          otherwise warn that it already exists and return

        SELECT * FROM PG_REPLICATION_SLOTS
        """
        logger.debug(f"Creating replication slot: {slot_name}")
        try:
            with self.advisory_lock(
                slot_name, max_retries=None, retry_interval=0.1
            ):
                self.execute(
                    sa.select("*").select_from(
                        sa.func.PG_CREATE_LOGICAL_REPLICATION_SLOT(
                            slot_name,
                            PLUGIN,
                        )
                    )
                )
        except Exception as e:
            logger.exception(f"{e}")
            raise
        logger.debug(f"Created replication slot: {slot_name}")

    def drop_replication_slot(self, slot_name: str) -> None:
        """Drop a replication slot."""
        logger.debug(f"Dropping replication slot: {slot_name}")
        if self.replication_slots(slot_name):
            try:
                with self.advisory_lock(
                    slot_name, max_retries=None, retry_interval=0.1
                ):
                    self.execute(
                        sa.select("*").select_from(
                            sa.func.PG_DROP_REPLICATION_SLOT(slot_name),
                        )
                    )
            except Exception as e:
                logger.exception(f"{e}")
                raise
        logger.debug(f"Dropped replication slot: {slot_name}")

    def advisory_key(self, slot_name: str) -> int:
        """Compute a stable bigint advisory key from slot name."""
        if self.is_mysql_compat:
            # 'adv:' + 60 hex chars = 64 total; deterministic and safe for GET_LOCK
            row = self.fetchone(
                sa.text(
                    "SELECT CONCAT('adv:', LEFT(SHA2(:slot, 256), 60))"
                ).bindparams(slot=slot_name)
            )
            return row[0]
        # PostgreSQL: stable bigint via hashtext
        row = self.fetchone(
            sa.text("SELECT hashtext(:slot)::bigint").bindparams(
                slot=slot_name
            )
        )
        return row[0]

    def pg_try_advisory_lock(
        self, key: t.Union[int, str], timeout: int = 0
    ) -> bool:
        """
        Attempts to acquire an dvisory/named lock based on a hashed slot name without blocking.

        PostgreSQL: integer key -> PG_TRY_ADVISORY_LOCK(key) -> bool
        MySQL/MariaDB: string name -> GET_LOCK(name, timeout) -> 1 on success
                    (timeout defaults to 0 = non-blocking)

        Returns:
            bool: True if the lock was acquired, False otherwise.
        """
        if self.is_mysql_compat:
            row = self.fetchone(
                sa.text("SELECT GET_LOCK(:name, :timeout)").bindparams(
                    name=str(key), timeout=int(timeout)
                )
            )
            return bool(row and row[0] == 1)

        row = self.fetchone(
            sa.text("SELECT PG_TRY_ADVISORY_LOCK(:key)").bindparams(key=key)
        )
        return bool(row and row[0])

    def pg_advisory_unlock(self, key: t.Union[int, str]) -> bool:
        """
        Releases an advisory lock associated with the hashed slot name.

        Returns:
            bool: True if the lock was released, False if it was not held.
        """
        if self.is_mysql_compat:
            row = self.fetchone(
                sa.text("SELECT RELEASE_LOCK(:name)").bindparams(name=str(key))
            )
            return bool(row and row[0] == 1)

        row = self.fetchone(
            sa.text("SELECT PG_ADVISORY_UNLOCK(:key)").bindparams(key=key)
        )
        return bool(row and row[0])

    @contextmanager
    def advisory_lock(
        self,
        slot_name: str,
        max_retries: int = 5,
        retry_interval: float = 1.0,
        backoff_type: str = "fixed",  # or "exponential"
        backoff_factor: float = 2.0,
        jitter: str = "full",  # "none" | "full" | "equal" | "decorrelated"
        max_delay: float = 30.0,  # cap for delay growth
    ):
        """
        Context manager to acquire a PostgreSQL advisory lock with optional retries.
        Acquire a PostgreSQL advisory lock with retries, backoff, and jitter.
        Jitter reduces lock-step contention so callers don't starve.
        """
        key: int = self.advisory_key(slot_name)
        attempt: int = 0

        base_delay: float = float(retry_interval)
        # current backoff window (seconds)
        delay: float = base_delay

        while True:
            if self.pg_try_advisory_lock(key):
                break

            if (max_retries is not None) and (attempt >= max_retries):
                raise RuntimeError(
                    f"Failed to acquire advisory lock for '{slot_name}' after {max_retries} retries."
                )

            # Compute sleep using jitter strategy
            if jitter == "decorrelated":
                # Decorrelated jitter chooses the *next* delay first.
                delay = min(max_delay, random.uniform(base_delay, delay * 3))
                sleep_for = delay
            else:
                # For other modes, sleep is derived from current delay.
                if jitter == "full":
                    sleep_for = random.uniform(0.0, delay)
                elif jitter == "equal":
                    sleep_for = (delay / 2.0) + random.uniform(
                        0.0, delay / 2.0
                    )
                elif jitter == "none":
                    sleep_for = delay
                else:
                    # Fallback to full jitter if an unknown option is passed
                    sleep_for = random.uniform(0.0, delay)

            time.sleep(max(0.0, sleep_for))

            # Increase delay for next attempt (except decorrelated which already advanced)
            if backoff_type == "exponential" and jitter != "decorrelated":
                delay = min(max_delay, delay * backoff_factor)
            # For fixed backoff, 'delay' stays at base_delay unless decorrelated changed it.

            attempt += 1

        try:
            yield
        finally:
            try:
                self.pg_advisory_unlock(key)
            except Exception:
                pass

    def _logical_slot_changes(
        self,
        slot_name: str,
        func: sa.sql.functions._FunctionGenerator,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        upto_lsn: t.Optional[str] = None,
        upto_nchanges: t.Optional[int] = None,
        limit: t.Optional[int] = None,
        offset: t.Optional[int] = None,
    ) -> sa.sql.Select:
        """
        Returns a SQLAlchemy Select statement that selects changes from a logical replication slot.

        Args:
            slot_name (str): The name of the logical replication slot to read from.
            func (sa.sql.functions._FunctionGenerator): The function to use to read from the slot.
            txmin (Optional[int], optional): The minimum transaction ID to read from. Defaults to None.
            txmax (Optional[int], optional): The maximum transaction ID to read from. Defaults to None.
            upto_lsn (Optional[str], optional): The maximum LSN to read up to. Defaults to None.
            upto_nchanges (Optional[int], optional): The maximum number of changes to read. Defaults to None.
            limit (Optional[int], optional): The maximum number of rows to return. Defaults to None.
            offset (Optional[int], optional): The number of rows to skip before returning. Defaults to None.

        Returns:
            sa.sql.Select: A SQLAlchemy Select statement that selects changes from the logical replication slot.
        """
        filters: list = []
        statement: sa.sql.Select = sa.select(
            sa.text("xid"),
            sa.text("data"),
        ).select_from(
            func(
                slot_name,
                upto_lsn,
                upto_nchanges,
            )
        )
        if txmin is not None:
            filters.append(
                sa.cast(
                    sa.cast(sa.column("xid"), sa.Text),
                    sa.BigInteger,
                )
                >= txmin
            )
        if txmax is not None:
            filters.append(
                sa.cast(
                    sa.cast(sa.column("xid"), sa.Text),
                    sa.BigInteger,
                )
                < txmax
            )
        if filters:
            statement = statement.where(sa.and_(*filters))
        if limit is not None:
            statement = statement.limit(limit)
        if offset is not None:
            statement = statement.offset(offset)
        return statement

    @property
    def current_wal_lsn(self) -> str:
        return self.fetchone(
            sa.select(sa.func.MAX(sa.text("pg_current_wal_lsn"))).select_from(
                sa.func.PG_CURRENT_WAL_LSN()
            )
        )[0]

    def logical_slot_get_changes(
        self,
        slot_name: str,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        upto_lsn: t.Optional[str] = None,
        upto_nchanges: t.Optional[int] = None,
        limit: t.Optional[int] = None,
        offset: t.Optional[int] = None,
    ) -> None:
        """Get/Consume changes from a logical replication slot.

        To get one change and data in existing replication slot:
        SELECT * FROM PG_LOGICAL_SLOT_GET_CHANGES('testdb', NULL, 1)

        To get ALL changes and data in existing replication slot:
        SELECT * FROM PG_LOGICAL_SLOT_GET_CHANGES('testdb', NULL, NULL)
        """
        with self.advisory_lock(
            slot_name, max_retries=None, retry_interval=0.1
        ):
            statement: sa.sql.Select = self._logical_slot_changes(
                slot_name,
                sa.func.PG_LOGICAL_SLOT_GET_CHANGES,
                txmin=txmin,
                txmax=txmax,
                upto_lsn=upto_lsn,
                upto_nchanges=upto_nchanges,
                limit=limit,
                offset=offset,
            )
            self.execute(
                statement, options=dict(stream_results=STREAM_RESULTS)
            )

    def logical_slot_peek_changes(
        self,
        slot_name: str,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        upto_lsn: t.Optional[str] = None,
        upto_nchanges: t.Optional[int] = None,
        limit: t.Optional[int] = None,
        offset: t.Optional[int] = None,
    ) -> t.List[sa.engine.row.Row]:
        """Peek a logical replication slot without consuming changes.

        SELECT * FROM PG_LOGICAL_SLOT_PEEK_CHANGES('testdb', NULL, 1)
        """
        with self.advisory_lock(
            slot_name, max_retries=None, retry_interval=0.1
        ):
            statement: sa.sql.Select = self._logical_slot_changes(
                slot_name,
                sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
                txmin=txmin,
                txmax=txmax,
                upto_lsn=upto_lsn,
                upto_nchanges=upto_nchanges,
                limit=limit,
                offset=offset,
            )
            return self.fetchall(statement)

    def logical_slot_count_changes(
        self,
        slot_name: str,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        upto_lsn: t.Optional[str] = None,
        upto_nchanges: t.Optional[int] = None,
    ) -> int:
        statement: sa.sql.Select = self._logical_slot_changes(
            slot_name,
            sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
            txmin=txmin,
            txmax=txmax,
            upto_lsn=upto_lsn,
            upto_nchanges=upto_nchanges,
        )
        with self.engine.connect() as conn:
            return conn.execute(
                statement.with_only_columns(*[sa.func.COUNT()])
            ).scalar()

    # Views...

    def view_exists(self, name: str, schema: str) -> bool:
        return name in self.views(schema)

    def create_view(
        self,
        index: str,
        schema: str,
        tables: t.Set,
        user_defined_fkey_tables: dict,
        node_columns: dict,
    ) -> None:
        create_view(
            self.engine,
            self.models,
            self.fetchall,
            index,
            schema,
            tables,
            user_defined_fkey_tables,
            self._materialized_views(schema),
            node_columns,
        )

    def drop_view(self, schema: str) -> None:
        """Drop a view."""
        logger.debug(f"Dropping view: {schema}.{MATERIALIZED_VIEW}")
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(DropView(schema, MATERIALIZED_VIEW))
        logger.debug(f"Dropped view: {schema}.{MATERIALIZED_VIEW}")

    def refresh_view(
        self, name: str, schema: str, concurrently: bool = False
    ) -> None:
        """Refresh a materialized view."""
        logger.debug(f"Refreshing view: {schema}.{name}")
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(RefreshView(schema, name, concurrently=concurrently))
        logger.debug(f"Refreshed view: {schema}.{name}")

    # Triggers...

    def trigger_exists(self, trigger: str, table: str, schema: str) -> bool:
        """
        Return True if the user-defined trigger is already present on table in schema.
        """
        sql: str = """
            SELECT EXISTS (
                SELECT 1
                FROM   pg_trigger     AS t
                JOIN   pg_class       AS c  ON c.oid = t.tgrelid
                JOIN   pg_namespace   AS n  ON n.oid = c.relnamespace
                WHERE  NOT t.tgisinternal           -- exclude system triggers
                AND  t.tgname   = :trigge
                AND  c.relname  = :table
                AND  n.nspname  = :schema
            )
        """
        params: dict = dict(
            trigger=trigger,
            table=table,
            schema=schema,
        )
        with self.engine.connect() as conn:
            return bool(conn.execute(sa.text(sql), params).scalar())

    def create_triggers(
        self,
        schema: str,
        tables: t.Optional[t.List[str]] = None,
        join_queries: bool = False,
        if_not_exists: bool = False,
    ) -> None:
        """Create a database triggers."""
        queries: t.List[str] = []
        for table in self.tables(schema):
            if (tables and table not in tables) or (
                table in self.views(schema)
            ):
                continue
            logger.debug(f"Creating trigger on table: {schema}.{table}")
            for name, level, tg_op in [
                ("notify", "ROW", ["INSERT", "UPDATE", "DELETE"]),
                ("truncate", "STATEMENT", ["TRUNCATE"]),
            ]:

                if if_not_exists or not self.view_exists(
                    MATERIALIZED_VIEW, schema
                ):

                    self.drop_triggers(schema, [table])
                    queries.append(
                        f'CREATE TRIGGER "{schema}_{table}_{name}" '
                        f'AFTER {" OR ".join(tg_op)} ON "{schema}"."{table}" '
                        f"FOR EACH {level} EXECUTE PROCEDURE "
                        f"{schema}.{TRIGGER_FUNC}()",
                    )
        if join_queries:
            if queries:
                self.execute(sa.text("; ".join(queries)))
        else:
            for query in queries:
                self.execute(sa.text(query))

    def drop_triggers(
        self,
        schema: str,
        tables: t.Optional[t.List[str]] = None,
        join_queries: bool = False,
    ) -> None:
        """Drop all pgsync defined triggers in database."""
        queries: t.List[str] = []
        for table in self.tables(schema):
            if tables and table not in tables:
                continue
            logger.debug(f"Dropping trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                queries.append(
                    f'DROP TRIGGER IF EXISTS "{schema}_{table}_{name}" ON '
                    f'"{schema}"."{table}"'
                )
        if join_queries:
            if queries:
                self.execute(sa.text("; ".join(queries)))
        else:
            for query in queries:
                self.execute(sa.text(query))

    def function_exists(self, schema: str) -> bool:
        """Check if the trigger function exists."""
        return self.exists(
            sa.text(
                "SELECT 1 FROM pg_proc WHERE proname = :name "
                "AND pronamespace = (SELECT oid FROM pg_namespace "
                "WHERE nspname = :schema)"
            ).bindparams(name=TRIGGER_FUNC, schema=schema),
        )

    def create_function(self, schema: str) -> None:
        self.execute(
            sa.text(
                CREATE_TRIGGER_TEMPLATE.replace(
                    MATERIALIZED_VIEW,
                    f"{schema}.{MATERIALIZED_VIEW}",
                ).replace(
                    TRIGGER_FUNC,
                    f"{schema}.{TRIGGER_FUNC}",
                )
            )
        )

    def drop_function(self, schema: str) -> None:
        self.execute(
            sa.text(
                f'DROP FUNCTION IF EXISTS "{schema}".{TRIGGER_FUNC}() CASCADE'
            )
        )

    def disable_trigger(self, schema: str, table: str) -> None:
        """Disable a pgsync defined trigger."""
        for name in ("notify", "truncate"):
            self.execute(
                sa.text(
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"DISABLE TRIGGER {schema}_{table}_{name}"
                )
            )

    def disable_triggers(self, schema: str) -> None:
        """Disable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            logger.debug(f"Disabling trigger on table: {schema}.{table}")
            self.disable_trigger(schema, table)
            logger.debug(f"Disabled trigger on table: {schema}.{table}")

    def enable_trigger(self, schema: str, table: str) -> None:
        """Enable a pgsync defined trigger."""
        for name in ("notify", "truncate"):
            self.execute(
                sa.text(
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"ENABLE TRIGGER {schema}_{table}_{name}"
                )
            )

    def enable_triggers(self, schema: str) -> None:
        """Enable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            logger.debug(f"Enabling trigger on table: {schema}.{table}")
            self.enable_trigger(schema, table)
            logger.debug(f"Enabled trigger on table: {schema}.{table}")

    @property
    def txid_current(self) -> int:
        """
        Get last committed transaction id from the database.

        SELECT txid_current()
        """
        return self.fetchone(
            sa.select("*").select_from(sa.func.TXID_CURRENT()),
            label="txid_current",
        )[0]

    def pg_visible_in_snapshot(
        self, literal_binds: bool = False
    ) -> t.Callable[[t.List[int]], dict]:
        def _pg_visible_in_snapshot(xid8s: t.List[int]) -> dict:
            if not xid8s:
                return {}
            # TODO: use the SQLAlchemy ORM to handle this query
            statement = sa.text(
                """
                SELECT xid AS xid8,
                PG_VISIBLE_IN_SNAPSHOT(xid::xid8, PG_CURRENT_SNAPSHOT()) AS visible
                FROM UNNEST(CAST(:xid8s AS text[]))
                WITH ORDINALITY AS t(xid, ord)
                ORDER BY t.ord
            """
            )
            if self.verbose:
                compiled_query(
                    statement,
                    label="xmin_visibility",
                    literal_binds=literal_binds,
                )

            # xid8s = list of xid8 strings
            params: dict = {"xid8s": list(map(str, xid8s))}
            with self.__engine_ro.connect() as conn:
                result = conn.execute(statement, params)
                return {int(row.xid8): row.visible for row in result}

        return _pg_visible_in_snapshot

    def parse_value(self, type_: str, value: str) -> t.Optional[str]:
        """
        Parse datatypes from db.

        NB: All integers are long in python3 and call to convert is just int
        """
        if self.verbose:
            logger.debug(f"type: {type_} value: {value}")
        if value.lower() == "null":
            return None
        if type_.lower() in self.INT_TYPES:
            try:
                value = int(value)
            except ValueError:
                raise
        if type_.lower() in self.CHAR_TYPES:
            value = value.lstrip("'").rstrip("'")
        if type_.lower() == "boolean":
            value = bool(value)
        if type_.lower() in self.FLOAT_TYPES:
            try:
                value = float(value)
            except ValueError:
                raise
        return value

    def parse_logical_slot(self, row: str) -> Payload:

        def _parse_logical_slot(data: str) -> t.Iterator[t.Tuple[str, t.Any]]:
            pos: int = 0
            while True:
                match = LOGICAL_SLOT_SUFFIX.search(data, pos)
                if not match:
                    break

                key = (match.groupdict().get("key") or "").replace('"', "")
                raw_value = match.groupdict().get("value") or ""
                type_ = match.groupdict().get("type") or ""

                parsed = self.parse_value(type_, raw_value)
                yield key, parsed

                start, end = match.span()
                # advance safely even if the pattern can match zero-length
                pos = end if end > start else end + 1

        match = LOGICAL_SLOT_PREFIX.search(row)
        if not match:
            raise LogicalSlotParseError(f"No match for row: {row}")

        data: dict = {"old": None, "new": None}
        data.update(**match.groupdict())
        payload: Payload = Payload(**data)

        span = match.span()
        # including trailing space below is deliberate
        suffix: str = f"{row[span[1]:]} "

        if "old-key" in suffix and "new-tuple" in suffix:
            # this can only be an UPDATE operation
            if payload.tg_op != UPDATE:
                msg = f"Unknown {payload.tg_op} operation for row: {row}"
                raise LogicalSlotParseError(msg)

            i: int = suffix.find("old-key:")
            if i > -1:
                j: int = suffix.find("new-tuple:")
                if j > -1:
                    s: str = suffix[i + len("old-key:") : j]
                    for key, value in _parse_logical_slot(s):
                        payload.old[key] = value

            i = suffix.find("new-tuple:")
            if i > -1:
                s = suffix[i + len("new-tuple:") :]
                for key, value in _parse_logical_slot(s):
                    payload.new[key] = value
        else:
            # this can be an INSERT, DELETE, UPDATE or TRUNCATE operation
            if payload.tg_op not in TG_OPS:
                raise LogicalSlotParseError(
                    f"Unknown {payload.tg_op} operation for row: {row}"
                )

            for key, value in _parse_logical_slot(suffix):
                payload.new[key] = value

        return payload

    # Querying...
    def execute(
        self,
        statement: sa.sql.Select,
        values: t.Optional[list] = None,
        options: t.Optional[dict] = None,
    ) -> None:
        """Execute a query statement."""
        pg_execute(self.engine, statement, values=values, options=options)

    def fetchone(
        self,
        statement: sa.sql.Select,
        label: t.Optional[str] = None,
        literal_binds: bool = False,
    ) -> sa.engine.Row:
        """Fetch one row query."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        with self.engine.connect() as conn:
            return conn.execute(statement).fetchone()

    def fetchall(
        self,
        statement: sa.sql.Select,
        label: t.Optional[str] = None,
        literal_binds: bool = False,
    ) -> t.List[sa.engine.Row]:
        """Fetch all rows from a query statement."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        with self.engine.connect() as conn:
            return conn.execute(statement).fetchall()

    def exists(
        self,
        statement: sa.sql.Select,
        label: t.Optional[str] = None,
        literal_binds: bool = False,
    ) -> t.List[sa.engine.Row]:
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)
        with self.engine.connect() as conn:
            result = conn.execute(statement).fetchone()
            if result is None:
                return False
            return result[0] > 0

    def fetchmany(
        self,
        statement: sa.sql.Select,
        chunk_size: t.Optional[int] = None,
        stream_results: t.Optional[bool] = None,
    ):
        chunk_size = chunk_size or QUERY_CHUNK_SIZE
        stream_results = stream_results or STREAM_RESULTS
        with self.engine.connect() as conn:
            result = conn.execution_options(
                stream_results=stream_results
            ).execute(statement.select())
            for partition in result.partitions(chunk_size):
                for keys, row, *primary_keys in partition:
                    yield keys, row, primary_keys
            result.close()
        self.engine.clear_compiled_cache()

    def fetchcount(self, statement: sa.sql.Subquery) -> int:
        with self.engine.connect() as conn:
            return conn.execute(
                statement.original.with_only_columns(
                    *[sa.func.COUNT()]
                ).order_by(None)
            ).scalar()


# helper methods


def subtransactions(session):
    """Context manager for executing code within a sub-transaction."""

    class ControlledExecution:
        def __init__(self, session):
            self.session = session

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise

    return ControlledExecution(session)


def pg_engine(
    database: str,
    user: t.Optional[str] = None,
    host: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    echo: bool = False,
    sslmode: t.Optional[str] = None,
    sslrootcert: t.Optional[str] = None,
):
    """Context manager for managing engines."""

    class ControlledExecution:
        def __init__(
            self,
            database: str,
            user: t.Optional[str] = None,
            host: t.Optional[str] = None,
            password: t.Optional[str] = None,
            port: t.Optional[int] = None,
            echo: bool = False,
            sslmode: t.Optional[str] = None,
            sslrootcert: t.Optional[str] = None,
        ):
            self.database = database
            self.user = user
            self.host = host
            self.password = password
            self.port = port
            self.echo = echo
            self.sslmode = sslmode
            self.sslrootcert = sslrootcert

        def __enter__(self) -> sa.engine.Engine:
            self._engine = _pg_engine(
                database,
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port,
                echo=self.echo,
                sslmode=self.sslmode,
                sslrootcert=self.sslrootcert,
            )
            return self._engine

        def __exit__(self, type, value, traceback) -> None:
            self._engine.connect().close()
            self._engine.dispose()

    return ControlledExecution(
        database,
        user=user,
        host=host,
        password=password,
        port=port,
        echo=echo,
        sslmode=sslmode,
        sslrootcert=sslrootcert,
    )


def _pg_engine(
    database: str,
    user: t.Optional[str] = None,
    host: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    echo: bool = False,
    sslmode: t.Optional[str] = None,
    sslrootcert: t.Optional[str] = None,
    url: t.Optional[str] = None,
) -> sa.engine.Engine:
    connect_args: dict = {}
    sslmode = sslmode or PG_SSLMODE
    sslrootcert = sslrootcert or PG_SSLROOTCERT

    if sslmode:
        if sslmode not in SSL_MODES:
            raise ValueError(f'Invalid sslmode: "{sslmode}"')
        connect_args["sslmode"] = sslmode

    if sslrootcert:
        if not os.path.exists(sslrootcert):
            raise IOError(
                f'"{sslrootcert}" not found.\n'
                f"Provide a valid file containing SSL certificate "
                f"authority (CA) certificate(s)."
            )
        connect_args["sslrootcert"] = sslrootcert

    if url is None:
        url: str = get_database_url(
            database,
            user=user,
            host=host,
            password=password,
            port=port,
        )

    # Use NullPool for testing to avoid connection exhaustion
    if SQLALCHEMY_USE_NULLPOOL:
        from sqlalchemy.pool import NullPool

        return sa.create_engine(
            url,
            echo=echo,
            connect_args=connect_args,
            poolclass=NullPool,
        )

    return sa.create_engine(
        url,
        echo=echo,
        connect_args=connect_args,
        pool_size=SQLALCHEMY_POOL_SIZE,
        max_overflow=SQLALCHEMY_MAX_OVERFLOW,
        pool_pre_ping=SQLALCHEMY_POOL_PRE_PING,
        pool_recycle=SQLALCHEMY_POOL_RECYCLE,
        pool_timeout=SQLALCHEMY_POOL_TIMEOUT,
    )


def pg_execute(
    engine: sa.engine.Engine,
    statement: sa.sql.Select,
    values: t.Optional[list] = None,
    options: t.Optional[dict] = None,
) -> None:
    with engine.connect() as conn:
        if options:
            conn = conn.execution_options(**options)
        conn.execute(statement, values)
        conn.commit()


def create_schema(database: str, schema: str, echo: bool = False) -> None:
    """Create database schema."""
    if not IS_MYSQL_COMPAT:
        logger.debug(f"Creating schema: {schema}")
        with pg_engine(database, echo=echo) as engine:
            pg_execute(
                engine, sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            )
        logger.debug(f"Created schema: {schema}")


def create_database(database: str, echo: bool = False) -> None:
    """Create a database."""
    logger.debug(f"Creating database: {database}")
    with pg_engine(
        MYSQL_DATABASE if IS_MYSQL_COMPAT else PG_DATABASE,
        echo=echo,
    ) as engine:
        pg_execute(
            engine,
            sa.text(f"CREATE DATABASE {database}"),
            options={"isolation_level": "AUTOCOMMIT"},
        )
    logger.debug(f"Created database: {database}")


def drop_database(database: str, echo: bool = False) -> None:
    """Drop a database."""
    logger.debug(f"Dropping database: {database}")
    with pg_engine(
        MYSQL_DATABASE if IS_MYSQL_COMPAT else PG_DATABASE, echo=echo
    ) as engine:
        pg_execute(
            engine,
            sa.text(f"DROP DATABASE IF EXISTS {database}"),
            options={"isolation_level": "AUTOCOMMIT"},
        )
    logger.debug(f"Dropped database: {database}")


def database_exists(database: str, echo: bool = False) -> bool:
    """Check if database is present."""
    with pg_engine(
        MYSQL_DATABASE if IS_MYSQL_COMPAT else PG_DATABASE,
        echo=echo,
    ) as engine:
        with engine.connect() as conn:
            if IS_MYSQL_COMPAT:
                sql = sa.text(
                    "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :db LIMIT 1"
                )
                return conn.execute(sql, {"db": database}).first() is not None

            else:
                row = conn.execute(
                    sa.select(
                        sa.text("1"),
                    )
                    .select_from(sa.text("pg_database"))
                    .where(sa.column("datname") == database),
                ).fetchone()
        return row is not None


def create_extension(
    database: str, extension: str, echo: bool = False
) -> None:
    """Create a database extension."""
    logger.debug(f"Creating extension: {extension}")
    with pg_engine(database, echo=echo) as engine:
        pg_execute(
            engine,
            sa.text(f'CREATE EXTENSION IF NOT EXISTS "{extension}"'),
        )
    logger.debug(f"Created extension: {extension}")


def drop_extension(database: str, extension: str, echo: bool = False) -> None:
    """Drop a database extension."""
    logger.debug(f"Dropping extension: {extension}")
    with pg_engine(database, echo=echo) as engine:
        pg_execute(engine, sa.text(f'DROP EXTENSION IF EXISTS "{extension}"'))
    logger.debug(f"Dropped extension: {extension}")
