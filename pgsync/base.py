"""PGSync Base."""

import logging
import os
import typing as t

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
    TG_OP,
    TRIGGER_FUNC,
    UPDATE,
)
from .exc import (
    LogicalSlotParseError,
    ReplicationSlotError,
    TableNotFoundError,
)
from .settings import (
    PG_SSLMODE,
    PG_SSLROOTCERT,
    QUERY_CHUNK_SIZE,
    STREAM_RESULTS,
)
from .trigger import CREATE_TRIGGER_TEMPLATE
from .urls import get_postgres_url
from .utils import compiled_query
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
        tg_op: str = t.Optional[None],
        table: str = t.Optional[None],
        schema: str = t.Optional[None],
        old: dict = t.Optional[None],
        new: dict = t.Optional[None],
        xmin: int = t.Optional[None],
        indices: t.List[str] = t.Optional[None],
    ):
        self.tg_op: str = tg_op
        self.table: str = table
        self.schema: str = schema
        self.old: dict = old or {}
        self.new: dict = new or {}
        self.xmin: str = xmin
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
            'public.customer': [{  referred table with a fully qualified name
                'local': 'customer_id',
                'remote': 'id',
                'value': 1
            }],
            'public.group': [{  referred table with a fully qualified name
                'local': 'group_id',
                'remote': 'id',
                'value': 1
            }]
        }
        """
        constraints: dict = {}
        for foreign_key in model.foreign_keys:
            referred_table: str = str(foreign_key.constraint.referred_table)
            constraints.setdefault(referred_table, [])
            if foreign_key.constraint.column_keys:
                if foreign_key.constraint.column_keys[0] in self.data:
                    constraints[referred_table].append({
                        "local": foreign_key.constraint.column_keys[0],
                        "remote": foreign_key.column.name,
                        "value": self.data[
                            foreign_key.constraint.column_keys[0]
                        ],
                    })
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

    def _can_create_replication_slot(self, slot_name: str) -> None:
        """Check if the given user can create and destroy replication slots."""
        if self.replication_slots(slot_name):
            logger.exception(f"Replication slot {slot_name} already exists")
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
        Session = sessionmaker(bind=self.engine.connect(), autoflush=True)
        return Session()

    @property
    def engine(self) -> sa.engine.Engine:
        """Get the database engine."""
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
        logger.debug(f"Truncating table: {schema}.{table}")
        self.execute(sa.text(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE'))
        logger.debug(f"Truncated table: {schema}.{table}")

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
                self.execute(
                    sa.select("*").select_from(
                        sa.func.PG_DROP_REPLICATION_SLOT(slot_name),
                    )
                )
            except Exception as e:
                logger.exception(f"{e}")
                raise
        logger.debug(f"Dropped replication slot: {slot_name}")

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
        self.execute(statement, options=dict(stream_results=STREAM_RESULTS))

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
    def create_view(
        self,
        index: str,
        schema: str,
        tables: t.Set,
        user_defined_fkey_tables: dict,
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
        )

    def drop_view(self, schema: str) -> None:
        """Drop a view."""
        logger.debug(f"Dropping view: {schema}.{MATERIALIZED_VIEW}")
        with self.engine.connect() as conn:
            conn.execute(DropView(schema, MATERIALIZED_VIEW))
        logger.debug(f"Dropped view: {schema}.{MATERIALIZED_VIEW}")

    def refresh_view(
        self, name: str, schema: str, concurrently: bool = False
    ) -> None:
        """Refresh a materialized view."""
        logger.debug(f"Refreshing view: {schema}.{name}")
        with self.engine.connect() as conn:
            conn.execute(RefreshView(schema, name, concurrently=concurrently))
        logger.debug(f"Refreshed view: {schema}.{name}")

    # Triggers...
    def create_triggers(
        self,
        schema: str,
        tables: t.Optional[t.List[str]] = None,
        join_queries: bool = False,
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
                self.drop_triggers(schema, [table])
                queries.append(
                    f'CREATE TRIGGER "{table}_{name}" '
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
                    f'DROP TRIGGER IF EXISTS "{table}_{name}" ON '
                    f'"{schema}"."{table}"'
                )
        if join_queries:
            if queries:
                self.execute(sa.text("; ".join(queries)))
        else:
            for query in queries:
                self.execute(sa.text(query))

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
                    f"DISABLE TRIGGER {table}_{name}"
                )
            )

    def disable_triggers(self, schema: str) -> None:
        """Disable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            logger.debug(f"Disabling trigger on table: {schema}.{table}")
            self.disable_trigger(schema, table)
            logger.debug(f"Disabled trigger on table: {schema}.{table}")

    def enable_trigger(self, schema: str, table, str) -> None:
        """Enable a pgsync defined trigger."""
        for name in ("notify", "truncate"):
            self.execute(
                sa.text(
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"ENABLE TRIGGER {table}_{name}"
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
        def _parse_logical_slot(data: str) -> t.Tuple[str, str]:
            while True:
                match = LOGICAL_SLOT_SUFFIX.search(data)
                if not match:
                    break

                key: str = match.groupdict().get("key")
                if key:
                    key = key.replace('"', "")
                value: str = match.groupdict().get("value")
                type_: str = match.groupdict().get("type")

                value = self.parse_value(type_, value)

                # set data for next iteration of the loop
                data = f"{data[match.span()[1]:]} "
                yield key, value

        match = LOGICAL_SLOT_PREFIX.search(row)
        if not match:
            raise LogicalSlotParseError(f"No match for row: {row}")

        data = {"old": None, "new": None}
        data.update(**match.groupdict())
        payload: Payload = Payload(**data)

        span = match.span()
        # including trailing space below is deliberate
        suffix: str = f"{row[span[1]:]} "

        if "old-key" and "new-tuple" in suffix:
            # this can only be an UPDATE operation
            if payload.tg_op != UPDATE:
                msg = f"Unknown {payload.tg_op} operation for row: {row}"
                raise LogicalSlotParseError(msg)

            i: int = suffix.index("old-key:")
            if i > -1:
                j: int = suffix.index("new-tuple:")
                s: str = suffix[i + len("old-key:") : j]
                for key, value in _parse_logical_slot(s):
                    payload.old[key] = value

            i = suffix.index("new-tuple:")
            if i > -1:
                s = suffix[i + len("new-tuple:") :]
                for key, value in _parse_logical_slot(s):
                    payload.new[key] = value
        else:
            # this can be an INSERT, DELETE, UPDATE or TRUNCATE operation
            if payload.tg_op not in TG_OP:
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

    url: str = get_postgres_url(
        database,
        user=user,
        host=host,
        password=password,
        port=port,
    )
    return sa.create_engine(url, echo=echo, connect_args=connect_args)


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
    logger.debug(f"Creating schema: {schema}")
    with pg_engine(database, echo=echo) as engine:
        pg_execute(engine, sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logger.debug(f"Created schema: {schema}")


def create_database(database: str, echo: bool = False) -> None:
    """Create a database."""
    logger.debug(f"Creating database: {database}")
    with pg_engine("postgres", echo=echo) as engine:
        pg_execute(
            engine,
            sa.text(f'CREATE DATABASE "{database}"'),
            options={"isolation_level": "AUTOCOMMIT"},
        )
    logger.debug(f"Created database: {database}")


def drop_database(database: str, echo: bool = False) -> None:
    """Drop a database."""
    logger.debug(f"Dropping database: {database}")
    with pg_engine("postgres", echo=echo) as engine:
        pg_execute(
            engine,
            sa.text(f'DROP DATABASE IF EXISTS "{database}"'),
            options={"isolation_level": "AUTOCOMMIT"},
        )
    logger.debug(f"Dropped database: {database}")


def database_exists(database: str, echo: bool = False) -> bool:
    """Check if database is present."""
    with pg_engine("postgres", echo=echo) as engine:
        with engine.connect() as conn:
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
