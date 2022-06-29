"""PGSync Base class."""
import logging
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import sqlalchemy as sa
import sqlparse
from sqlalchemy.dialects import postgresql  # noqa
from sqlalchemy.orm import sessionmaker

from .constants import (
    BUILTIN_SCHEMAS,
    DEFAULT_SCHEMA,
    LOGICAL_SLOT_PREFIX,
    LOGICAL_SLOT_SUFFIX,
    MATERIALIZED_VIEW,
    PLUGIN,
    TG_OP,
    TRIGGER_FUNC,
    UPDATE,
)
from .exc import (
    ForeignKeyError,
    InvalidPermissionError,
    LogicalSlotParseError,
    TableNotFoundError,
)
from .node import Node
from .settings import (
    PG_SSLMODE,
    PG_SSLROOTCERT,
    QUERY_CHUNK_SIZE,
    QUERY_LITERAL_BINDS,
)
from .trigger import CREATE_TRIGGER_TEMPLATE
from .urls import get_postgres_url
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
    def __init__(self, database: str, verbose: bool = False, *args, **kwargs):
        """Initialize the base class constructor."""
        self.__engine: sa.engine.Engine = pg_engine(database, **kwargs)
        self.__schemas: Optional[dict] = None
        # models is a dict of f'{schema}.{table}'
        self.models: Dict[str] = {}
        self.__metadata: Dict[str] = {}
        self.__indices: Dict[str] = {}
        self.__views: Dict[str] = {}
        self.__materialized_views: Dict[str] = {}
        self.verbose: bool = verbose
        self._conn = None

    def connect(self) -> None:
        """Connect to database."""
        try:
            conn = self.__engine.connect()
            conn.close()
        except Exception as e:
            logger.exception(f"Cannot connect to database: {e}")
            raise

    def pg_settings(self, column: str) -> Optional[str]:
        try:
            return self.fetchone(
                sa.select([sa.column("setting")])
                .select_from(sa.text("pg_settings"))
                .where(sa.column("name") == column),
                label="pg_settings",
            )[0]
        except (TypeError, IndexError):
            return None

    def has_permissions(self, username: str, permissions: List[str]) -> bool:
        """Check if the given user is a superuser or replication user."""
        if not set(permissions).issubset(
            set(("usecreatedb", "usesuper", "userepl"))
        ):
            raise InvalidPermissionError(
                f"Invalid user permission {permissions}"
            )

        # Azure usernames are of the form username@host on the SQLAlchemy
        # engine - engine.url.username@host but stored in
        # pg_user as just user.
        # we need to extract the real username from: username@host.
        host_part: str = self.engine.url.host.split(".")[0]
        username: str = username.split(f"@{host_part}")[0]

        with self.__engine.connect() as conn:
            return (
                conn.execute(
                    sa.select([sa.column("usename")])
                    .select_from(sa.text("pg_user"))
                    .where(
                        sa.and_(
                            *[
                                sa.column("usename") == username,
                                sa.or_(
                                    *[
                                        (
                                            sa.column(permission)
                                            == True  # noqa E712
                                        )
                                        for permission in permissions
                                    ]
                                ),
                            ]
                        )
                    )
                    .with_only_columns([sa.func.COUNT()])
                    .order_by(None)
                ).scalar()
                > 0
            )

    # Tables...
    def model(self, table: str, schema: str) -> sa.sql.Alias:
        """Get an SQLAlchemy model representation from a table.

        Args:
            table (str): The tablename
            schema (str): The database schema

        Returns:
            The SQLAlchemy aliased model representation

        """
        name: str = f"{schema}.{table}"
        if name not in self.models:
            if schema not in self.__metadata:
                metadata = sa.MetaData(schema=schema)
                metadata.reflect(self.__engine, views=True)
                self.__metadata[schema] = metadata
            metadata = self.__metadata[schema]
            if name not in metadata.tables:
                raise TableNotFoundError(
                    f'Table "{name}" not found in registry'
                )
            model = metadata.tables[name]
            model.append_column(sa.Column("xmin", sa.BigInteger))
            model.append_column(sa.Column("ctid"), TupleIdentifierType)
            # support SQLQlchemy/Postgres 14 which somehow now reflects
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
            self.models[f"{model.original}"] = model

        return self.models[name]

    @property
    def conn(self):
        return self._conn

    @property
    def database(self) -> str:
        """str: Get the database name."""
        return self.__engine.url.database

    @property
    def session(self) -> sessionmaker:
        Session = sessionmaker(bind=self.__engine.connect(), autoflush=True)
        return Session()

    @property
    def engine(self) -> sa.engine.Engine:
        """Get the database engine."""
        return self.__engine

    @property
    def schemas(self) -> list:
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
                if is_view(self.engine, schema, table, materialized=False):
                    self.__views[schema].append(table)
        return self.__views[schema]

    def _materialized_views(self, schema: str) -> list:
        """Get all materialized views."""
        if schema not in self.__materialized_views:
            self.__materialized_views[schema] = []
            for table in sa.inspect(self.engine).get_view_names(schema):
                if is_view(self.engine, schema, table, materialized=True):
                    self.__materialized_views[schema].append(table)
        return self.__materialized_views[schema]

    def indices(self, table: str) -> list:
        """Get the database table indexes."""
        if table not in self.__indices:
            self.__indices[table] = sa.inspect(self.engine).get_indexes(table)
        return self.__indices[table]

    def tables(self, schema: str) -> List:
        """:obj:`list` of :obj:`str`: Get all tables.

        returns the fully qualified table name with schema {schema}.{table}
        """
        if schema not in self.__metadata:
            metadata = sa.MetaData(schema=schema)
            metadata.reflect(self.__engine)
            self.__metadata[schema] = metadata
        metadata = self.__metadata[schema]
        return metadata.tables.keys()

    def _get_schema(self, schema: str, table: str) -> Tuple[str, str]:
        pairs: list = table.split(".")
        if len(pairs) == 2:
            return pairs[0], pairs[1]
        if len(pairs) == 1:
            return schema, pairs[0]
        raise ValueError(f"Invalid definition {table} for schema: {schema}")

    def truncate_table(self, table: str, schema: str = DEFAULT_SCHEMA) -> None:
        """Truncate a table.

        Note:
            we need to quote table names that can be reserved sql statements
            like user

        Args:
            table (str): The tablename
            schema (str): The database schema

        """
        if not table.startswith(f"{schema}."):
            table = f"{schema}.{table}"
        schema, table = self._get_schema(schema, table)
        logger.debug(f"Truncating table: {schema}.{table}")
        query = f'TRUNCATE TABLE "{schema}"."{table}" CASCADE'
        self.execute(query)

    def truncate_tables(
        self, tables: List[str], schema: str = DEFAULT_SCHEMA
    ) -> None:
        """Truncate all tables."""
        logger.debug(f"Truncating tables: {tables}")
        for table in tables:
            self.truncate_table(table, schema=schema)

    def truncate_schema(self, schema: str) -> None:
        """Truncate all tables in a schema."""
        logger.debug(f"Truncating schema: {schema}")
        self.truncate_tables(self.tables(schema), schema=schema)

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
    ) -> List[str]:
        """List replication slots.

        SELECT * FROM PG_REPLICATION_SLOTS
        """
        return self.fetchall(
            sa.select(["*"])
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
        return self.fetchone(
            sa.select(["*"]).select_from(
                sa.func.PG_CREATE_LOGICAL_REPLICATION_SLOT(
                    slot_name,
                    PLUGIN,
                )
            ),
            label="create_replication_slot",
        )

    def drop_replication_slot(self, slot_name: str) -> None:
        """Drop a replication slot."""
        logger.debug(f"Dropping replication slot: {slot_name}")
        if self.replication_slots(slot_name):
            try:
                return self.fetchone(
                    sa.select(["*"]).select_from(
                        sa.func.PG_DROP_REPLICATION_SLOT(slot_name),
                    ),
                    label="drop_replication_slot",
                )
            except Exception as e:
                logger.exception(f"{e}")
                raise

    def _logical_slot_changes(
        self,
        slot_name: str,
        func: sa.sql.functions._FunctionGenerator,
        txmin: Optional[int] = None,
        txmax: Optional[int] = None,
        upto_lsn: Optional[int] = None,
        upto_nchanges: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> sa.sql.Select:
        filters: list = []
        statement: sa.sql.Select = sa.select(
            [sa.column("xid"), sa.column("data")]
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

    def logical_slot_get_changes(
        self,
        slot_name: str,
        txmin: Optional[int] = None,
        txmax: Optional[int] = None,
        upto_lsn: Optional[int] = None,
        upto_nchanges: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[sa.engine.row.LegacyRow]:
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
        return self.fetchall(statement)

    def logical_slot_peek_changes(
        self,
        slot_name: str,
        txmin: Optional[int] = None,
        txmax: Optional[int] = None,
        upto_lsn: Optional[int] = None,
        upto_nchanges: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[sa.engine.row.LegacyRow]:
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
        txmin: Optional[int] = None,
        txmax: Optional[int] = None,
        upto_lsn: Optional[int] = None,
        upto_nchanges: Optional[int] = None,
    ) -> int:
        statement: sa.sql.Select = self._logical_slot_changes(
            slot_name,
            sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
            txmin=txmin,
            txmax=txmax,
            upto_lsn=upto_lsn,
            upto_nchanges=upto_nchanges,
        )
        with self.__engine.connect() as conn:
            return conn.execute(
                statement.with_only_columns([sa.func.COUNT()])
            ).scalar()

    # Views...
    def create_view(
        self, schema: str, tables: list, user_defined_fkey_tables: dict
    ) -> None:
        create_view(
            self.engine,
            self.model,
            self.fetchall,
            schema,
            tables,
            user_defined_fkey_tables,
            self._materialized_views(schema),
        )

    def drop_view(self, schema: str) -> None:
        """Drop a view."""
        logger.debug(f"Dropping view: {schema}.{MATERIALIZED_VIEW}")
        self.engine.execute(DropView(schema, MATERIALIZED_VIEW))
        logger.debug(f"Dropped view: {schema}.{MATERIALIZED_VIEW}")

    def refresh_view(
        self, name: str, schema: str, concurrently: bool = False
    ) -> None:
        """Refresh a materialized view."""
        logger.debug(f"Refreshing view: {schema}.{name}")
        self.engine.execute(
            RefreshView(schema, name, concurrently=concurrently)
        )
        logger.debug(f"Refreshed view: {schema}.{name}")

    # Triggers...
    def create_triggers(
        self,
        schema: str,
        tables: Optional[List[str]] = None,
        join_queries: bool = False,
    ) -> None:
        """Create a database triggers."""
        queries: List[str] = []
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
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
                    f"{TRIGGER_FUNC}()",
                )
        if join_queries:
            if queries:
                self.execute("; ".join(queries))
        else:
            for query in queries:
                self.execute(sa.DDL(query))

    def drop_triggers(
        self,
        schema: str,
        tables: Optional[List[str]] = None,
        join_queries: bool = False,
    ) -> None:
        """Drop all pgsync defined triggers in database."""
        queries: List[str] = []
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
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
                self.execute("; ".join(queries))
        else:
            for query in queries:
                self.execute(sa.DDL(query))

    def create_function(self) -> None:
        self.execute(CREATE_TRIGGER_TEMPLATE)

    def drop_function(self) -> None:
        self.execute(f"DROP FUNCTION IF EXISTS {TRIGGER_FUNC}()")

    def disable_triggers(self, schema: str) -> None:
        """Disable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            logger.debug(f"Disabling trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                query = (
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"DISABLE TRIGGER {table}_{name}"
                )
                self.execute(query)

    def enable_triggers(self, schema: str) -> None:
        """Enable all pgsync defined triggers in database."""
        for table in self.tables(schema):
            schema, table = self._get_schema(schema, table)
            logger.debug(f"Enabling trigger on table: {schema}.{table}")
            for name in ("notify", "truncate"):
                query = (
                    f'ALTER TABLE "{schema}"."{table}" '
                    f"ENABLE TRIGGER {table}_{name}"
                )
                self.execute(query)

    @property
    def txid_current(self) -> int:
        """
        Get last committed transaction id from the database.

        SELECT txid_current()
        """
        return self.fetchone(
            sa.select(["*"]).select_from(sa.func.TXID_CURRENT()),
            label="txid_current",
        )[0]

    def parse_value(self, type_: str, value: str) -> Optional[str]:
        """
        Parse datatypes from db.

        NB: All integers are long in python3 and call to convert is just int
        """
        if value.lower() == "null":
            return None

        if type_.lower() in (
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
        ):
            try:
                value: int = int(value)
            except ValueError:
                raise
        if type_.lower() in (
            "char",
            "character",
            "character varying",
            "text",
            "uuid",
            "varchar",
        ):
            value: str = value.lstrip("'").rstrip("'")
        if type_.lower() == "boolean":
            try:
                value: bool = bool(value)
            except ValueError:
                raise
        if type_.lower() in (
            "double precision",
            "float4",
            "float8",
            "real",
        ):
            try:
                value: float = float(value)
            except ValueError:
                raise
        return value

    def parse_logical_slot(self, row: str):
        def _parse_logical_slot(data):

            while True:

                match = LOGICAL_SLOT_SUFFIX.search(data)
                if not match:
                    break

                key = match.groupdict().get("key")
                if key:
                    key = key.replace('"', "")
                value = match.groupdict().get("value")
                type_ = match.groupdict().get("type")

                value = self.parse_value(type_, value)

                # set data for next iteration of the loop
                data = f"{data[match.span()[1]:]} "
                yield key, value

        payload: dict = dict(
            schema=None, tg_op=None, table=None, old={}, new={}
        )

        match = LOGICAL_SLOT_PREFIX.search(row)
        if not match:
            raise LogicalSlotParseError(f"No match for row: {row}")

        payload.update(match.groupdict())
        span = match.span()
        # trailing space is deliberate
        suffix: str = f"{row[span[1]:]} "
        tg_op: str = payload["tg_op"]

        if "old-key" and "new-tuple" in suffix:
            # this can only be an UPDATE operation
            if tg_op != UPDATE:
                msg = f"Unknown {tg_op} operation for row: {row}"
                raise LogicalSlotParseError(msg)

            i = suffix.index("old-key:")
            if i > -1:
                j = suffix.index("new-tuple:")
                s = suffix[i + len("old-key:") : j]
                for key, value in _parse_logical_slot(s):
                    payload["old"][key] = value

            i = suffix.index("new-tuple:")
            if i > -1:
                s = suffix[i + len("new-tuple:") :]
                for key, value in _parse_logical_slot(s):
                    payload["new"][key] = value
        else:
            # this can be an INSERT, DELETE, UPDATE or TRUNCATE operation
            if tg_op not in TG_OP:
                message = f"Unknown {tg_op} operation for row: {row}"
                raise LogicalSlotParseError(message)

            for key, value in _parse_logical_slot(suffix):
                payload["new"][key] = value

        return payload

    # Querying...
    def execute(self, statement: sa.sql.Select, values=None, options=None):
        """Execute a query statement."""
        conn = self.__engine.connect()
        try:
            if options:
                conn = conn.execution_options(**options)
            conn.execute(statement, values)
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise

    def fetchone(
        self, statement: sa.sql.Select, label=None, literal_binds=False
    ):
        """Fetch one row query."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        conn = self.__engine.connect()
        try:
            row = conn.execute(statement).fetchone()
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise
        return row

    def fetchall(
        self,
        statement: sa.sql.Select,
        label: Optional[str] = None,
        literal_binds: bool = False,
    ):
        """Fetch all rows from a query statement."""
        if self.verbose:
            compiled_query(statement, label=label, literal_binds=literal_binds)

        conn = self.__engine.connect()
        try:
            rows = conn.execute(statement).fetchall()
            conn.close()
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise
        return rows

    def fetchmany(
        self,
        statement: sa.sql.Select,
        chunk_size: Optional[int] = None,
    ):
        chunk_size: int = chunk_size or QUERY_CHUNK_SIZE
        with self.__engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                statement.select()
            )
            for partition in result.partitions(chunk_size):
                for keys, row, *primary_keys in partition:
                    yield keys, row, primary_keys

    def fetchcount(self, statement: sa.sql.Subquery) -> int:
        with self.__engine.connect() as conn:
            return conn.execute(
                statement.original.with_only_columns(
                    [sa.func.COUNT()]
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


def _get_foreign_keys(model_a, model_b) -> dict:

    foreign_keys: dict = defaultdict(list)

    if model_a.foreign_keys:

        for key in model_a.original.foreign_keys:
            if key._table_key() == str(model_b.original):
                foreign_keys[str(key.parent.table)].append(key.parent)
                foreign_keys[str(key.column.table)].append(key.column)

    if not foreign_keys:

        if model_b.original.foreign_keys:

            for key in model_b.original.foreign_keys:
                if key._table_key() == str(model_a.original):
                    foreign_keys[str(key.parent.table)].append(key.parent)
                    foreign_keys[str(key.column.table)].append(key.column)

    if not foreign_keys:
        raise ForeignKeyError(
            f"No foreign key relationship between "
            f'"{model_a.original}" and "{model_b.original}"'
        )

    return foreign_keys


def get_foreign_keys(node_a: Node, node_b: Node) -> dict:
    """Return dict of single foreign key with multiple columns.

    e.g:
        {
            fk1['table_1']: [column_1, column_2, column_N],
            fk2['table_2']: [column_1, column_2, column_N],
        }

    column_1, column_2, column_N are of type ForeignKeyContraint
    """
    foreign_keys: dict = {}
    # if either offers a foreign_key via relationship, use it!
    if (
        node_a.relationship.foreign_key.parent
        or node_b.relationship.foreign_key.parent
    ):
        if node_a.relationship.foreign_key.parent:
            foreign_keys[node_a.parent.name] = sorted(
                node_a.relationship.foreign_key.parent
            )
            foreign_keys[node_a.name] = sorted(
                node_a.relationship.foreign_key.child
            )
        if node_b.relationship.foreign_key.parent:
            foreign_keys[node_b.parent.name] = sorted(
                node_b.relationship.foreign_key.parent
            )
            foreign_keys[node_b.name] = sorted(
                node_b.relationship.foreign_key.child
            )
    else:
        for table, columns in _get_foreign_keys(
            node_a.model,
            node_b.model,
        ).items():
            foreign_keys[table] = sorted([column.name for column in columns])
    return foreign_keys


def pg_engine(
    database: str,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[str] = None,
    echo: bool = False,
    sslmode: Optional[str] = None,
    sslrootcert: Optional[str] = None,
) -> sa.engine.Engine:
    connect_args = {}
    sslmode: str = sslmode or PG_SSLMODE
    sslrootcert: str = sslrootcert or PG_SSLROOTCERT

    if sslmode:
        if sslmode not in (
            "allow",
            "disable",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ):
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
    query: str,
    values: Optional[list] = None,
    options: Optional[dict] = None,
) -> None:
    options: dict = options or {"isolation_level": "AUTOCOMMIT"}
    conn = engine.connect()
    try:
        if options:
            conn = conn.execution_options(**options)
        conn.execute(query, values)
        conn.close()
    except Exception as e:
        logger.exception(f"Exception {e}")
        raise


def create_schema(engine: sa.engine.Engine, schema: str) -> None:
    """Create database schema."""
    if schema != DEFAULT_SCHEMA:
        engine.execute(sa.schema.CreateSchema(schema))


def create_database(database: str, echo: bool = False) -> None:
    """Create a database."""
    logger.debug(f"Creating database: {database}")
    engine: sa.engine.Engine = pg_engine(database="postgres", echo=echo)
    pg_execute(engine, f'CREATE DATABASE "{database}"')
    logger.debug(f"Created database: {database}")


def drop_database(database: str, echo: bool = False) -> None:
    """Drop a database."""
    logger.debug(f"Dropping database: {database}")
    engine: sa.engine.Engine = pg_engine(database="postgres", echo=echo)
    pg_execute(engine, f'DROP DATABASE IF EXISTS "{database}"')
    logger.debug(f"Dropped database: {database}")


def create_extension(
    database: str, extension: str, echo: bool = False
) -> None:
    """Create a database extension."""
    logger.debug(f"Creating extension: {extension}")
    engine: sa.engine.Engine = pg_engine(database=database, echo=echo)
    pg_execute(engine, f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
    logger.debug(f"Created extension: {extension}")


def drop_extension(database: str, extension: str, echo: bool = False) -> None:
    """Drop a database extension."""
    logger.debug(f"Dropping extension: {extension}")
    engine: sa.engine.Engine = pg_engine(database=database, echo=echo)
    pg_execute(engine, f'DROP EXTENSION IF EXISTS "{extension}"')
    logger.debug(f"Dropped extension: {extension}")


def compiled_query(
    query: str, label: Optional[str] = None, literal_binds: bool = False
) -> None:
    """Compile an SQLAlchemy query with an optional label."""

    # overide env value of literal_binds
    if QUERY_LITERAL_BINDS:
        literal_binds = QUERY_LITERAL_BINDS

    query: str = str(
        query.compile(
            dialect=sa.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": literal_binds},
        )
    )
    query: str = sqlparse.format(query, reindent=True, keyword_case="upper")
    if label:
        logger.debug(f"\033[4m{label}:\033[0m\n{query}")
        sys.stdout.write(f"\033[4m{label}:\033[0m\n{query}\n")
    else:
        logging.debug(f"{query}")
        sys.stdout.write(f"{query}\n")
    sys.stdout.write("-" * 79)
    sys.stdout.write("\n")
