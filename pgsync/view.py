"""PGSync views."""
import logging
import warnings
from typing import Callable, List

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import Values
from sqlalchemy.sql.selectable import Select

from .constants import DEFAULT_SCHEMA, MATERIALIZED_VIEW

logger = logging.getLogger(__name__)


class CreateView(DDLElement):
    def __init__(
        self,
        schema: str,
        name: str,
        selectable: Select,
        materialized: bool = True,
    ):
        self.schema: str = schema
        self.name: str = name
        self.selectable: Select = selectable
        self.materialized: bool = materialized


@compiler.compiles(CreateView)
def compile_create_view(
    element: CreateView, compiler: PGDDLCompiler, **kwargs
) -> str:
    statement: str = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized: str = "MATERIALIZED" if element.materialized else ""
    return (
        f'CREATE {materialized} VIEW "{element.schema}"."{element.name}" AS '
        f"{statement}"
    )


class DropView(DDLElement):
    def __init__(
        self,
        schema: str,
        name: str,
        materialized: bool = True,
        cascade: bool = True,
    ):
        self.schema: str = schema
        self.name: str = name
        self.materialized: bool = materialized
        self.cascade: bool = cascade


@compiler.compiles(DropView)
def compile_drop_view(
    element: DropView, compiler: PGDDLCompiler, **kwargs
) -> str:
    cascade: str = "CASCADE" if element.cascade else ""
    materialized: str = "MATERIALIZED" if element.materialized else ""
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class RefreshView(DDLElement):
    def __init__(
        self,
        schema: str,
        name: str,
        concurrently: bool = False,
    ):
        self.schema: str = schema
        self.name: str = name
        self.concurrently: bool = concurrently


@compiler.compiles(RefreshView)
def compile_refresh_view(
    element: RefreshView, compiler: PGDDLCompiler, **kwargs
) -> str:
    concurrently: str = "CONCURRENTLY" if element.concurrently else ""
    return (
        f"REFRESH MATERIALIZED VIEW {concurrently} "
        f'"{element.schema}"."{element.name}"'
    )


class CreateIndex(DDLElement):
    def __init__(self, name: str, schema: str, view: str, columns: list):
        self.schema: str = schema
        self.name: str = name
        self.view: str = view
        self.columns: list = columns


@compiler.compiles(CreateIndex)
def compile_create_index(
    element: CreateIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.view}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    def __init__(self, name: str):
        self.name: str = name


@compiler.compiles(DropIndex)
def compile_drop_index(
    element: DropIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    return f"DROP INDEX IF EXISTS {element.name}"


def _primary_keys(
    engine, model: Callable, schema: str, tables: List[str]
) -> sa.sql.selectable.Select:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        pg_class = model("pg_class", "pg_catalog")
        pg_index = model("pg_index", "pg_catalog")
        pg_attribute = model("pg_attribute", "pg_catalog")
        pg_namespace = model("pg_namespace", "pg_catalog")

    alias = pg_class.alias("x")
    inclause: list = []
    for table in tables:
        pairs = table.split(".")
        if len(pairs) == 1:
            inclause.append(engine.dialect.identifier_preparer.quote(pairs[0]))
        elif len(pairs) == 2:
            inclause.append(
                f"{pairs[0]}.{engine.dialect.identifier_preparer.quote(pairs[-1])}"
            )
        else:
            raise Exception(f"cannot determine schema and table from {table}")

    return (
        sa.select(
            [
                sa.func.REPLACE(
                    sa.func.REVERSE(
                        sa.func.SPLIT_PART(
                            sa.func.REVERSE(
                                sa.cast(
                                    sa.cast(
                                        pg_index.c.indrelid,
                                        sa.dialects.postgresql.REGCLASS,
                                    ),
                                    sa.Text,
                                )
                            ),
                            ".",
                            1,
                        )
                    ),
                    '"',
                    "",
                ).label("table_name"),
                sa.func.ARRAY_AGG(pg_attribute.c.attname).label(
                    "primary_keys"
                ),
            ]
        )
        .join(
            pg_attribute,
            pg_attribute.c.attrelid == pg_index.c.indrelid,
        )
        .join(
            pg_class,
            pg_class.c.oid == pg_index.c.indexrelid,
        )
        .join(
            alias,
            alias.c.oid == pg_index.c.indrelid,
        )
        .join(
            pg_namespace,
            pg_namespace.c.oid == pg_class.c.relnamespace,
        )
        .where(
            *[
                pg_namespace.c.nspname.notin_(["pg_catalog", "pg_toast"]),
                pg_index.c.indisprimary,
                sa.cast(
                    sa.cast(
                        pg_index.c.indrelid,
                        sa.dialects.postgresql.REGCLASS,
                    ),
                    sa.Text,
                ).in_(inclause),
                pg_attribute.c.attnum == sa.any_(pg_index.c.indkey),
            ]
        )
        .group_by(pg_index.c.indrelid)
    )


def _foreign_keys(
    model, schema: str, tables: List[str]
) -> sa.sql.selectable.Select:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        table_constraints = model(
            "table_constraints",
            "information_schema",
        )
        key_column_usage = model(
            "key_column_usage",
            "information_schema",
        )
        constraint_column_usage = model(
            "constraint_column_usage",
            "information_schema",
        )

    return (
        sa.select(
            [
                table_constraints.c.table_name,
                sa.func.ARRAY_AGG(
                    sa.cast(
                        key_column_usage.c.column_name,
                        sa.TEXT,
                    )
                ).label("foreign_keys"),
            ]
        )
        .join(
            key_column_usage,
            sa.and_(
                key_column_usage.c.constraint_name
                == table_constraints.c.constraint_name,
                key_column_usage.c.table_schema
                == table_constraints.c.table_schema,
                key_column_usage.c.table_schema == schema,
            ),
        )
        .join(
            constraint_column_usage,
            sa.and_(
                constraint_column_usage.c.constraint_name
                == table_constraints.c.constraint_name,
                constraint_column_usage.c.table_schema
                == table_constraints.c.table_schema,
            ),
        )
        .where(
            *[
                table_constraints.c.table_name.in_(tables),
                table_constraints.c.constraint_type == "FOREIGN KEY",
            ]
        )
        .group_by(table_constraints.c.table_name)
    )


def create_view(
    engine,
    model: Callable,
    fetchall: Callable,
    schema: str,
    tables: list,
    user_defined_fkey_tables: dict,
) -> None:
    """
    View describing primary_keys and foreign_keys for each table
    with an index on table_name

    This is only called once on bootstrap.
    It is used within the trigger function to determine what payload
    values to send to pg_notify.

    Since views cannot be modified, we query the existing view for exiting
    rows and union this to the next query.

    So if 'specie' was the only row before, and the next query returns
    'unit' and 'structure', we want to end up with the result below.

    table_name | primary_keys | foreign_keys
    -----------+--------------+--------------
    specie     | {id}         | {id, user_id}
    unit       | {id}         | {id, profile_id}
    structure  | {id}         | {id}
    """

    views: list = sa.inspect(engine).get_view_names(schema)

    rows: dict = {}
    if MATERIALIZED_VIEW in views:
        for table_name, primary_keys, foreign_keys in fetchall(
            sa.select(["*"]).select_from(
                sa.text(f"{schema}.{MATERIALIZED_VIEW}")
            )
        ):
            rows.setdefault(
                table_name,
                {"primary_keys": set([]), "foreign_keys": set([])},
            )
            if primary_keys:
                rows[table_name]["primary_keys"] = set(primary_keys)
            if foreign_keys:
                rows[table_name]["foreign_keys"] = set(foreign_keys)

        engine.execute(DropView(schema, MATERIALIZED_VIEW))

    if schema != DEFAULT_SCHEMA:
        for table in set(tables):
            tables.add(f"{schema}.{table}")

    for table_name, columns in fetchall(
        _primary_keys(engine, model, schema, tables)
    ):
        rows.setdefault(
            table_name,
            {"primary_keys": set([]), "foreign_keys": set([])},
        )
        if columns:
            rows[table_name]["primary_keys"] |= set(columns)

    for table_name, columns in fetchall(_foreign_keys(model, schema, tables)):
        rows.setdefault(
            table_name,
            {"primary_keys": set([]), "foreign_keys": set([])},
        )
        if columns:
            rows[table_name]["foreign_keys"] |= set(columns)

    if user_defined_fkey_tables:
        for table_name, columns in user_defined_fkey_tables.items():
            rows.setdefault(
                table_name,
                {"primary_keys": set([]), "foreign_keys": set([])},
            )
            if columns:
                rows[table_name]["foreign_keys"] |= set(columns)

    if not rows:
        rows.setdefault(
            None,
            {"primary_keys": set([]), "foreign_keys": set([])},
        )

    statement = sa.select(
        Values(
            sa.column("table_name"),
            sa.column("primary_keys"),
            sa.column("foreign_keys"),
        )
        .data(
            [
                (
                    table_name,
                    array(fields["primary_keys"])
                    if fields.get("primary_keys")
                    else None,
                    array(fields.get("foreign_keys"))
                    if fields.get("foreign_keys")
                    else None,
                )
                for table_name, fields in rows.items()
            ]
        )
        .alias("t")
    )
    logger.debug(f"Creating view: {schema}.{MATERIALIZED_VIEW}")
    engine.execute(CreateView(schema, MATERIALIZED_VIEW, statement))
    engine.execute(DropIndex("_idx"))
    engine.execute(
        CreateIndex("_idx", schema, MATERIALIZED_VIEW, ["table_name"])
    )
    logger.debug(f"Created view: {schema}.{MATERIALIZED_VIEW}")


def is_materialized_view(
    engine,
    schema: str,
    table: str,
) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                sa.select([sa.column("matviewname")])
                .select_from(sa.text("pg_matviews"))
                .where(
                    sa.and_(
                        *[
                            sa.column("matviewname") == table,
                            sa.column("schemaname") == schema,
                        ]
                    )
                )
                .with_only_columns([sa.func.COUNT()])
                .order_by(None)
            ).scalar()
            > 0
        )
