"""PGSync views."""
import logging

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


def create_view(
    engine,
    schema: str,
    tables: list,
    user_defined_fkey_tables: dict,
    base: "Base",  # noqa F821
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
        for table_name, primary_keys, foreign_keys in base.fetchall(
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

    for table_name, columns in base.fetchall(
        base._primary_keys(schema, tables)
    ):
        rows.setdefault(
            table_name,
            {"primary_keys": set([]), "foreign_keys": set([])},
        )
        if columns:
            rows[table_name]["primary_keys"] |= set(columns)

    for table_name, columns in base.fetchall(
        base._foreign_keys(schema, tables)
    ):
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


def drop_view(engine, schema: str) -> None:
    logger.debug(f"Dropping view: {schema}.{MATERIALIZED_VIEW}")
    engine.execute(DropView(schema, MATERIALIZED_VIEW))
    logger.debug(f"Dropped view: {schema}.{MATERIALIZED_VIEW}")
