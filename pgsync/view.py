"""PGSync views."""
import logging
import warnings
from typing import Callable, List, Set

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
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
    def __init__(self, name: str, schema: str, entity: str, columns: list):
        self.schema: str = schema
        self.name: str = name
        self.entity: str = entity
        self.columns: list = columns


@compiler.compiles(CreateIndex)
def compile_create_index(
    element: CreateIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.entity}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    def __init__(self, name: str):
        self.name: str = name


@compiler.compiles(DropIndex)
def compile_drop_index(
    element: DropIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    return f"DROP INDEX IF EXISTS {element.name}"


def _get_constraints(
    models: Callable,
    schema: str,
    tables: Set[str],
    label: str,
    constraint_type: str,
) -> sa.sql.Select:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        table_constraints = models("table_constraints", "information_schema")
        key_column_usage = models("key_column_usage", "information_schema")
    return (
        sa.select(
            [
                table_constraints.c.table_name,
                sa.func.ARRAY_AGG(
                    sa.cast(
                        key_column_usage.c.column_name,
                        sa.TEXT,
                    )
                ).label(label),
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
        .where(
            *[
                table_constraints.c.table_name.in_(tables),
                table_constraints.c.constraint_type == constraint_type,
            ]
        )
        .group_by(table_constraints.c.table_name)
    )


def _primary_keys(
    models: Callable, schema: str, tables: Set[str]
) -> sa.sql.Select:
    return _get_constraints(
        models,
        schema,
        tables,
        label="primary_keys",
        constraint_type="PRIMARY KEY",
    )


def _foreign_keys(
    models: Callable, schema: str, tables: Set[str]
) -> sa.sql.Select:
    return _get_constraints(
        models,
        schema,
        tables,
        label="foreign_keys",
        constraint_type="FOREIGN KEY",
    )


def create_view(
    engine: sa.engine.Engine,
    models: Callable,
    fetchall: Callable,
    index: str,
    schema: str,
    tables: Set,
    user_defined_fkey_tables: dict,
    views: List[str],
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

     table_name | primary_keys | foreign_keys     | indices
    ------------+--------------+------------------+------------
     specie     | {id}         | {id, user_id}    | {foo, bar}
     unit       | {id}         | {id, profile_id} | {foo, bar}
     structure  | {id}         | {id}             | {foo, bar}
     unit       | {id}         | {id, profile_id} | {foo, bar}
     structure  | {id}         | {id}             | {foo, bar}
    """

    rows: dict = {}
    if MATERIALIZED_VIEW in views:
        for table_name, primary_keys, foreign_keys, indices in fetchall(
            sa.select(["*"]).select_from(
                sa.text(f"{schema}.{MATERIALIZED_VIEW}")
            )
        ):
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                },
            )
            if primary_keys:
                rows[table_name]["primary_keys"] = set(primary_keys)
            if foreign_keys:
                rows[table_name]["foreign_keys"] = set(foreign_keys)
            if indices:
                rows[table_name]["indices"] = set(indices)

        engine.execute(DropView(schema, MATERIALIZED_VIEW))

    if schema != DEFAULT_SCHEMA:
        for table in set(tables):
            tables.add(f"{schema}.{table}")

    for table_name, columns in fetchall(_primary_keys(models, schema, tables)):
        rows.setdefault(
            table_name,
            {"primary_keys": set(), "foreign_keys": set(), "indices": set()},
        )
        if columns:
            rows[table_name]["primary_keys"] |= set(columns)
            rows[table_name]["indices"] |= set([index])

    for table_name, columns in fetchall(_foreign_keys(models, schema, tables)):
        rows.setdefault(
            table_name,
            {"primary_keys": set(), "foreign_keys": set(), "indices": set()},
        )
        if columns:
            rows[table_name]["foreign_keys"] |= set(columns)
            rows[table_name]["indices"] |= set([index])

    if user_defined_fkey_tables:
        for table_name, columns in user_defined_fkey_tables.items():
            rows.setdefault(
                table_name,
                {
                    "primary_keys": set(),
                    "foreign_keys": set(),
                    "indices": set(),
                },
            )
            if columns:
                rows[table_name]["foreign_keys"] |= set(columns)
                rows[table_name]["indices"] |= set([index])

    if not rows:
        rows.setdefault(
            None,
            {"primary_keys": set(), "foreign_keys": set(), "indices": set()},
        )

    statement = sa.select(
        sa.sql.Values(
            sa.column("table_name"),
            sa.column("primary_keys"),
            sa.column("foreign_keys"),
            sa.column("indices"),
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
                    array(fields.get("indices"))
                    if fields.get("indices")
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
        CreateIndex(
            "_idx",
            schema,
            MATERIALIZED_VIEW,
            ["table_name"],
        )
    )
    logger.debug(f"Created view: {schema}.{MATERIALIZED_VIEW}")


def is_view(
    engine: sa.engine.Engine,
    schema: str,
    table: str,
    materialized: bool = True,
) -> bool:
    column: str = "matviewname" if materialized else "viewname"
    pg_table: str = "pg_matviews" if materialized else "pg_views"
    with engine.connect() as conn:
        return (
            conn.execute(
                sa.select([sa.column(column)])
                .select_from(sa.text(pg_table))
                .where(
                    sa.and_(
                        *[
                            sa.column(column) == table,
                            sa.column("schemaname") == schema,
                        ]
                    )
                )
                .with_only_columns([sa.func.COUNT()])
                .order_by(None)
            ).scalar()
            > 0
        )
