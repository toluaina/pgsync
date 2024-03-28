"""PGSync views."""

import logging
import typing as t
import warnings

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.selectable import Select

from .constants import DEFAULT_SCHEMA, MATERIALIZED_VIEW

logger = logging.getLogger(__name__)


class CreateView(DDLElement):
    """
    A class representing a CREATE VIEW statement in PostgreSQL.

    Attributes:
        schema (str): The name of the schema that the view belongs to.
        name (str): The name of the view.
        selectable (Select): The SELECT statement that defines the view.
        materialized (bool): Whether the view is materialized or not. Defaults to True.
    """

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
    """
    Compiles a CREATE VIEW statement for PostgreSQL.

    Args:
        element (CreateView): The CreateView object to compile.
        compiler (PGDDLCompiler): The compiler to use.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled CREATE VIEW statement.
    """
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
    """
    A class representing a SQL statement to drop a view.

    Attributes:
    - schema (str): The name of the schema containing the view to be dropped.
    - name (str): The name of the view to be dropped.
    - materialized (bool): Whether the view to be dropped is a materialized view. Defaults to True.
    - cascade (bool): Whether to drop objects that depend on the view to be dropped. Defaults to True.
    """

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
    """
    Compiles a DROP VIEW statement for PostgreSQL.

    Args:
        element (DropView): The DropView object to compile.
        compiler (PGDDLCompiler): The compiler to use.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled DROP VIEW statement.
    """
    cascade: str = "CASCADE" if element.cascade else ""
    materialized: str = "MATERIALIZED" if element.materialized else ""
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class RefreshView(DDLElement):
    """
    A class representing a view refresh operation in PostgreSQL.

    Attributes:
    -----------
    schema : str
        The schema of the view to be refreshed.
    name : str
        The name of the view to be refreshed.
    concurrently : bool, optional
        Whether or not to refresh the view concurrently. Default is False.
    """

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
    """
    Compiles a `RefreshView` object into a SQL string that can be executed against a PostgreSQL database.

    Args:
        element (RefreshView): The `RefreshView` object to compile.
        compiler (PGDDLCompiler): The compiler to use for generating the SQL string.
        **kwargs: Additional keyword arguments to pass to the compiler.

    Returns:
        str: The compiled SQL string.
    """
    concurrently: str = "CONCURRENTLY" if element.concurrently else ""
    return (
        f"REFRESH MATERIALIZED VIEW {concurrently} "
        f'"{element.schema}"."{element.name}"'
    )


class CreateIndex(DDLElement):
    """
    A class representing a CREATE INDEX statement in SQL.

    Attributes:
    - name (str): The name of the index.
    - schema (str): The name of the schema that the index belongs to.
    - entity (str): The name of the table or view that the index is created on.
    - columns (list): A list of column names that the index is created on.
    """

    def __init__(self, name: str, schema: str, entity: str, columns: list):
        self.schema: str = schema
        self.name: str = name
        self.entity: str = entity
        self.columns: list = columns


@compiler.compiles(CreateIndex)
def compile_create_index(
    element: CreateIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a CreateIndex object into a SQL statement.

    Args:
        element (CreateIndex): The CreateIndex object to compile.
        compiler (PGDDLCompiler): The compiler to use for compilation.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled SQL statement.
    """
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.entity}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    """
    A class representing a DROP INDEX statement in SQL.

    Attributes:
    - name (str): The name of the index to be dropped.
    """

    def __init__(self, name: str):
        self.name: str = name


@compiler.compiles(DropIndex)
def compile_drop_index(
    element: DropIndex, compiler: PGDDLCompiler, **kwargs
) -> str:
    """
    Compiles a DropIndex object into a SQL string that drops the index if it exists.

    Args:
        element (DropIndex): The DropIndex object to compile.
        compiler (PGDDLCompiler): The compiler to use for compilation.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The compiled SQL string.
    """
    return f"DROP INDEX IF EXISTS {element.name}"


def _get_constraints(
    models: t.Callable,
    schema: str,
    tables: t.Set[str],
    label: str,
    constraint_type: str,
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy Select object that selects the table name and an array of column names for each table in the given set of tables that has a constraint of the given constraint type.

    Args:
        models (Callable): A callable that returns a SQLAlchemy Table object for the given table name and schema.
        schema (str): The schema to search for constraints.
        tables (Set[str]): The set of table names to search for constraints.
        label (str): The label to give to the array of column names in the result set.
        constraint_type (str): The type of constraint to search for.

    Returns:
        sa.sql.Select: A SQLAlchemy Select object that selects the table name and an array of column names for each table in the given set of tables that has a constraint of the given constraint type.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        table_constraints = models("table_constraints", "information_schema")
        key_column_usage = models("key_column_usage", "information_schema")
    return (
        sa.select(
            *[
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
    models: t.Callable, schema: str, tables: t.Set[str]
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy Select object that represents the primary keys of the specified tables in the given schema.

    Args:
        models (Callable): A callable that returns a SQLAlchemy MetaData object.
        schema (str): The name of the schema containing the tables.
        tables (Set[str]): A set of table names to retrieve primary keys for.

    Returns:
        sa.sql.Select: A SQLAlchemy Select object representing the primary keys of the specified tables.
    """
    return _get_constraints(
        models,
        schema,
        tables,
        label="primary_keys",
        constraint_type="PRIMARY KEY",
    )


def _foreign_keys(
    models: t.Callable, schema: str, tables: t.Set[str]
) -> sa.sql.Select:
    """
    Returns a SQLAlchemy SELECT statement that retrieves foreign key constraints for the specified tables in the given schema.

    Args:
        models (Callable): A callable that returns a SQLAlchemy metadata object.
        schema (str): The name of the schema to retrieve foreign key constraints from.
        tables (Set[str]): A set of table names to retrieve foreign key constraints for.

    Returns:
        sa.sql.Select: A SQLAlchemy SELECT statement that retrieves foreign key constraints for the specified tables in the given schema.
    """
    return _get_constraints(
        models,
        schema,
        tables,
        label="foreign_keys",
        constraint_type="FOREIGN KEY",
    )


def create_view(
    engine: sa.engine.Engine,
    models: t.Callable,
    fetchall: t.Callable,
    index: str,
    schema: str,
    tables: t.Set,
    user_defined_fkey_tables: dict,
    views: t.List[str],
) -> None:
    """
    This module defines a function `create_view` that creates a view describing primary_keys and foreign_keys for each table
    with an index on table_name. The view is used within the trigger function to determine what payload values to send to pg_notify.

    Args:
        engine (sa.engine.Engine): SQLAlchemy engine object.
        models (Callable): A callable that returns a list of SQLAlchemy models.
        fetchall (Callable): A callable that returns the result of a SELECT query.
        index (str): The name of the index.
        schema (str): The name of the schema.
        tables (Set): A set of table names.
        user_defined_fkey_tables (dict): A dictionary containing user-defined foreign key tables.
        views (List[str]): A list of views.

    Returns:
        None

    Raises:
        None

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
            sa.select("*").select_from(
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
        with engine.connect() as conn:
            conn.execute(DropView(schema, MATERIALIZED_VIEW))
            conn.commit()

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
                    (
                        array(fields["primary_keys"])
                        if fields.get("primary_keys")
                        else None
                    ),
                    (
                        array(fields.get("foreign_keys"))
                        if fields.get("foreign_keys")
                        else None
                    ),
                    (
                        array(fields.get("indices"))
                        if fields.get("indices")
                        else None
                    ),
                )
                for table_name, fields in rows.items()
            ]
        )
        .alias("t")
    )
    logger.debug(f"Creating view: {schema}.{MATERIALIZED_VIEW}")
    with engine.connect() as conn:
        conn.execute(CreateView(schema, MATERIALIZED_VIEW, statement))
        conn.execute(DropIndex("_idx"))
        conn.execute(
            CreateIndex(
                "_idx",
                schema,
                MATERIALIZED_VIEW,
                ["table_name"],
            )
        )
        conn.commit()
    logger.debug(f"Created view: {schema}.{MATERIALIZED_VIEW}")


def is_view(
    engine: sa.engine.Engine,
    schema: str,
    table: str,
    materialized: bool = True,
) -> bool:
    """
    Check if a given table is a view in the specified schema.

    Args:
        engine (sa.engine.Engine): SQLAlchemy engine to use for the database connection.
        schema (str): Name of the schema to check for the table.
        table (str): Name of the table to check.
        materialized (bool, optional): Whether to check for a materialized view or a regular view. Defaults to True.

    Returns:
        bool: True if the table is a view, False otherwise.
    """
    column: str = "matviewname" if materialized else "viewname"
    pg_table: str = "pg_matviews" if materialized else "pg_views"
    with engine.connect() as conn:
        return (
            conn.execute(
                sa.select(*[sa.column(column)])
                .select_from(sa.text(pg_table))
                .where(
                    sa.and_(
                        *[
                            sa.column(column) == table,
                            sa.column("schemaname") == schema,
                        ]
                    )
                )
                .with_only_columns(*[sa.func.COUNT()])
                .order_by(None)
            ).scalar()
            > 0
        )
