"""PGSync views."""
from typing import AnyStr, List

from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql.selectable import Select


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
    materialized: bool = "MATERIALIZED" if element.materialized else ""
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
    element: CreateView, compiler: PGDDLCompiler, **kwargs
) -> str:
    materialized: bool = "MATERIALIZED" if element.materialized else ""
    cascade: bool = "CASCADE" if element.cascade else ""
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class CreateIndex(DDLElement):
    def __init__(self, name: str, schema: str, view: str, columns: List):
        self.schema: str = schema
        self.name: str = name
        self.view: str = view
        self.columns: List = columns


@compiler.compiles(CreateIndex)
def compile_create_index(
    element: CreateView, compiler: PGDDLCompiler, **kwargs
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
    element: CreateView, compiler: PGDDLCompiler, **kwargs
) -> str:
    return f"DROP INDEX IF EXISTS {element.name}"
