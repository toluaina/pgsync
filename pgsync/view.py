"""PGSync views."""
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement


class CreateView(DDLElement):
    def __init__(self, schema, name, selectable, materialized=True):
        self.schema = schema
        self.name = name
        self.selectable = selectable
        self.materialized = materialized


@compiler.compiles(CreateView)
def compile_create_view(element, compiler, **kwargs):
    statement = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized = "MATERIALIZED" if element.materialized else ""
    return (
        f'CREATE {materialized} VIEW "{element.schema}"."{element.name}" AS '
        f"{statement}"
    )


class DropView(DDLElement):
    def __init__(self, schema, name, materialized=True, cascade=True):
        self.schema = schema
        self.name = name
        self.materialized = materialized
        self.cascade = cascade


@compiler.compiles(DropView)
def compile_drop_view(element, compiler, **kwargs):
    materialized = "MATERIALIZED" if element.materialized else ""
    cascade = "CASCADE" if element.cascade else ""
    return (
        f"DROP {materialized} VIEW IF EXISTS "
        f'"{element.schema}"."{element.name}" {cascade}'
    )


class CreateIndex(DDLElement):
    def __init__(self, name, schema, view, columns):
        self.schema = schema
        self.name = name
        self.view = view
        self.columns = columns


@compiler.compiles(CreateIndex)
def compile_create_index(element, compiler, **kwargs):
    return (
        f"CREATE UNIQUE INDEX {element.name} ON "
        f'"{element.schema}"."{element.view}" ({", ".join(element.columns)})'
    )


class DropIndex(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(DropIndex)
def compile_drop_index(element, compiler, **kwargs):
    return f"DROP INDEX IF EXISTS {element.name}"
