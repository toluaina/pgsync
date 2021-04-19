from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement


class CreateView(DDLElement):
    def __init__(self, name, selectable, materialized=True):
        self.name = name
        self.selectable = selectable
        self.materialized = materialized


@compiler.compiles(CreateView)
def compile_create_view(element, compiler, **kwargs):
    statement = compiler.sql_compiler.process(
        element.selectable,
        literal_binds=True,
    )
    materialized = 'MATERIALIZED' if element.materialized else ''
    return f'CREATE {materialized} VIEW {element.name} AS {statement}'


class DropView(DDLElement):
    def __init__(self, name, materialized=True, cascade=True):
        self.name = name
        self.materialized = materialized
        self.cascade = cascade


@compiler.compiles(DropView)
def compile_drop_view(element, compiler, **kwargs):
    materialized = 'MATERIALIZED' if element.materialized else ''
    cascade = 'CASCADE' if element.cascade else ''
    return f'DROP {materialized} VIEW IF EXISTS {element.name} {cascade}'
