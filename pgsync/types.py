import sqlalchemy.types as types


class TupleIdentifierType(types.UserDefinedType):
    cache_ok: bool = True

    def get_col_spec(self, **kw) -> str:
        return "TID"

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process
