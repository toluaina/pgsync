"""PGSync Error classes."""


class RelationshipTypeError(Exception):
    """
    This error is raised if the relationship type is none of
    "One to one", "One to many" or "Many to Many"
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RelationshipVariantError(Exception):
    """
    This error is raised if the relationship variant is not one of
    "Scalar" or "Object"
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RelationshipAttributeError(Exception):
    """
    This error is raised if the relationship attribute is not one of
    "type" or "variant"
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class LabelError(Exception):
    """
    This error is raised if the label is invalid
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class TableNotFoundError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class TableNotInNodeError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class InvalidSchemaError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class NodeAttributeError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ColumnNotFoundError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ColumnNotSpecifiedError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ForeignKeyError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RelationshipError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class IndexError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DocTypeError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class MultipleForeignKeysError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class MultipleThroughTablesError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SuperUserError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SchemaError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class CompositeKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class FetchColumnForeignKeysError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ParseLogicalSlotError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RDSError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
