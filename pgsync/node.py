"""PGSync Node class representation."""
import sqlalchemy as sa
from six import string_types

from .base import get_primary_keys
from .constants import (
    NODE_ATTRIBUTES,
    RELATIONSHIP_ATTRIBUTES,
    RELATIONSHIP_TYPES,
    RELATIONSHIP_VARIANTS,
    SCHEMA,
)
from .exc import (
    ColumnNotFoundError,
    InvalidSchemaError,
    MultipleThroughTablesError,
    NodeAttributeError,
    RelationshipAttributeError,
    RelationshipError,
    RelationshipTypeError,
    RelationshipVariantError,
    TableNotInNodeError,
)


class Node(object):
    """Node class."""

    def __init__(self, *args, **kwargs):  # noqa: C901
        """Node constructor."""
        for key, value in kwargs.items():
            setattr(self, key, value)

        if 'parent' not in self.__dict__.keys():
            self.parent = None

        if 'children' not in self.__dict__.keys():
            self.children = []

        self.table_columns = self.model.columns.keys()
        self._primary_keys = get_primary_keys(self.model)

        if not self._primary_keys:
            self._primary_keys = kwargs.get('primary_key')

        # columns to fetch
        self.column_names = [
            column for column in kwargs.get(
                'columns', []
            ) if isinstance(column, string_types)
        ]
        if not self.column_names:
            self.column_names = [
                str(column) for column in self.table_columns
            ]
            self.column_names.remove('xmin')

        if self.label is None:
            self.label = self.table
        self.columns = []

        for column_name in self.column_names:

            tokens = None

            # https://www.postgresql.org/docs/current/functions-json.html
            # NB: this will not work for multi level ops: a->b->c
            # TODO: support multi level ops
            for op in ('->', '->>', '#>', '#>>'):
                if op in column_name:
                    tokens = column_name.split(op)
                    break

            if tokens:
                self.columns.append(
                    f'{tokens[0]}_{tokens[1]}'
                )
                self.columns.append(
                    getattr(self.model.c, tokens[0])[tokens[1]]
                )
            else:
                if column_name not in self.table_columns:
                    raise ColumnNotFoundError(
                        f'Column "{column_name}" not present on '
                        f'table "{self.table}"'
                    )
                self.columns.append(column_name)
                self.columns.append(getattr(self.model.c, column_name))

        self.relationship_type = None
        self.relationship_variant = None
        self.through_tables = []
        self._subquery = None
        self._filters = []
        self._mapping = {}

        if 'relationship' in kwargs:
            relationship = kwargs.get('relationship')
            if not set(relationship.keys()).issubset(
                set(RELATIONSHIP_ATTRIBUTES)
            ):
                attrs = set(relationship.keys()).difference(
                    set(RELATIONSHIP_ATTRIBUTES)
                )
                raise RelationshipAttributeError(
                    f'Relationship attribute {attrs} is invalid.'
                )

            self.relationship_type = self._safe_get(relationship, 'type')
            self.relationship_variant = self._safe_get(relationship, 'variant')

            if (
                self.relationship_type and
                self.relationship_type not in RELATIONSHIP_TYPES
            ):
                raise RelationshipTypeError(
                    f'Relationship type "{self.relationship_type}" '
                    f'is invalid.'
                )

            if (
                self.relationship_variant and
                self.relationship_variant not in RELATIONSHIP_VARIANTS
            ):
                raise RelationshipVariantError(
                    f'Relationship variant "{self.relationship_variant}" '
                    f'is invalid.'
                )

            self.through_tables = relationship.get('through_tables', [])

            if self.through_tables and len(self.through_tables) > 1:
                raise MultipleThroughTablesError(
                    f'Multiple through tables: {self.through_tables}'
                )

    def __repr__(self):
        return f'node: {self.schema}.{self.table}'

    @property
    def primary_keys(self):
        return [
            getattr(
                self.model.c, str(sa.text(primary_key))
            ) for primary_key in self._primary_keys
        ]

    def _safe_get(self, obj, attr):
        value = obj.get(attr)
        if value is not None:
            value = value.lower()
        return value

    @property
    def is_root(self):
        return self.parent is None

    def add_child(self, node):
        # all child nodes must have a relationship defined
        node.parent = self
        if not node.is_root and (
            node.relationship_type is None or
            node.relationship_variant is None
        ):
            raise RelationshipError(
                f'Relationship not present on table '
                f'"{node.schema}.{node.table}"'
            )
        self.children.append(node)

    def display(self, prefix='', leaf=True):
        print(prefix, ' - ' if leaf else '|- ', self.table, sep='') # noqa T001
        prefix += '   ' if leaf else '|  '
        for i, child in enumerate(self.children):
            leaf = i == (len(self.children) - 1)
            child.display(prefix, leaf)


def traverse_breadth_first(root):
    stack = [root]
    while stack:
        node = stack.pop(0)
        yield node
        for child in node.children:
            stack.append(child)


def traverse_post_order(root):
    for child in root.children:
        yield from traverse_post_order(child)
    yield root


class Tree(object):

    def __init__(self, base, **kwargs):
        self.base = base
        self.nodes = set()
        self.through_nodes = set()

    def build(self, root: dict) -> Node:

        table = root.get('table')
        schema = root.get('schema', SCHEMA)

        if table is None:
            raise TableNotInNodeError(
                f'Table not specified in node: {root}'
            )
        if schema and schema not in self.base.schemas:
            raise InvalidSchemaError(
                f'Unknown schema name(s): {schema}'
            )

        if not set(root.keys()).issubset(
            set(NODE_ATTRIBUTES)
        ):
            attrs = set(root.keys()).difference(
                set(NODE_ATTRIBUTES)
            )
            raise NodeAttributeError(
                f'Unknown node attribute(s): {attrs}'
            )

        node = Node(
            model=self.base.model(table, schema=schema),
            table=table,
            schema=schema,
            primary_key=root.get('primary_key', []),
            label=root.get('label', table),
            transform=root.get('transform', {}),
            columns=root.get('columns', []),
            relationship=root.get('relationship', {}),
        )

        self.nodes.add(node.table)

        for through_table in node.through_tables:
            self.through_nodes.add(through_table)

        for child in root.get('children', []):
            if 'table' not in child:
                raise TableNotInNodeError(
                    f'Table not specified in node: {child}'
                )
            if not set(child.keys()).issubset(
                set(NODE_ATTRIBUTES)
            ):
                attrs = set(child.keys()).difference(
                    set(NODE_ATTRIBUTES)
                )
                raise NodeAttributeError(
                    f'Unknown node attribute(s): {attrs}'
                )
            node.add_child(self.build(child))
        return node
