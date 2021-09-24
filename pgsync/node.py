"""PGSync Node class representation."""
import re
from typing import List, Optional

import sqlalchemy as sa
from six import string_types

from .constants import (
    JSONB_OPERATORS,
    NODE_ATTRIBUTES,
    RELATIONSHIP_ATTRIBUTES,
    RELATIONSHIP_FOREIGN_KEYS,
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
    RelationshipForeignKeyError,
    RelationshipTypeError,
    RelationshipVariantError,
    TableNotInNodeError,
)


def _safe_get(obj, attr):
    value = obj.get(attr)
    if value is not None:
        value = value.lower()
    return value


class ForeignKey(object):
    def __init__(self, foreign_key: Optional[str] = None):
        """ForeignKey constructor."""
        foreign_key: str = foreign_key or dict()
        self.parent: str = foreign_key.get("parent")
        self.child: str = foreign_key.get("child")
        if not set(foreign_key.keys()).issubset(
            set(RELATIONSHIP_FOREIGN_KEYS)
        ):
            raise RelationshipForeignKeyError(
                "Relationship ForeignKey must contain a parent and child."
            )
        self.parent = foreign_key.get("parent")
        self.child = foreign_key.get("child")

    def __str__(self):
        return f"foreign_key: {self.parent}:{self.child}"

    def __repr__(self):
        return self.__str__()


class Relationship(object):
    def __init__(self, relationship=None):
        """Relationship constructor."""
        relationship: dict = relationship or dict()
        self.type: str = relationship.get("type")
        self.variant: str = relationship.get("variant")
        self.through_tables: List = relationship.get("through_tables", [])

        if not set(relationship.keys()).issubset(set(RELATIONSHIP_ATTRIBUTES)):
            attrs = set(relationship.keys()).difference(
                set(RELATIONSHIP_ATTRIBUTES)
            )
            raise RelationshipAttributeError(
                f"Relationship attribute {attrs} is invalid."
            )
        if self.type and self.type not in RELATIONSHIP_TYPES:
            raise RelationshipTypeError(
                f'Relationship type "{self.type}" is invalid.'
            )
        if self.variant and self.variant not in RELATIONSHIP_VARIANTS:
            raise RelationshipVariantError(
                f'Relationship variant "{self.variant}" is invalid.'
            )
        if self.through_tables and len(self.through_tables) > 1:
            raise MultipleThroughTablesError(
                f"Multiple through tables: {self.through_tables}"
            )
        self.type = _safe_get(relationship, "type")
        self.variant = _safe_get(relationship, "variant")
        self.through_tables = relationship.get("through_tables", [])
        self.foreign_key = ForeignKey(relationship.get("foreign_key"))

    def __str__(self):
        return (
            f"relationship: {self.variant}.{self.type}:{self.through_tables}"
        )

    def __repr__(self):
        return self.__str__()


class Node(object):
    """Node class."""

    def __init__(self, *args, **kwargs):  # noqa: C901
        """Node constructor."""
        for key, value in kwargs.items():
            setattr(self, key, value)

        if "parent" not in self.__dict__.keys():
            self.parent = None

        if "children" not in self.__dict__.keys():
            self.children = []

        self.table_columns = self.model.columns.keys()

        if not self.model.primary_keys:
            setattr(self.model, "primary_keys", kwargs.get("primary_key"))

        # columns to fetch
        self.column_names = [
            column
            for column in kwargs.get("columns", [])
            if isinstance(column, string_types)
        ]
        if not self.column_names:
            self.column_names = [str(column) for column in self.table_columns]
            self.column_names.remove("xmin")
            self.column_names.remove("oid")

        if self.label is None:
            self.label = self.table
        self.columns = []

        for column_name in self.column_names:

            tokens = None
            if any(op in column_name for op in JSONB_OPERATORS):
                tokens = re.split(
                    f"({'|'.join(JSONB_OPERATORS)})",
                    column_name,
                )

            if tokens:
                tokenized = getattr(self.model.c, tokens[0])
                for token in tokens[1:]:
                    if token in JSONB_OPERATORS:
                        tokenized = tokenized.op(token)
                        continue
                    if token.isdigit():
                        token = int(token)
                    tokenized = tokenized(token)
                self.columns.append(
                    "_".join([x for x in tokens if x not in JSONB_OPERATORS])
                )
                self.columns.append(tokenized)
                # compiled_query(self.columns[-1], 'JSONB Query')

            else:
                if column_name not in self.table_columns:
                    raise ColumnNotFoundError(
                        f'Column "{column_name}" not present on '
                        f'table "{self.table}"'
                    )
                self.columns.append(column_name)
                self.columns.append(getattr(self.model.c, column_name))

        self.relationship = Relationship(kwargs.get("relationship"))
        self._subquery = None
        self._filters = []
        self._mapping = {}

    def __str__(self):
        return f"node: {self.schema}.{self.table}"

    def __repr__(self):
        return self.__str__()

    @property
    def primary_keys(self):
        return [
            getattr(self.model.c, str(sa.text(primary_key)))
            for primary_key in self.model.primary_keys
        ]

    @property
    def is_root(self):
        return self.parent is None

    @property
    def name(self):
        """
        returns a fully qualified node name
        """
        return f"{self.schema}.{self.table}"

    def add_child(self, node):
        """
        all nodes except the root node must have a relationship defined
        """
        node.parent = self
        if not node.is_root and (
            not node.relationship.type or not node.relationship.variant
        ):
            raise RelationshipError(
                f'Relationship not present on "{node.name}"'
            )
        self.children.append(node)

    def display(self, prefix: str = "", leaf: bool = True):
        print(
            prefix, " - " if leaf else "|- ", self.table, sep=""
        )  # noqa T001
        prefix += "   " if leaf else "|  "
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

        table: str = root.get("table")
        schema: str = root.get("schema", SCHEMA)

        if table is None:
            raise TableNotInNodeError(f"Table not specified in node: {root}")
        if schema and schema not in self.base.schemas:
            raise InvalidSchemaError(f"Unknown schema name(s): {schema}")

        if not set(root.keys()).issubset(set(NODE_ATTRIBUTES)):
            attrs = set(root.keys()).difference(set(NODE_ATTRIBUTES))
            raise NodeAttributeError(f"Unknown node attribute(s): {attrs}")

        node = Node(
            model=self.base.model(table, schema=schema),
            table=table,
            schema=schema,
            primary_key=root.get("primary_key", []),
            label=root.get("label", table),
            transform=root.get("transform", {}),
            columns=root.get("columns", []),
            relationship=root.get("relationship", {}),
        )

        self.nodes.add(node.table)

        for through_table in node.relationship.through_tables:
            self.through_nodes.add(through_table)

        for child in root.get("children", []):
            if "table" not in child:
                raise TableNotInNodeError(
                    f"Table not specified in node: {child}"
                )
            if not set(child.keys()).issubset(set(NODE_ATTRIBUTES)):
                attrs = set(child.keys()).difference(set(NODE_ATTRIBUTES))
                raise NodeAttributeError(f"Unknown node attribute(s): {attrs}")
            node.add_child(self.build(child))
        return node


# TODO: deprecate this method
def node_from_table(base, table, schema):
    return Node(
        model=base.model(table, schema=schema),
        table=table,
        schema=schema,
        label=table,
        primary_key=[],
    )


def get_node(tree, table, node_dict):

    root = tree.build(node_dict)
    for node in traverse_post_order(root):
        if table == node.table:
            return node
        elif table in node.relationship.through_tables:
            return Node(
                model=tree.base.model(table, schema=node.schema),
                table=table,
                label=table,
                schema=node.schema,
                primary_key=[],
                parent=node,
            )
    else:
        raise RuntimeError(f"Node for {table} not found")
