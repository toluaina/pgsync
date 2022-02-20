"""PGSync Node class representation."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Set

import sqlalchemy as sa
from six import string_types

from .constants import (
    DEFAULT_SCHEMA,
    JSONB_OPERATORS,
    NODE_ATTRIBUTES,
    RELATIONSHIP_ATTRIBUTES,
    RELATIONSHIP_FOREIGN_KEYS,
    RELATIONSHIP_TYPES,
    RELATIONSHIP_VARIANTS,
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


@dataclass
class ForeignKey:
    foreign_key: Optional[dict] = None

    def __post_init__(self):
        """ForeignKey constructor."""
        self.foreign_key: str = self.foreign_key or dict()
        self.parent: str = self.foreign_key.get("parent")
        self.child: str = self.foreign_key.get("child")
        if not set(self.foreign_key.keys()).issubset(
            set(RELATIONSHIP_FOREIGN_KEYS)
        ):
            raise RelationshipForeignKeyError(
                "ForeignKey Relationship must contain a parent and child."
            )
        self.parent = self.foreign_key.get("parent")
        self.child = self.foreign_key.get("child")

    def __str__(self):
        return f"foreign_key: {self.parent}:{self.child}"


@dataclass
class Relationship:
    relationship: Optional[dict] = None

    def __post_init__(self):
        """Relationship constructor."""
        self.relationship: dict = self.relationship or dict()
        self.type: str = self.relationship.get("type")
        self.variant: str = self.relationship.get("variant")
        self.through_tables: List = self.relationship.get("through_tables", [])

        if not set(self.relationship.keys()).issubset(
            set(RELATIONSHIP_ATTRIBUTES)
        ):
            attrs = set(self.relationship.keys()).difference(
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
        if len(self.through_tables) > 1:
            raise MultipleThroughTablesError(
                f"Multiple through tables: {self.through_tables}"
            )
        if self.type:
            self.type = self.type.lower()
        if self.variant:
            self.variant = self.variant.lower()
        self.foreign_key = ForeignKey(self.relationship.get("foreign_key"))

    def __str__(self):
        return (
            f"relationship: {self.variant}.{self.type}:{self.through_tables}"
        )


@dataclass
class Node(object):

    model: sa.sql.selectable.Alias
    table: str
    schema: str
    primary_key: Optional[list] = None
    label: Optional[str] = None
    transform: Optional[dict] = None
    columns: Optional[list] = None
    relationship: Optional[dict] = None
    parent: Optional[Node] = None

    def __post_init__(self):
        self.columns = self.columns or []
        self.children: List[Node] = []
        self.table_columns: List[str] = self.model.columns.keys()
        if not self.model.primary_keys:
            setattr(self.model, "primary_keys", self.primary_key)

        # columns to fetch
        self.column_names: List[str] = [
            column
            for column in self.columns
            if isinstance(column, string_types)
        ]
        if not self.column_names:
            self.column_names = [str(column) for column in self.table_columns]
            for name in ("ctid", "oid", "xmin"):
                self.column_names.remove(name)

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
                tokenized = self.model.c[tokens[0]]
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
                self.columns.append(self.model.c[column_name])

        self.relationship: Relationship = Relationship(self.relationship)
        self._subquery = None
        self._filters: list = []
        self._mapping: dict = {}

    def __str__(self):
        return f"node: {self.schema}.{self.table}"

    @property
    def primary_keys(self):
        return [
            self.model.c[str(sa.text(primary_key))]
            for primary_key in self.model.primary_keys
        ]

    @property
    def is_root(self) -> bool:
        return self.parent is None

    @property
    def name(self) -> str:
        """
        returns a fully qualified node name
        """
        return f"{self.schema}.{self.table}"

    def add_child(self, node: Node) -> None:
        """
        all nodes except the root node must have a relationship defined
        """
        node.parent: Node = self
        if not node.is_root and (
            not node.relationship.type or not node.relationship.variant
        ):
            raise RelationshipError(
                f'Relationship not present on "{node.name}"'
            )
        self.children.append(node)

    def display(self, prefix: str = "", leaf: bool = True) -> None:
        print(
            prefix, " - " if leaf else "|- ", self.table, sep=""
        )  # noqa T001
        prefix += "   " if leaf else "|  "
        for i, child in enumerate(self.children):
            leaf = i == (len(self.children) - 1)
            child.display(prefix, leaf)

    def traverse_breadth_first(self) -> Node:
        stack: List[Node] = [self]
        while stack:
            node: Node = stack.pop(0)
            yield node
            for child in node.children:
                stack.append(child)

    def traverse_post_order(self) -> Node:
        for child in self.children:
            yield from child.traverse_post_order()
        yield self


@dataclass
class Tree:
    base: "base.Base"

    def __post_init__(self):
        self.nodes: Set[str] = set()
        self.through_nodes: Set[str] = set()

    def build(self, root: dict) -> Node:

        table: str = root.get("table")
        schema: str = root.get("schema", DEFAULT_SCHEMA)

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


# TODO: deprecate this method and use get_node
def node_from_table(base, table: str, schema: str) -> Node:
    return Node(
        model=base.model(table, schema=schema),
        table=table,
        schema=schema,
        label=table,
        primary_key=[],
    )


def get_node(tree, table: str, node_dict: dict) -> Node:

    root: Node = tree.build(node_dict)
    for node in root.traverse_post_order():
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
