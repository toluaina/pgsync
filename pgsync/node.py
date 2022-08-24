"""PGSync Node class representation."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple

import sqlalchemy as sa

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
        """Foreignkey constructor."""
        self.foreign_key: str = self.foreign_key or dict()
        self.parent: str = self.foreign_key.get("parent")
        self.child: str = self.foreign_key.get("child")
        if self.foreign_key:
            if sorted(self.foreign_key.keys()) != sorted(
                RELATIONSHIP_FOREIGN_KEYS
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
        self.through_tables: List[str] = self.relationship.get(
            "through_tables", []
        )
        self.primary_key: List[str] = self.relationship.get("primary_key", [])
        self.through_nodes: List[Node] = []

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
        self.foreign_key: ForeignKey = ForeignKey(
            self.relationship.get("foreign_key")
        )

    def __str__(self):
        return (
            f"relationship: {self.variant}.{self.type}:{self.through_tables}.{self.primary_key}"
        )


@dataclass
class Node(object):

    models: Callable
    table: str
    schema: str
    primary_key: Optional[list] = None
    label: Optional[str] = None
    transform: Optional[dict] = None
    columns: Optional[list] = None
    relationship: Optional[dict] = None
    parent: Optional[Node] = None
    base_tables: Optional[list] = None

    def __post_init__(self):
        self.model: sa.sql.Alias = self.models(self.table, self.schema)
        self.columns = self.columns or []
        self.children: List[Node] = []
        self.table_columns: List[str] = self.model.columns.keys()
        if not self.model.primary_keys:
            setattr(self.model, "primary_keys", self.primary_key)

        # columns to fetch
        self.column_names: List[str] = [
            column for column in self.columns if isinstance(column, str)
        ]
        if not self.column_names:
            self.column_names = [str(column) for column in self.table_columns]
            for name in ("ctid", "oid", "xmin"):
                self.column_names.remove(name)

        if self.label is None:
            self.label = self.table

        self.prepare_columns()

        self.relationship: Relationship = Relationship(self.relationship)
        self._subquery = None
        self._filters: list = []
        self._mapping: dict = {}

        for through_table in self.relationship.through_tables:
            self.relationship.through_nodes.append(
                Node(
                    models=self.models,
                    table=through_table,
                    schema=self.schema,
                    parent=self,
                    primary_key=self.relationship.primary_key,
                )
            )

    def __str__(self):
        return f"Node: {self.schema}.{self.label}"

    def prepare_columns(self):

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
                    "_".join(
                        [
                            x.replace("{", "").replace("}", "")
                            for x in tokens
                            if x not in JSONB_OPERATORS
                        ]
                    )
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
        """Returns a fully qualified node name."""
        return f"{self.schema}.{self.table}"

    def add_child(self, node: Node) -> None:
        """All nodes except the root node must have a relationship defined."""
        node.parent: Node = self
        if not node.is_root and (
            not node.relationship.type or not node.relationship.variant
        ):
            raise RelationshipError(
                f'Relationship not present on "{node.name}"'
            )
        if node not in self.children:
            self.children.append(node)

    def display(self, prefix: str = "", leaf: bool = True) -> None:
        print(
            prefix,
            " - " if leaf else "|- ",
            f"{self.schema}.{self.label}",
            sep="",
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

    models: Callable

    def __post_init__(self):
        self.tables: Set[str] = set()
        self.__nodes: Dict[Node] = {}

    def build(self, data: dict) -> Node:

        table: str = data.get("table")
        schema: str = data.get("schema", DEFAULT_SCHEMA)
        key: Tuple[str, str] = (schema, table)

        if table is None:
            raise TableNotInNodeError(f"Table not specified in node: {data}")

        if not set(data.keys()).issubset(set(NODE_ATTRIBUTES)):
            attrs = set(data.keys()).difference(set(NODE_ATTRIBUTES))
            raise NodeAttributeError(f"Unknown node attribute(s): {attrs}")

        node = Node(
            models=self.models,
            table=table,
            schema=schema,
            primary_key=data.get("primary_key", []),
            label=data.get("label", table),
            transform=data.get("transform", {}),
            columns=data.get("columns", []),
            relationship=data.get("relationship", {}),
            base_tables=data.get("base_tables", []),
        )

        self.tables.add(node.table)

        for child in data.get("children", []):
            node.add_child(self.build(child))

        self.__nodes[key] = node
        return node

    def get_node(self, root: Node, table: str, schema: str) -> Node:
        """Get node by name."""
        key: Tuple[str, str] = (schema, table)
        if key not in self.__nodes:
            for node in root.traverse_post_order():
                if table == node.table and schema == node.schema:
                    self.__nodes[key] = node
                    return self.__nodes[key]
                else:
                    for through_node in node.relationship.through_nodes:
                        if (
                            table == through_node.table
                            and schema == through_node.schema
                        ):
                            self.__nodes[key] = through_node
                            return self.__nodes[key]
            else:
                raise RuntimeError(f"Node for {schema}.{table} not found")
        return self.__nodes[key]
