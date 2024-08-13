"""PGSync Node class representation."""

from __future__ import annotations

import re
import threading
import typing as t
from dataclasses import dataclass

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
    SchemaError,
    TableNotInNodeError,
)


@dataclass
class ForeignKey:
    """
    A class representing a foreign key relationship between two tables.

    Attributes:
        foreign_key (Optional[dict]): A dictionary containing the parent and child table names.
        parent (str): The name of the parent table.
        child (str): The name of the child table.
    """

    foreign_key: t.Optional[dict] = None

    def __post_init__(self):
        """Initialize the ForeignKey object.

        Sets the parent and child attributes based on the values in the foreign_key dictionary.
        If the foreign_key dictionary is not provided, it is set to an empty dictionary.
        Raises a RelationshipForeignKeyError if the foreign_key dictionary does not contain
        both a parent and child key.
        """
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

    def __repr__(self):
        return self.__str__()


@dataclass
class Relationship:
    relationship: t.Optional[dict] = None

    def __post_init__(self):
        """Relationship constructor."""
        self.relationship: dict = self.relationship or dict()
        self.type: str = self.relationship.get("type")
        self.variant: str = self.relationship.get("variant")
        self.tables: t.List[str] = self.relationship.get("through_tables", [])
        self.throughs: t.List[Node] = []

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
        if len(self.tables) > 1:
            raise MultipleThroughTablesError(
                f"Multiple through tables: {self.tables}"
            )
        if self.type:
            self.type = self.type.lower()
        if self.variant:
            self.variant = self.variant.lower()
        self.foreign_key: ForeignKey = ForeignKey(
            self.relationship.get("foreign_key")
        )

    def __str__(self):
        return f"relationship: {self.variant}.{self.type}:{self.tables}"

    def __repr__(self):
        return self.__str__()


@dataclass
class Node(object):
    models: t.Callable
    table: str
    schema: str
    primary_key: t.Optional[list] = None
    label: t.Optional[str] = None
    transform: t.Optional[dict] = None
    columns: t.Optional[list] = None
    relationship: t.Optional[dict] = None
    parent: t.Optional[Node] = None
    base_tables: t.Optional[list] = None

    def __post_init__(self):
        self.model: sa.sql.Alias = self.models(self.table, self.schema)
        self.columns = self.columns or []
        self.children: t.List[Node] = []
        self.table_columns: t.List[str] = self.model.columns.keys()
        if not self.model.primary_keys:
            setattr(self.model, "primary_keys", self.primary_key)

        # columns to fetch
        self.column_names: t.List[str] = [
            column for column in self.columns if isinstance(column, str)
        ]
        if not self.column_names:
            self.column_names = [str(column) for column in self.table_columns]
            for name in ("ctid", "oid", "xmin"):
                self.column_names.remove(name)

        if self.label is None:
            self.label = self.table

        self.setup()

        self.relationship: Relationship = Relationship(self.relationship)
        self._subquery = None
        self._filters: list = []
        self._mapping: dict = {}

        for through_table in self.relationship.tables:
            self.relationship.throughs.append(
                Node(
                    models=self.models,
                    table=through_table,
                    schema=self.schema,
                    parent=self,
                    primary_key=[],
                )
            )

    def __str__(self):
        return f"Node: {self.schema}.{self.label}"

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.name)

    def setup(self):
        self.columns = []

        for column_name in self.column_names:
            tokens: t.Optional[list] = None
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

    def traverse_breadth_first(self) -> t.Generator:
        stack: t.List[Node] = [self]
        while stack:
            node: Node = stack.pop(0)
            yield node
            for child in node.children:
                stack.append(child)

    def traverse_post_order(self) -> t.Generator:
        for child in self.children:
            yield from child.traverse_post_order()
        yield self


@dataclass
class Tree(threading.local):
    models: t.Callable
    nodes: dict

    def __post_init__(self):
        self.tables: t.Set[str] = set()
        self.__nodes: t.Dict[Node] = {}
        self.__schemas: t.Set[str] = set()
        self.root: t.Optional[Node] = None
        self.build(self.nodes)

    def display(self) -> None:
        self.root.display()

    def traverse_breadth_first(self) -> t.Generator:
        return self.root.traverse_breadth_first()

    def traverse_post_order(self) -> t.Generator:
        return self.root.traverse_post_order()

    def build(self, nodes: dict) -> Node:
        if not isinstance(nodes, dict):
            raise SchemaError(
                "Incompatible schema. Please run v2 schema migration"
            )
        table: str = nodes.get("table")
        schema: str = nodes.get("schema", DEFAULT_SCHEMA)
        key: t.Tuple[str, str] = (schema, table)

        if table is None:
            raise TableNotInNodeError(f"Table not specified in node: {nodes}")

        if not set(nodes.keys()).issubset(set(NODE_ATTRIBUTES)):
            attrs = set(nodes.keys()).difference(set(NODE_ATTRIBUTES))
            raise NodeAttributeError(f"Unknown node attribute(s): {attrs}")

        node: Node = Node(
            models=self.models,
            table=table,
            schema=schema,
            primary_key=nodes.get("primary_key", []),
            label=nodes.get("label", table),
            transform=nodes.get("transform", {}),
            columns=nodes.get("columns", []),
            relationship=nodes.get("relationship", {}),
            base_tables=nodes.get("base_tables", []),
        )
        if self.root is None:
            self.root = node

        self.tables.add(node.table)
        for through in node.relationship.throughs:
            self.tables.add(through.table)

        for child in nodes.get("children", []):
            node.add_child(self.build(child))

        self.__nodes[key] = node
        self.__schemas.add(schema)
        return node

    def get_node(self, table: str, schema: str) -> Node:
        """Get node by name."""
        key: t.Tuple[str, str] = (schema, table)
        if key not in self.__nodes:
            for node in self.traverse_post_order():
                if table == node.table and schema == node.schema:
                    self.__nodes[key] = node
                    return self.__nodes[key]
                else:
                    for through in node.relationship.throughs:
                        if table == through.table and schema == through.schema:
                            self.__nodes[key] = through
                            return self.__nodes[key]
            else:
                raise RuntimeError(f"Node for {schema}.{table} not found")
        return self.__nodes[key]

    @property
    def schemas(self) -> t.Set[str]:
        return self.__schemas
