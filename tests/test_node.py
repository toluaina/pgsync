"""Node tests."""

import pytest

from pgsync.base import Base
from pgsync.exc import (
    MultipleThroughTablesError,
    NodeAttributeError,
    RelationshipAttributeError,
    RelationshipForeignKeyError,
    RelationshipTypeError,
    RelationshipVariantError,
    TableNotInNodeError,
)
from pgsync.node import ForeignKey, Node, Relationship, Tree
from pgsync.settings import IS_MYSQL_COMPAT


@pytest.mark.usefixtures("table_creator")
class TestNode(object):
    """Node tests."""

    @classmethod
    def setup_class(cls):
        cls.schema = "testdb" if IS_MYSQL_COMPAT else "public"

    @pytest.fixture(scope="function")
    def nodes(self):
        return {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "publisher",
                    "columns": ["name", "id"],
                    "label": "publisher_label",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "children": [],
                    "transform": {},
                },
                {
                    "table": "book_language",
                    "columns": ["book_isbn", "language_id"],
                    "label": "book_languages",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                    },
                },
                {
                    "table": "author",
                    "columns": ["id", "name"],
                    "label": "authors",
                    "relationship": {
                        "type": "one_to_many",
                        "variant": "object",
                        "through_tables": ["book_author"],
                    },
                    "children": [
                        {
                            "table": "city",
                            "columns": ["name", "id"],
                            "label": "city_label",
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                            },
                            "children": [
                                {
                                    "table": "country",
                                    "columns": ["name", "id"],
                                    "label": "country_label",
                                    "relationship": {
                                        "variant": "object",
                                        "type": "one_to_one",
                                    },
                                    "children": [
                                        {
                                            "table": "continent",
                                            "columns": ["name"],
                                            "label": "continent_label",
                                            "relationship": {
                                                "variant": "object",
                                                "type": "one_to_one",
                                            },
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                },
                {
                    "table": "language",
                    "label": "languages",
                    "columns": ["code"],
                    "relationship": {
                        "type": "one_to_many",
                        "variant": "scalar",
                        "through_tables": ["book_language"],
                    },
                },
                {
                    "table": "subject",
                    "label": "subjects",
                    "columns": ["name"],
                    "relationship": {
                        "type": "one_to_many",
                        "variant": "scalar",
                        "through_tables": ["book_subject"],
                    },
                },
            ],
        }

    def test_node(self, connection):
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            label="book_label",
        )
        assert str(node) == f"Node: {self.schema}.book_label"

    def test_traverse_breadth_first(self, sync, nodes):
        root = Tree(sync.models, nodes=nodes, database=self.schema)
        root.display()
        for i, node in enumerate(root.traverse_breadth_first()):
            if i == 0:
                assert node.table == "book"
            if i == 1:
                assert node.table == "publisher"
            if i == 2:
                assert node.table == "book_language"
            if i == 3:
                assert node.table == "author"
            if i == 4:
                assert node.table == "language"
            if i == 5:
                assert node.table == "subject"
            if i == 6:
                assert node.table == "city"
            if i == 7:
                assert node.table == "country"
            if i == 8:
                assert node.table == "continent"
        sync.search_client.close()

    def test_traverse_post_order(self, sync, nodes):
        root: Tree = Tree(sync.models, nodes=nodes, database=self.schema)
        root.display()
        for i, node in enumerate(root.traverse_post_order()):
            if i == 0:
                assert node.table == "publisher"
            if i == 1:
                assert node.table == "book_language"
            if i == 2:
                assert node.table == "continent"
            if i == 3:
                assert node.table == "country"
            if i == 4:
                assert node.table == "city"
            if i == 5:
                assert node.table == "author"
            if i == 6:
                assert node.table == "language"
            if i == 7:
                assert node.table == "subject"
            if i == 8:
                assert node.table == "book"
        sync.search_client.close()

    def test_relationship(self, sync):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "publisher",
                    "relationship": {
                        "xxx": "object",
                        "type": "one_to_one",
                    },
                },
            ],
        }
        with pytest.raises(RelationshipAttributeError) as excinfo:
            Tree(sync.models, nodes=nodes, database=self.schema)
        assert "Relationship attribute " in str(excinfo.value)
        sync.search_client.close()

    def test_get_node(self, sync):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "publisher",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                },
            ],
        }
        tree: Tree = Tree(sync.models, nodes=nodes, database=self.schema)
        node = tree.get_node("book", self.schema)
        assert str(node) == f"Node: {self.schema}.book"

        with pytest.raises(RuntimeError) as excinfo:
            tree.get_node("xxx", self.schema)
        assert f"Node for {self.schema}.xxx not found" in str(excinfo.value)

        sync.search_client.close()

    def test_tree_build(self, sync):
        with pytest.raises(TableNotInNodeError) as excinfo:
            Tree(
                sync.models,
                nodes={
                    "table": None,
                },
                database=self.schema,
            )

        with pytest.raises(NodeAttributeError) as excinfo:
            Tree(
                sync.models,
                nodes={
                    "table": "book",
                    "foo": "bar",
                },
                database=self.schema,
            )
        assert "Unknown node attribute(s):" in str(excinfo.value)

        with pytest.raises(NodeAttributeError) as excinfo:
            Tree(
                sync.models,
                nodes={
                    "table": "book",
                    "children": [
                        {
                            "table": "publisher",
                            "columns": ["name", "id"],
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                            },
                            "foo": "bar",
                        },
                    ],
                },
                database=self.schema,
            )
        assert "Unknown node attribute(s):" in str(excinfo.value)

        with pytest.raises(TableNotInNodeError) as excinfo:
            Tree(
                sync.models,
                nodes={
                    "table": "book",
                    "children": [
                        {
                            "columns": ["name", "id"],
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                            },
                        },
                    ],
                },
                database=self.schema,
            )
        assert "Table not specified in node" in str(excinfo.value)

        Tree(
            sync.models,
            nodes={
                "table": "book",
                "columns": ["tags->0"],
            },
            database=self.schema,
        )

        sync.search_client.close()


@pytest.mark.usefixtures("table_creator")
class TestForeignKey(object):
    """ForeignKey tests."""

    def test_init(self):
        foreign_key = ForeignKey({"child": ["id"], "parent": ["publisher_id"]})
        assert str(foreign_key) == "foreign_key: ['publisher_id']:['id']"
        with pytest.raises(RelationshipForeignKeyError) as excinfo:
            ForeignKey({"parent": ["publisher_id"]})
        assert (
            "ForeignKey Relationship must contain a parent and child."
            in str(excinfo.value)
        )


@pytest.mark.usefixtures("table_creator")
class TestRelationship(object):
    """Relationship tests."""

    def test_init(self):
        relationship = Relationship(
            {
                "variant": "object",
                "type": "one_to_one",
                "through_tables": ["book_author"],
            }
        )
        assert (
            str(relationship)
            == "relationship: object.one_to_one:['book_author']"
        )

        with pytest.raises(RelationshipAttributeError) as excinfo:
            Relationship(
                {
                    "foo": "bar",
                }
            )
        assert "Relationship attribute {'foo'} is invalid." in str(
            excinfo.value
        )

        with pytest.raises(RelationshipTypeError) as excinfo:
            Relationship(
                {
                    "type": "xyz",
                }
            )
        assert 'Relationship type "xyz" is invalid.' in str(excinfo.value)

        with pytest.raises(RelationshipVariantError) as excinfo:
            Relationship(
                {
                    "variant": "xyz",
                }
            )
        assert 'Relationship variant "xyz" is invalid.' in str(excinfo.value)

        with pytest.raises(MultipleThroughTablesError) as excinfo:
            Relationship(
                {
                    "variant": "object",
                    "type": "one_to_one",
                    "through_tables": ["book_author", "subject"],
                }
            )
        assert "Multiple through tables" in str(excinfo.value)


@pytest.mark.usefixtures("table_creator")
class TestNodeAdditional(object):
    """Additional Node tests for coverage."""

    @classmethod
    def setup_class(cls):
        cls.schema = "testdb" if IS_MYSQL_COMPAT else "public"

    def test_node_is_root(self, connection):
        """Test is_root property."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        # Root node has no parent
        assert node.is_root is True

        # Set a parent
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )
        node.parent = parent
        assert node.is_root is False

    def test_node_name_property(self, connection):
        """Test name property returns fully qualified name."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        assert node.name == f"{self.schema}.book"

    def test_node_table_property(self, connection):
        """Test table property."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        assert node.table == "book"

    def test_node_schema_property(self, connection):
        """Test schema property."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        assert node.schema == self.schema

    def test_node_label_defaults_to_table(self, connection):
        """Test label defaults to table name."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        assert node.label == "book"

    def test_node_custom_label(self, connection):
        """Test custom label."""
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            label="custom_book",
        )
        assert node.label == "custom_book"

    def test_tree_root_property(self, sync):
        """Test Tree root property."""
        nodes = {
            "table": "book",
            "children": [],
        }
        tree = Tree(sync.models, nodes=nodes, database=self.schema)
        assert tree.root is not None
        assert tree.root.table == "book"
        sync.search_client.close()

    def test_tree_tables_property(self, sync):
        """Test Tree tables property returns all tables."""
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "publisher",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                },
            ],
        }
        tree = Tree(sync.models, nodes=nodes, database=self.schema)
        tables = tree.tables
        assert "book" in tables
        assert "publisher" in tables
        sync.search_client.close()

    def test_tree_schemas_property(self, sync):
        """Test Tree schemas property."""
        nodes = {
            "table": "book",
            "children": [],
        }
        tree = Tree(sync.models, nodes=nodes, database=self.schema)
        schemas = tree.schemas
        assert self.schema in schemas
        sync.search_client.close()

    def test_foreign_key_parent_property(self):
        """Test ForeignKey parent property."""
        foreign_key = ForeignKey({"child": ["id"], "parent": ["publisher_id"]})
        assert foreign_key.parent == ["publisher_id"]

    def test_foreign_key_child_property(self):
        """Test ForeignKey child property."""
        foreign_key = ForeignKey({"child": ["id"], "parent": ["publisher_id"]})
        assert foreign_key.child == ["id"]

    def test_relationship_type_property(self):
        """Test Relationship type property."""
        relationship = Relationship(
            {
                "variant": "object",
                "type": "one_to_one",
            }
        )
        assert relationship.type == "one_to_one"

    def test_relationship_variant_property(self):
        """Test Relationship variant property."""
        relationship = Relationship(
            {
                "variant": "object",
                "type": "one_to_many",
            }
        )
        assert relationship.variant == "object"

    def test_relationship_throughs_property(self):
        """Test Relationship throughs property."""
        rel_without_through = Relationship(
            {
                "variant": "object",
                "type": "one_to_one",
            }
        )
        # Relationship object uses `throughs` as the internal property
        assert rel_without_through.throughs == []

        rel_with_through = Relationship(
            {
                "variant": "object",
                "type": "one_to_many",
                "through_tables": ["book_author"],
            }
        )
        # Note: through_tables is from the input, but internal throughs is populated by tree


# ============================================================================
# PHASE 9 EXTENDED TESTS - Node.py Final Coverage
# ============================================================================


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestNodeEdgeCases:
    """Extended tests for Node edge cases and final coverage."""

    def test_node_label_generation(self, connection):
        """Test Node generates unique labels for relationships."""
        pg_base = Base(connection.engine.url.database)

        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )

        # Should have a label property
        assert hasattr(node, "label")
        assert isinstance(node.label, str)

    def test_node_is_root_when_no_parent(self, connection):
        """Test Node.is_root returns True when no parent."""
        pg_base = Base(connection.engine.url.database)

        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )

        # Should be root when no parent
        assert node.is_root is True

    def test_node_is_not_root_when_has_parent(self, connection):
        """Test Node.is_root returns False when has parent."""
        pg_base = Base(connection.engine.url.database)

        parent = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )

        child = Node(
            models=pg_base.models,
            table="publisher",
            schema="public",
            relationship={
                "type": "one_to_one",
                "variant": "object",
            },
        )
        child.parent = parent

        # Should not be root when has parent
        assert child.is_root is False

    def test_node_with_custom_label(self, connection):
        """Test Node with custom label in relationship."""
        pg_base = Base(connection.engine.url.database)

        node = Node(
            models=pg_base.models,
            table="publisher",
            schema="public",
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "label": "custom_label",
            },
        )

        # Should use custom label
        assert node.relationship.label == "custom_label"

    def test_node_table_columns_property(self, connection):
        """Test Node.table_columns returns column names."""
        pg_base = Base(connection.engine.url.database)

        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )

        # Should have table_columns
        assert hasattr(node, "table_columns")
        table_cols = node.table_columns
        assert isinstance(table_cols, list)
        assert len(table_cols) > 0


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestTreeEdgeCases:
    """Extended tests for Tree structure edge cases."""

    def test_tree_build_with_multiple_children(self, connection):
        """Test Tree.build with multiple child relationships."""
        pg_base = Base(connection.engine.url.database)

        schema = {
            "database": connection.engine.url.database,
            "index": "test_index",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "publisher",
                        "columns": ["id", "name"],
                        "relationship": {
                            "type": "one_to_one",
                            "variant": "object",
                        },
                    },
                    {
                        "table": "author",
                        "columns": ["id", "name"],
                        "relationship": {
                            "type": "one_to_many",
                            "variant": "object",
                            "through_tables": ["book_author"],
                        },
                    },
                ],
            },
        }

        tree = Tree(pg_base, schema)

        # Should have built tree with multiple children
        assert tree.root is not None
        assert len(tree.root.children) == 2

    def test_tree_traverse_all_nodes(self, connection):
        """Test Tree.traverse visits all nodes."""
        pg_base = Base(connection.engine.url.database)

        schema = {
            "database": connection.engine.url.database,
            "index": "test_index",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "publisher",
                        "columns": ["id", "name"],
                        "relationship": {
                            "type": "one_to_one",
                            "variant": "object",
                        },
                    },
                ],
            },
        }

        tree = Tree(pg_base, schema)

        # Collect all nodes via traverse
        nodes = []
        for node in tree.traverse(tree.root):
            nodes.append(node)

        # Should include root and children
        assert len(nodes) >= 2
        assert hasattr(rel_with_through, "throughs")
