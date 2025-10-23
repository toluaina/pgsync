"""Tests for watched_columns feature."""

import pytest

from pgsync.base import subtransactions
from pgsync.constants import UPDATE
from pgsync.node import Tree, Node


@pytest.mark.usefixtures("table_creator")
class TestWatchedColumns(object):
    """Tests for watched_columns functionality."""

    @pytest.fixture(scope="function")
    def data(self, sync, book_cls, publisher_cls):
        session = sync.session

        publishers = [
            publisher_cls(
                id=1,
                name="Test Publisher",
            ),
        ]

        books = [
            book_cls(
                isbn="test-isbn",
                title="Test Book",
                description="Test Description",
                publisher_id=1,
            ),
        ]

        with subtransactions(session):
            session.add_all(publishers)
            session.add_all(books)

        yield books, publishers

        with subtransactions(session):
            session.query(book_cls).delete()
            session.query(publisher_cls).delete()

        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.search_client.close()

    def _set_tree(self, sync, watched):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "watched_columns": watched,
        }
        sync.tree = Tree(sync.models, nodes)

    def test_watched_columns_process_watched_update(self, sync, data):
        self._set_tree(sync, ["title"])
        payload = {
            "tg_op": UPDATE,
            "table": "book",
            "schema": "public",
            "old": {"isbn": "test-isbn", "title": "Test Book"},
            "new": {"isbn": "test-isbn", "title": "Updated Title"},
            "xmin": 12346, "indices": ["testdb"]
        }
        assert sync._should_skip_update_due_to_watched_columns(payload) is False

    def test_watched_columns_no_change_in_watched(self, sync, data):
        self._set_tree(sync, ["title", "description"])
        payload = {
            "tg_op": UPDATE,
            "table": "book",
            "schema": "public",
            "old": {"isbn": "test-isbn", "title": "Test Book", "description": "Test Description"},
            "new": {"isbn": "test-isbn", "title": "Test Book", "description": "Test Description"},
            "xmin": 12351, "indices": ["testdb"]
        }
        assert sync._should_skip_update_due_to_watched_columns(payload) is True

    def test_watched_columns_with_multiple_watched_columns(self, sync, data):
        self._set_tree(sync, ["title", "description"])
        payload = {
            "tg_op": UPDATE,
            "table": "book",
            "schema": "public",
            "old": {"isbn": "test-isbn", "title": "Test Book", "description": "Test Description"},
            "new": {"isbn": "test-isbn", "title": "Test Book", "description": "Updated Description"},
            "xmin": 12350, "indices": ["testdb"]
        }
        assert sync._should_skip_update_due_to_watched_columns(payload) is False

    def test_node_watched_columns_attribute(self, sync):
        """Test that Node correctly handles watched_columns attribute."""

        # Use real models from sync fixture instead of Mock
        # Test node without watched_columns
        node_data = {
            "table": "book",
            "schema": "public",
            "columns": ["isbn", "title"],
        }

        node = Node(models=sync.models, **node_data)
        assert node.watched_columns is None

        # Test node with watched_columns
        node_data_with_watched = {
            "table": "book",
            "schema": "public",
            "columns": ["isbn", "title"],
            "watched_columns": ["title", "isbn"]
        }

        node_watched = Node(models=sync.models, **node_data_with_watched)
        assert node_watched.watched_columns == ["title", "isbn"]

    def test_tree_watched_columns_tables_tracking(self, sync):
        """Test that Tree correctly tracks tables with watched_columns."""
        nodes = {
            "table": "book",
            "columns": ["isbn", "title"],
            "watched_columns": ["title"],  # This table has watched_columns
            "children": [
                {
                    "table": "publisher",
                    "columns": ["id", "name"],
                    # This table has no watched_columns
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one"
                    }
                }
            ]
        }

        # Use real models from sync fixture instead of Mock
        tree = Tree(models=sync.models, nodes=nodes)

        # Only book table should be in watched_columns_tables
        assert "book" in tree.watched_columns_tables
        assert "publisher" not in tree.watched_columns_tables
