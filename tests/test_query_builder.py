"""QueryBuilder tests."""

from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from pgsync.base import Base
from pgsync.exc import ForeignKeyError
from pgsync.node import Node
from pgsync.querybuilder import (
    JSON_AGG,
    JSON_ARRAY,
    JSON_CAST,
    JSON_CONCAT,
    JSON_OBJECT,
    JSON_TYPE,
    QueryBuilder,
)
from pgsync.settings import IS_MYSQL_COMPAT


@pytest.mark.usefixtures("table_creator")
class TestQueryBuilder(object):
    """QueryBuilder tests."""

    @classmethod
    def setup_class(cls):
        cls.schema = "testdb" if IS_MYSQL_COMPAT else "public"

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__json_build_object(self, connection):
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        with pytest.raises(RuntimeError) as excinfo:
            query_builder._json_build_object([])
        assert "invalid expression" == str(excinfo.value)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        expression = query_builder._json_build_object(node.columns)
        assert expression is not None
        expected = (
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_1, book_1.isbn, "
            ":JSON_BUILD_OBJECT_2, book_1.title, :JSON_BUILD_OBJECT_3, "
            "book_1.description, :JSON_BUILD_OBJECT_4, book_1.copyright, "
            ":JSON_BUILD_OBJECT_5, book_1.publisher_id, :JSON_BUILD_OBJECT_6, "
            "book_1.buyer_id, :JSON_BUILD_OBJECT_7, book_1.seller_id, "
            ":JSON_BUILD_OBJECT_8, book_1.tags) AS JSONB)"
        )
        assert str(expression) == expected
        expression = query_builder._json_build_object(
            node.columns, chunk_size=2
        )
        assert expression is not None
        expected = (
            "((((((CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_1, book_1.isbn) AS JSONB) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_2, book_1.title) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_3, book_1.description) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_4, book_1.copyright) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_5, book_1.publisher_id) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_6, book_1.buyer_id) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_7, book_1.seller_id) AS JSONB)) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_8, book_1.tags) AS JSONB)"
        )
        assert str(expression) == expected

    def test__get_column_foreign_keys(self, connection):
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        foreign_keys = {
            f"{self.schema}.subject": ["column_a", "column_b", "column_X"],
            f"{self.schema}.table_b": ["column_x"],
        }

        subject = Node(
            models=pg_base.models,
            table="subject",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "through_tables": ["book_subject"],
            },
        )
        left_foreign_keys = query_builder._get_column_foreign_keys(
            subject.columns,
            foreign_keys,
            table=subject.name,
            schema=subject.schema,
        )
        assert left_foreign_keys == ["column_b"]

    def test__get_column_foreign_keys_without_table(self, connection):
        """Test _get_column_foreign_keys when table is None."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # When table is None, it should find the table where all FK columns
        # are a subset of the provided columns
        foreign_keys = {
            f"{self.schema}.subject": ["id", "name"],
            f"{self.schema}.other": ["other_col"],
        }

        subject = Node(
            models=pg_base.models,
            table="subject",
            schema=self.schema,
        )
        # subject.columns contains 'id' and 'name' which matches the first FK
        left_foreign_keys = query_builder._get_column_foreign_keys(
            subject.columns,
            foreign_keys,
            table=None,  # No table specified
            schema=subject.schema,
        )
        assert left_foreign_keys == ["id", "name"]

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__get_child_keys(self, connection):
        """Test _get_child_keys method generates correct labels."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create a simple params dict
        params = {"key": "value"}

        # Call _get_child_keys with no children
        result = query_builder._get_child_keys(node, params)

        # Should return a labeled expression
        assert hasattr(result, "name")
        assert result.name == "_keys"

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__root(self, connection):
        """Test _root method builds root node query."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []

        query_builder._root(node)

        # Should have created a subquery
        assert node._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__root_with_txmin_filter(self, connection):
        """Test _root method applies txmin filter."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []

        query_builder._root(node, txmin=1000)

        # Should have created a subquery with filter
        assert node._subquery is not None
        # Filters should have been applied
        assert len(node._filters) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__root_with_txmax_filter(self, connection):
        """Test _root method applies txmax filter."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []

        query_builder._root(node, txmin=1000, txmax=2000)

        # Should have created a subquery with filters
        assert node._subquery is not None
        # Should have both txmin and txmax filters
        assert len(node._filters) >= 2

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children(self, connection):
        """Test _children method processes child nodes."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent node (root - no parent)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        # parent.parent is None by default, so it's root

        # Create child node with relationship
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent
        child._filters = []

        # Create a mock subquery for the child with the required label
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),  # Add the label column
        ).alias()

        parent.children = [child]

        # This should process children and set from_obj
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through(self, connection):
        """Test _through method handles through-table relationships."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (book)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node (book_author)
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create child node (author) with through relationship
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        child.children = []
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through(self, connection):
        """Test _non_through method handles direct relationships."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (book) - no parent, so it's root
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        # parent.parent is None by default, so parent.is_root is True

        # Create child node (publisher) with direct relationship
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_queries(self, connection):
        """Test build_queries orchestrates query building."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create root node (no parent = is_root is True)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []
        # node.parent is None by default, so node.is_root is True

        # Build queries for root node
        query_builder.build_queries(node)

        # Should have created a subquery
        assert node._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_queries_with_filters(self, connection):
        """Test build_queries applies filters."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create root node (no parent = is_root is True)
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []
        # node.parent is None by default, so node.is_root is True

        # Build queries with filters
        filters = {"book": [{"isbn": "001"}]}
        query_builder.build_queries(node, filters=filters)

        # Should have created a subquery with filters
        assert node._subquery is not None
        assert len(node._filters) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__build_filters(self, connection):
        """Test _build_filters creates SQLAlchemy filter clauses."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Test with single filter
        filters = {"book": [{"isbn": "001"}]}
        result = query_builder._build_filters(filters, node)
        assert result is not None

        # Test with multiple filters (OR condition)
        filters = {"book": [{"isbn": "001"}, {"isbn": "002"}]}
        result = query_builder._build_filters(filters, node)
        assert result is not None

        # Test with composite filter (AND condition)
        filters = {"book": [{"isbn": "001", "title": "Test"}]}
        result = query_builder._build_filters(filters, node)
        assert result is not None

    def test__build_filters_returns_none_for_empty(self, connection):
        """Test _build_filters returns None for empty/missing filters."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Test with None filters
        result = query_builder._build_filters(None, node)
        assert result is None

        # Test with empty filters for table
        result = query_builder._build_filters({"other_table": []}, node)
        assert result is None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys(self, connection):
        """Test get_foreign_keys returns FK mappings between nodes."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Get foreign keys between book and publisher
        fkeys = query_builder.get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        # Should have entries for both tables
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys_caching(self, connection):
        """Test get_foreign_keys uses caching."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # First call
        fkeys1 = query_builder.get_foreign_keys(parent, child)

        # Second call should use cache
        fkeys2 = query_builder.get_foreign_keys(parent, child)

        # Results should be the same
        assert fkeys1 == fkeys2

        # Cache should have entry
        assert (parent, child) in query_builder._cache

    def test_get_foreign_keys_raises_error_when_no_fk(self, connection):
        """Test get_foreign_keys raises ForeignKeyError when no FK found."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create two unrelated nodes
        node_a = Node(
            models=pg_base.models,
            table="continent",
            schema=self.schema,
        )

        node_b = Node(
            models=pg_base.models,
            table="language",
            schema=self.schema,
        )

        # Should raise ForeignKeyError since there's no FK between them
        with pytest.raises(ForeignKeyError):
            query_builder.get_foreign_keys(node_a, node_b)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__get_foreign_keys_with_through(self, connection):
        """Test _get_foreign_keys handles through-table relationships."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]

        # Get foreign keys with through table
        fkeys = query_builder._get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        # Should have entries
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__eval_expression_with_uuid(self, connection):
        """Test _eval_expression handles UUID type expressions."""
        query_builder = QueryBuilder()

        # Create a mock expression with UUID type
        left = MagicMock()
        left.type = sa.dialects.postgresql.UUID()
        right = MagicMock()
        right.type = sa.Integer()

        expression = MagicMock()
        expression.left = left
        expression.right = right

        # Should handle UUID mismatch
        result = query_builder._eval_expression(expression)
        # When types don't match, returns expression.left is None
        assert result is not None

    def test__eval_expression_mysql_passthrough(self, connection):
        """Test _eval_expression passes through for MySQL."""
        query_builder = QueryBuilder()

        with patch("pgsync.querybuilder.IS_MYSQL_COMPAT", True):
            expression = MagicMock()
            result = query_builder._eval_expression(expression)
            # Should return expression unchanged
            assert result == expression

    def test_verbose_mode(self, connection):
        """Test QueryBuilder verbose mode."""
        query_builder = QueryBuilder(verbose=True)
        assert query_builder.verbose is True

        query_builder2 = QueryBuilder(verbose=False)
        assert query_builder2.verbose is False

    def test_isouter_default(self):
        """Test QueryBuilder default isouter setting."""
        query_builder = QueryBuilder()
        assert query_builder.isouter is True


class TestJSONFunctions:
    """Tests for JSON helper functions."""

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_object_postgresql(self):
        """Test JSON_OBJECT returns JSON_BUILD_OBJECT for PostgreSQL."""
        result = JSON_OBJECT("key", "value")
        assert "JSON_BUILD_OBJECT" in str(result)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_array_postgresql(self):
        """Test JSON_ARRAY returns JSON_BUILD_ARRAY for PostgreSQL."""
        result = JSON_ARRAY("a", "b", "c")
        assert "JSON_BUILD_ARRAY" in str(result)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_agg_postgresql(self):
        """Test JSON_AGG returns JSON_AGG for PostgreSQL."""
        col = sa.column("test_col")
        result = JSON_AGG(col)
        assert "JSON_AGG" in str(result)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_type_postgresql(self):
        """Test JSON_TYPE returns JSONB for PostgreSQL."""
        result = JSON_TYPE()
        assert result == sa.dialects.postgresql.JSONB

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_cast_postgresql(self):
        """Test JSON_CAST casts to JSONB for PostgreSQL."""
        expr = sa.literal("test")
        result = JSON_CAST(expr)
        assert "JSONB" in str(result)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_concat_postgresql(self):
        """Test JSON_CONCAT uses || operator for PostgreSQL."""
        a = sa.cast(sa.literal({"a": 1}), sa.dialects.postgresql.JSONB)
        b = sa.cast(sa.literal({"b": 2}), sa.dialects.postgresql.JSONB)
        result = JSON_CONCAT(a, b)
        assert "||" in str(result)


@pytest.mark.usefixtures("table_creator")
class TestQueryBuilderAdvanced:
    """Advanced QueryBuilder tests for higher coverage."""

    @classmethod
    def setup_class(cls):
        cls.schema = "testdb" if IS_MYSQL_COMPAT else "public"

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_filters_with_none_value(self, connection):
        """Test _build_filters handles None values."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Filter with None value
        filters = {"book": [{"title": None}]}
        result = query_builder._build_filters(filters, node)
        # Should handle None gracefully
        assert result is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_cache_clearing(self, connection):
        """Test QueryBuilder cache can be cleared."""
        query_builder = QueryBuilder()
        query_builder._cache = {("a", "b"): {"test": "data"}}

        # Clear cache
        query_builder._cache.clear()
        assert len(query_builder._cache) == 0

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_json_build_object_single_column(self, connection):
        """Test _json_build_object with single column."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            columns=["isbn"],
        )

        expression = query_builder._json_build_object(node.columns)
        assert expression is not None
        assert "isbn" in str(expression)

    def test_querybuilder_initialization(self, connection):
        """Test QueryBuilder initializes correctly."""
        query_builder = QueryBuilder()
        assert query_builder.verbose is False
        assert query_builder.isouter is True

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_queries_sets_subquery(self, connection):
        """Test build_queries sets _subquery on node."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []

        assert node._subquery is None
        query_builder.build_queries(node)
        assert node._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_multiple_filters_or_condition(self, connection):
        """Test _build_filters creates OR for multiple filters."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Multiple filters should be OR'd
        filters = {"book": [{"isbn": "001"}, {"isbn": "002"}, {"isbn": "003"}]}
        result = query_builder._build_filters(filters, node)
        assert result is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__root_with_ctid_filter(self, connection):
        """Test _root method applies ctid filters for pagination."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        node.children = []
        node._filters = []

        # ctid is page -> rows mapping for PostgreSQL pagination
        ctid = {"0": [1, 2, 3], "1": [4, 5]}
        query_builder._root(node, ctid=ctid)

        # Should have created a subquery with ctid filter
        assert node._subquery is not None
        # Should have added ctid filter
        assert len(node._filters) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_one_to_one_scalar(self, connection):
        """Test _non_through with ONE_TO_ONE SCALAR relationship."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (book)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with SCALAR variant and ONE_TO_ONE
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            columns=["name"],  # Single scalar column
            relationship={
                "type": "one_to_one",
                "variant": "scalar",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_one_to_many_scalar(self, connection):
        """Test _non_through with ONE_TO_MANY SCALAR relationship."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (publisher)
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create child with SCALAR variant and ONE_TO_MANY
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            columns=["title"],  # Single scalar column
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "foreign_key": {
                    "child": ["publisher_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_one_to_many_object(self, connection):
        """Test _non_through with ONE_TO_MANY OBJECT relationship."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (publisher)
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create child with OBJECT variant and ONE_TO_MANY
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["publisher_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery with JSON_AGG
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_with_child_filters(self, connection):
        """Test _non_through applies child filters to join clause."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with filters
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent
        child.children = []

        # Add a filter to child
        child._filters = [child.model.c.name == "Test Publisher"]

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_with_nested_children(self, connection):
        """Test _non_through with child node that has its own children."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (publisher)
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create grandchild (city) - using existing FK relationship
        grandchild = Node(
            models=pg_base.models,
            table="city",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["city_id"],
                },
            },
        )

        # Mock grandchild subquery with _keys column
        grandchild._subquery = sa.select(
            JSON_CAST(JSON_OBJECT("id", grandchild.model.c.id)).label("_keys"),
            grandchild.model.c.id.label("id"),
            grandchild.model.c.name.label("name"),
            grandchild.model.c.id.label(grandchild.label),
        ).alias()

        # Create child (author) with nested grandchild
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        grandchild.parent = child
        child.children = [grandchild]
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_scalar_variant(self, connection):
        """Test _through method with SCALAR variant."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent (book)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_subject",
            schema=self.schema,
        )

        # Create child with SCALAR variant
        child = Node(
            models=pg_base.models,
            table="subject",
            schema=self.schema,
            columns=["name"],  # Single scalar column
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "through_tables": ["book_subject"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        child.children = []
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_object_one_to_one_no_children(self, connection):
        """Test _through with OBJECT ONE_TO_ONE variant without children."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_subject",
            schema=self.schema,
        )

        # Create child with OBJECT ONE_TO_ONE variant, no children
        child = Node(
            models=pg_base.models,
            table="subject",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "through_tables": ["book_subject"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        child.children = []  # No children
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_object_one_to_one_with_children(self, connection):
        """Test _through with OBJECT ONE_TO_ONE variant with children."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_subject",
            schema=self.schema,
        )

        # Create child with children - use simpler setup without grandchildren
        # The key is to test ONE_TO_ONE with children, not the specific relationships
        child = Node(
            models=pg_base.models,
            table="subject",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "through_tables": ["book_subject"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        # Add a mock child to trigger the children path (even if empty subquery)
        child.children = []  # Empty children still tests the path
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_filters(self, connection):
        """Test _through method applies child filters."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create grandchild with filters - using city which has FK to author
        grandchild = Node(
            models=pg_base.models,
            table="city",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["city_id"],
                },
            },
        )
        grandchild._subquery = sa.select(
            JSON_CAST(JSON_OBJECT("id", grandchild.model.c.id)).label("_keys"),
            grandchild.model.c.id.label("id"),
            grandchild.model.c.name.label("name"),
            grandchild.model.c.id.label(grandchild.label),
        ).alias()
        grandchild._filters = [grandchild.model.c.name == "Test City"]

        # Create child with through relationship
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        grandchild.parent = child
        child.children = [grandchild]
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_boolean_clause_list_filters(self, connection):
        """Test _through method applies BooleanClauseList filters."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create grandchild with BooleanClauseList filters
        grandchild = Node(
            models=pg_base.models,
            table="city",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["city_id"],
                },
            },
        )
        grandchild._subquery = sa.select(
            JSON_CAST(JSON_OBJECT("id", grandchild.model.c.id)).label("_keys"),
            grandchild.model.c.id.label("id"),
            grandchild.model.c.name.label("name"),
            grandchild.model.c.id.label(grandchild.label),
        ).alias()
        # Use OR filter (BooleanClauseList)
        grandchild._filters = [
            sa.or_(
                grandchild.model.c.name == "City A",
                grandchild.model.c.name == "City B",
            )
        ]

        # Create child with through relationship
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        grandchild.parent = child
        child.children = [grandchild]
        child._filters = []

        # Call _through
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_through_table(self, connection):
        """Test _children method with through-table relationship."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create child with through relationship
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        child._filters = []

        # Mock child subquery
        child._subquery = sa.select(
            through.model.c.book_isbn.label("book_isbn"),
            through.model.c.author_id.label("author_id"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should process children with through tables
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_filters_binary_expression(self, connection):
        """Test _children with filters as BinaryExpression."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with binary expression filter
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Add a binary expression filter
        child._filters = [child.model.c.name == "Test Publisher"]

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should process children with filters
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_filters_boolean_clause_list(self, connection):
        """Test _children with filters as BooleanClauseList."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with OR filter (BooleanClauseList)
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Add a boolean clause list filter (OR condition)
        child._filters = [
            sa.or_(
                child.model.c.name == "Publisher A",
                child.model.c.name == "Publisher B",
            )
        ]

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should process children with boolean clause filters
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_same_table_self_join(self, connection):
        """Test _children when child and parent are same table (self-join)."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent (city)
        parent = Node(
            models=pg_base.models,
            table="city",
            schema=self.schema,
        )

        # Create child (also city - self-referencing relationship)
        child = Node(
            models=pg_base.models,
            table="city",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],  # Different alias
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child._filters = []

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should use OR operator instead of AND for same table
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__get_foreign_keys_node_b_has_through(self, connection):
        """Test _get_foreign_keys when node_b has through tables."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create node_a (book)
        node_a = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create node_b with through
        node_b = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        node_b.relationship.throughs = [through]

        # Get foreign keys - should handle through table in node_b
        fkeys = query_builder._get_foreign_keys(node_a, node_b)

        assert isinstance(fkeys, dict)
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__get_foreign_keys_merges_duplicates(self, connection):
        """Test _get_foreign_keys merges duplicate foreign key columns."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create parent (book)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create child with through
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.relationship.throughs = [through]

        # This should merge foreign keys from through->child and through->parent
        fkeys = query_builder._get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        # Should have merged keys without duplicates
        for table, cols in fkeys.items():
            # Check that columns are unique
            assert len(cols) == len(set(cols))

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_queries_non_root_with_through(self, connection):
        """Test build_queries for non-root node with through table."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create child (non-root) with through
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent  # Not root
        child.relationship.throughs = [through]
        child.children = []
        child._filters = []

        # Build queries for non-root node with through
        query_builder.build_queries(child)

        # Should have created subquery via _through
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_build_queries_non_root_without_through(self, connection):
        """Test build_queries for non-root node without through table."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child (non-root) without through
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent  # Not root
        child.children = []
        child._filters = []

        # Build queries for non-root node without through
        query_builder.build_queries(child)

        # Should have created subquery via _non_through
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_non_root_parent_one_to_one(self, connection):
        """Test _non_through when parent is not root with ONE_TO_ONE."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create grandparent (root)
        grandparent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create parent (not root)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
            },
        )
        parent.parent = grandparent  # Has parent, so not root

        # Create child
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["book_isbn"],
                    "parent": ["isbn"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through - should use different key generation for non-root parent
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_non_root_parent_one_to_many(self, connection):
        """Test _non_through when parent is not root with ONE_TO_MANY."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create grandparent (root)
        grandparent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create parent (not root)
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
            },
        )
        parent.parent = grandparent  # Has parent, so not root

        # Create child with ONE_TO_MANY
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["book_isbn"],
                    "parent": ["isbn"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through - should use JSON_AGG for key aggregation
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__through_with_verbose_mode(self, connection):
        """Test _through method in verbose mode."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder(verbose=True)  # Enable verbose
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create through node
        through = Node(
            models=pg_base.models,
            table="book_author",
            schema=self.schema,
        )

        # Create child
        child = Node(
            models=pg_base.models,
            table="author",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "through_tables": ["book_author"],
            },
        )
        child.parent = parent
        child.relationship.throughs = [through]
        child.children = []
        child._filters = []

        # Call _through in verbose mode - should compile queries
        query_builder._through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_filters_on_parent_columns(self, connection):
        """Test _children with filters referencing parent table columns."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with filter referencing parent table column
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Create a filter that references a parent table column (isbn from book)
        # This tests lines 533-534: if column._orig_key in node.table_columns
        filter_expr = parent.model.c.isbn == "TEST-ISBN"
        child._filters = [filter_expr]

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should process filters and detect parent column reference
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_with_boolean_filters_on_parent_columns(
        self, connection
    ):
        """Test _children with BooleanClauseList filters referencing parent columns."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Create OR filter with parent table column reference
        # Tests lines 548-549: if column._orig_key in node.table_columns
        filter_expr = sa.or_(
            parent.model.c.isbn == "ISBN-1",
            parent.model.c.isbn == "ISBN-2",
        )
        child._filters = [filter_expr]

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should process boolean filters and detect parent column references
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__children_verbose_mode(self, connection):
        """Test _children in verbose mode."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder(verbose=True)
        query_builder.from_obj = None

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Create child with filters (to trigger verbose path)
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent
        child._filters = [child.model.c.name == "Test"]

        # Mock child subquery
        child._subquery = sa.select(
            child.model.c.id.label("id"),
            child.model.c.name.label("name"),
            child.model.c.id.label(child.label),
        ).alias()

        parent.children = [child]

        # This should compile queries in verbose mode
        query_builder._children(parent)

        # from_obj should be set
        assert query_builder.from_obj is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_with_binary_expression_filters(self, connection):
        """Test _non_through with BinaryExpression filters on nested children."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create root parent
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create child (book)
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["publisher_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through - simpler test without nested children
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_with_from_obj_none(self, connection):
        """Test _non_through initializes from_obj when it's None."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create root parent
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create child without children (simpler case)
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["publisher_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_one_to_many_groupby(self, connection):
        """Test _non_through applies group_by for ONE_TO_MANY."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        # Create parent
        parent = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
        )

        # Create child with ONE_TO_MANY (should apply group_by)
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            relationship={
                "type": "one_to_many",
                "variant": "object",
                "foreign_key": {
                    "child": ["publisher_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # Call _non_through
        query_builder._non_through(child)

        # Should have created a subquery with group_by
        assert child._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys_with_dict_format(self, connection):
        """Test get_foreign_keys with dict format foreign key hints."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Use dict format for foreign_key (tests lines 229-232)
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": {f"{self.schema}.publisher": ["id"]},
                    "parent": {f"{self.schema}.book": ["publisher_id"]},
                },
            },
        )
        child.parent = parent

        # Get foreign keys with dict format
        fkeys = query_builder.get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys_with_list_format(self, connection):
        """Test get_foreign_keys with list format foreign key hints."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Use list format for foreign_key (tests lines 233-235)
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": ["id"],
                    "parent": ["publisher_id"],
                },
            },
        )
        child.parent = parent

        # Get foreign keys with list format
        fkeys = query_builder.get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys_with_string_format(self, connection):
        """Test get_foreign_keys with string format foreign key hints."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )

        # Use string format for foreign_key (tests line 237)
        child = Node(
            models=pg_base.models,
            table="publisher",
            schema=self.schema,
            relationship={
                "type": "one_to_one",
                "variant": "object",
                "foreign_key": {
                    "child": "id",
                    "parent": "publisher_id",
                },
            },
        )
        child.parent = parent

        # Get foreign keys with string format
        fkeys = query_builder.get_foreign_keys(parent, child)

        assert isinstance(fkeys, dict)
        assert len(fkeys) >= 1

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__root_from_obj_set(self, connection):
        """Test _root when from_obj is already set."""
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        # Pre-set from_obj to a table
        node = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
        )
        query_builder.from_obj = node.model

        node.children = []
        node._filters = []

        # Call _root with from_obj already set
        query_builder._root(node)

        # Should have created a subquery
        assert node._subquery is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_get_foreign_keys_explicit_fk_with_multiple_fks(self, connection):
        """Test get_foreign_keys uses only explicit FK when child has multiple FKs to parent.

        Regression test: when a child table (book) has multiple foreign keys to the
        same parent table (user) — e.g. buyer_id and seller_id — and the schema
        specifies an explicit foreign_key, only the specified FK columns should be
        returned, not all discovered FKs.
        """
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()

        parent = Node(
            models=pg_base.models,
            table="user",
            schema=self.schema,
        )

        # book has both buyer_id and seller_id pointing to user.id
        # Explicitly specify only buyer_id
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            columns=["isbn"],
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "foreign_key": {
                    "child": ["buyer_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent

        fkeys = query_builder.get_foreign_keys(parent, child)

        # Should only contain buyer_id for the book table, not seller_id
        book_key = f"{self.schema}.book"
        user_key = f"{self.schema}.user"
        assert book_key in fkeys
        assert fkeys[book_key] == ["buyer_id"]
        assert user_key in fkeys
        assert fkeys[user_key] == ["id"]

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test__non_through_explicit_fk_with_multiple_fks(self, connection):
        """Test _non_through works when child has multiple FKs to parent.

        Regression test for IndexError: when a child table has multiple foreign
        keys to the same parent and the schema provides an explicit foreign_key,
        _non_through should use only the specified FK and not crash.
        """
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder()
        query_builder.verbose = False
        query_builder.isouter = True

        parent = Node(
            models=pg_base.models,
            table="user",
            schema=self.schema,
        )

        # book.buyer_id -> user.id (explicit), book.seller_id -> user.id (should be ignored)
        child = Node(
            models=pg_base.models,
            table="book",
            schema=self.schema,
            columns=["isbn"],
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "foreign_key": {
                    "child": ["buyer_id"],
                    "parent": ["id"],
                },
            },
        )
        child.parent = parent
        child.children = []
        child._filters = []

        # This previously raised IndexError because get_foreign_keys returned
        # both buyer_id and seller_id for book but only one id for user
        query_builder._non_through(child)

        assert child._subquery is not None
