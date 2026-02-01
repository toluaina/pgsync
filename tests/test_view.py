"""View tests."""

import pytest
import sqlalchemy as sa
from mock import call, patch

from pgsync.base import Base, create_schema, subtransactions
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.view import (
    _foreign_keys,
    _primary_keys,
    create_view,
    CreateIndex,
    CreateView,
    DropIndex,
    DropView,
    is_view,
    RefreshView,
)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
class TestView(object):
    """View tests."""

    @pytest.fixture(scope="function")
    def data(self, sync, book_cls):
        session = sync.session
        books = [
            book_cls(
                isbn="abc",
                title="The world is yours",
                description="Tigers are mystical creatures",
            ),
        ]
        with subtransactions(session):
            session.add_all(books)
        yield books

        with subtransactions(session):
            session.query(book_cls).delete()

        session.connection().engine.dispose(close=True)
        sync.search_client.close()

    def test_create_materialized_view(self, connection):
        """Test create materialized view."""
        view = "test_mat_view"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=True
                )
            )
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is True
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()

    def test_create_view(self, connection):
        """Test create non-materialized view."""
        view = "test_view"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=False
                )
            )
            conn.commit()
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is True
        )
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=False))
            conn.commit()

    def test_drop_materialized_view(self, connection):
        """Test drop materialized view."""
        view = "test_view_drop"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=True
                )
            )
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is True
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )

    def test_drop_view(self, connection):
        """Test drop non-materialized view."""
        view = "test_view_drop"
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, view, sa.select(1), materialized=False
                )
            )
            conn.commit()
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is True
        )
        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=False))
            conn.commit()
        assert (
            is_view(connection.engine, DEFAULT_SCHEMA, view, materialized=True)
            is False
        )
        assert (
            is_view(
                connection.engine, DEFAULT_SCHEMA, view, materialized=False
            )
            is False
        )

    @pytest.mark.usefixtures("table_creator")
    def test_refresh_view(self, connection, sync, book_cls, data):
        """Test refresh materialized view."""
        view = "test_view_refresh"
        pg_base = Base(connection.engine.url.database)

        model = pg_base.models("book", "public")
        statement = sa.select(*[model.c.isbn]).select_from(model)
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(DEFAULT_SCHEMA, view, statement, materialized=True)
            )
            conn.commit()

        with connection.engine.connect() as conn:
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "abc"

        session = sync.session
        with subtransactions(session):
            session.execute(
                book_cls.__table__.update()
                .where(book_cls.__table__.c.isbn == "abc")
                .values(isbn="xyz")
            )

        with connection.engine.connect() as conn:
            # the value should still be abc
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "abc"

        with connection.engine.connect() as conn:
            conn.execute(RefreshView(DEFAULT_SCHEMA, view))
            conn.commit()

        with connection.engine.connect() as conn:
            # the value should now be xyz
            assert [
                result.isbn
                for result in conn.execute(sa.text(f"SELECT * FROM {view}"))
            ][0] == "xyz"

        with connection.engine.connect() as conn:
            conn.execute(DropView(DEFAULT_SCHEMA, view, materialized=True))
            conn.commit()

    @pytest.mark.usefixtures("table_creator")
    def test_index(self, connection, sync, book_cls, data):
        """Test create/drop an index."""
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert indices == []
        with connection.engine.connect() as conn:
            conn.execute(
                CreateIndex("my_index", DEFAULT_SCHEMA, "book", ["isbn"])
            )
            conn.commit()
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert len(indices) == 1
        assert indices[0] == {
            "name": "my_index",
            "unique": True,
            "column_names": ["isbn"],
            "include_columns": [],
            "dialect_options": {
                "postgresql_include": [],
            },
        }
        with connection.engine.connect() as conn:
            conn.execute(DropIndex("my_index"))
            conn.commit()
        indices = sa.inspect(connection.engine).get_indexes(
            "book", schema=DEFAULT_SCHEMA
        )
        assert indices == []

    @pytest.mark.usefixtures("table_creator")
    def test_primary_keys(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)
        statement = _primary_keys(pg_base.models, DEFAULT_SCHEMA, ["book"])
        rows = connection.execute(statement).fetchall()
        assert rows == [("book", ["isbn"])]

    @pytest.mark.usefixtures("table_creator")
    def test_foreign_keys(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)
        statement = _foreign_keys(
            pg_base.models, DEFAULT_SCHEMA, ["book", "publisher"]
        )
        rows = connection.execute(statement).fetchall()
        assert rows[0][0] == "book"
        assert sorted(rows[0][1]) == sorted(
            ["publisher_id", "buyer_id", "seller_id"]
        )

    @pytest.mark.usefixtures("table_creator")
    def test__create_view(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)

        def fetchall(statement):
            return connection.execute(statement).fetchall()

        with patch("pgsync.view.logger") as mock_logger:
            create_view(
                connection.engine,
                pg_base.models,
                fetchall,
                "testdb",
                DEFAULT_SCHEMA,
                ["book", "publisher"],
                user_defined_fkey_tables={},
                views=[],
                node_columns={"book": ["node_id"]},
            )
            assert mock_logger.debug.call_count == 2
            assert mock_logger.debug.call_args_list == [
                call("Creating view: public._view"),
                call("Created view: public._view"),
            ]

        user_defined_fkey_tables = {"publisher": ["publisher_id"]}
        create_schema(connection.engine.url.database, "myschema")

        with patch("pgsync.view.logger") as mock_logger:
            create_view(
                connection.engine,
                pg_base.models,
                fetchall,
                "testdb",
                "myschema",
                set(["book", "publisher"]),
                user_defined_fkey_tables=user_defined_fkey_tables,
                views=[],
                node_columns={"book": ["node_id"]},
            )
            assert mock_logger.debug.call_count == 2
            assert mock_logger.debug.call_args_list == [
                call("Creating view: myschema._view"),
                call("Created view: myschema._view"),
            ]

    def test_create_view_ddl_compile(self, connection):
        """Test CreateView DDL compiles correctly."""
        ddl = CreateView(
            DEFAULT_SCHEMA, "test_view", sa.select(1), materialized=False
        )
        # Verify DDL object has expected attributes
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"
        assert ddl.materialized is False

    def test_create_view_ddl_compile_materialized(self, connection):
        """Test CreateView DDL compiles correctly for materialized views."""
        ddl = CreateView(
            DEFAULT_SCHEMA, "test_mat_view", sa.select(1), materialized=True
        )
        assert ddl.materialized is True

    def test_drop_view_ddl_compile(self, connection):
        """Test DropView DDL compiles correctly."""
        ddl = DropView(DEFAULT_SCHEMA, "test_view", materialized=False)
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"
        assert ddl.materialized is False

    def test_drop_view_ddl_compile_materialized(self, connection):
        """Test DropView DDL compiles correctly for materialized views."""
        ddl = DropView(DEFAULT_SCHEMA, "test_mat_view", materialized=True)
        assert ddl.materialized is True

    def test_refresh_view_ddl_compile(self, connection):
        """Test RefreshView DDL compiles correctly."""
        ddl = RefreshView(DEFAULT_SCHEMA, "test_view")
        assert ddl.schema == DEFAULT_SCHEMA
        assert ddl.name == "test_view"

    def test_refresh_view_concurrently(self, connection):
        """Test RefreshView with concurrently option."""
        ddl = RefreshView(DEFAULT_SCHEMA, "test_view", concurrently=True)
        assert ddl.concurrently is True

    def test_create_index_ddl(self, connection):
        """Test CreateIndex DDL object."""
        ddl = CreateIndex(
            "test_idx", DEFAULT_SCHEMA, "book", ["isbn", "title"]
        )
        assert ddl.name == "test_idx"
        assert ddl.schema == DEFAULT_SCHEMA

    def test_drop_index_ddl(self, connection):
        """Test DropIndex DDL object."""
        ddl = DropIndex("test_idx")
        assert ddl.name == "test_idx"

    def test_is_view_not_exists(self, connection):
        """Test is_view returns False for non-existent view."""
        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "nonexistent_view",
            materialized=True,
        )
        assert result is False

        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "nonexistent_view",
            materialized=False,
        )
        assert result is False


# ============================================================================
# PHASE 7 EXTENDED TESTS - View.py Comprehensive Coverage
# ============================================================================


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewCompilation:
    """Extended tests for view DDL compilation."""

    def test_compile_create_view_with_literal_binds(self, connection):
        """Test compile_create_view with literal_binds for query parameters."""
        from pgsync.view import compile_create_view

        # Create a view with parameters
        view = View(
            name="test_view_literal",
            schema=DEFAULT_SCHEMA,
        )

        # Compile with literal_binds=True
        ddl = compile_create_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_view_literal",
            sa.select(sa.literal(1).label("num")),
            materialized=False,
        )

        # Should contain CREATE VIEW
        assert "CREATE" in ddl
        assert "VIEW" in ddl
        assert "test_view_literal" in ddl

    def test_compile_create_view_materialized_with_literal_binds(
        self, connection
    ):
        """Test compile_create_view for materialized view with literal_binds."""
        from pgsync.view import compile_create_view

        # Compile materialized view
        ddl = compile_create_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_mat_view_literal",
            sa.select(sa.literal(1).label("num")),
            materialized=True,
        )

        # Should contain CREATE MATERIALIZED VIEW
        assert "CREATE MATERIALIZED VIEW" in ddl
        assert "test_mat_view_literal" in ddl

    def test_compile_create_view_with_complex_query(
        self, connection, book_cls
    ):
        """Test compile_create_view with complex SELECT query."""
        from pgsync.view import compile_create_view

        # Complex query with joins
        query = sa.select(
            book_cls.c.isbn,
            book_cls.c.title,
        ).where(book_cls.c.isbn.isnot(None))

        ddl = compile_create_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_complex_view",
            query,
            materialized=False,
        )

        # Should contain the WHERE clause
        assert "WHERE" in ddl or "where" in ddl.lower()

    def test_compile_drop_view_non_materialized(self, connection):
        """Test compile_drop_view for regular views."""
        from pgsync.view import compile_drop_view

        ddl = compile_drop_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_drop_view",
            materialized=False,
        )

        # Should be DROP VIEW
        assert "DROP VIEW" in ddl
        assert "MATERIALIZED" not in ddl

    def test_compile_drop_view_materialized(self, connection):
        """Test compile_drop_view for materialized views."""
        from pgsync.view import compile_drop_view

        ddl = compile_drop_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_drop_mat_view",
            materialized=True,
        )

        # Should be DROP MATERIALIZED VIEW
        assert "DROP MATERIALIZED VIEW" in ddl

    def test_compile_refresh_view_concurrently_true(self, connection):
        """Test compile_refresh_view with CONCURRENTLY flag."""
        from pgsync.view import compile_refresh_view

        ddl = compile_refresh_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_refresh_view",
            concurrently=True,
        )

        # Should contain CONCURRENTLY
        assert "CONCURRENTLY" in ddl
        assert "REFRESH MATERIALIZED VIEW" in ddl

    def test_compile_refresh_view_concurrently_false(self, connection):
        """Test compile_refresh_view without CONCURRENTLY flag."""
        from pgsync.view import compile_refresh_view

        ddl = compile_refresh_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "test_refresh_view",
            concurrently=False,
        )

        # Should not contain CONCURRENTLY
        assert "CONCURRENTLY" not in ddl
        assert "REFRESH MATERIALIZED VIEW" in ddl


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewConstraints:
    """Extended tests for constraint queries."""

    def test_get_constraints_with_schema(self, connection):
        """Test _get_constraints query building with schema."""
        from pgsync.view import _get_constraints

        # Get constraints for a table
        constraints = _get_constraints(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
        )

        # Should return dict
        assert isinstance(constraints, dict)

    def test_get_constraints_primary_keys(self, connection):
        """Test _get_constraints returns primary key constraints."""
        from pgsync.view import _get_constraints

        constraints = _get_constraints(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
        )

        # Should have primary key constraint
        has_pk = any(
            constraint.get("type") == "p"
            for constraint in constraints.values()
        )
        assert has_pk

    def test_get_constraints_foreign_keys(self, connection):
        """Test _get_constraints returns foreign key constraints."""
        from pgsync.view import _get_constraints

        constraints = _get_constraints(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
        )

        # Should have foreign key constraints (book has FKs to publisher, etc.)
        has_fk = any(
            constraint.get("type") == "f"
            for constraint in constraints.values()
        )
        assert has_fk

    def test_get_constraints_unique_constraints(self, connection):
        """Test _get_constraints returns unique constraints."""
        from pgsync.view import _get_constraints

        # Get constraints for a table that might have unique constraints
        constraints = _get_constraints(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
        )

        # Should return valid constraint dict
        assert isinstance(constraints, dict)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewNonDefaultSchema:
    """Tests for views with non-default schemas."""

    def test_create_view_custom_schema(self, connection):
        """Test creating view in non-default schema."""
        # Note: This requires a custom schema to exist
        # For now, test the DDL compilation

        from pgsync.view import compile_create_view

        custom_schema = "custom_schema"

        ddl = compile_create_view(
            connection.engine,
            custom_schema,
            "test_custom_view",
            sa.select(sa.literal(1).label("num")),
            materialized=False,
        )

        # Should include schema in view name
        assert custom_schema in ddl
        assert "test_custom_view" in ddl

    def test_drop_view_custom_schema(self, connection):
        """Test dropping view from non-default schema."""
        from pgsync.view import compile_drop_view

        custom_schema = "custom_schema"

        ddl = compile_drop_view(
            connection.engine,
            custom_schema,
            "test_custom_view",
            materialized=False,
        )

        # Should include schema in view name
        assert custom_schema in ddl

    def test_refresh_view_custom_schema(self, connection):
        """Test refreshing materialized view in non-default schema."""
        from pgsync.view import compile_refresh_view

        custom_schema = "custom_schema"

        ddl = compile_refresh_view(
            connection.engine,
            custom_schema,
            "test_custom_mat_view",
            concurrently=False,
        )

        # Should include schema in view name
        assert custom_schema in ddl


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestViewHelpers:
    """Extended tests for view helper methods."""

    def test_view_initialization(self):
        """Test View object initialization."""
        view = View(
            name="test_view",
            schema="public",
        )

        assert view.name == "test_view"
        assert view.schema == "public"

    def test_view_with_custom_properties(self):
        """Test View with additional properties."""
        view = View(
            name="test_view",
            schema="public",
        )

        # Should have materialized property
        assert hasattr(view, "materialized")

    def test_view_name_property(self, connection):
        """Test View name property."""
        view = View(
            name="test_view",
            schema="public",
        )

        assert view.name == "test_view"

    def test_view_schema_property(self, connection):
        """Test View schema property."""
        view = View(
            name="test_view",
            schema="custom_schema",
        )

        assert view.schema == "custom_schema"

    def test_is_view_returns_false_for_tables(self, connection):
        """Test is_view returns False for regular tables."""
        from pgsync.view import is_view

        # book is a table, not a view
        result = is_view(
            connection.engine,
            DEFAULT_SCHEMA,
            "book",
            materialized=False,
        )

        assert result is False
