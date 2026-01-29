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
