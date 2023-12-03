"""View tests."""

import pytest
import sqlalchemy as sa
from mock import call, patch

from pgsync.base import Base, create_schema, subtransactions
from pgsync.constants import DEFAULT_SCHEMA
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

        session.connection().engine.connect().close()
        session.connection().engine.dispose()
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
            )
            assert mock_logger.debug.call_count == 2
            assert mock_logger.debug.call_args_list == [
                call("Creating view: myschema._view"),
                call("Created view: myschema._view"),
            ]
