"""QueryBuilder tests."""
import pytest

from pgsync.base import Base
from pgsync.exc import ForeignKeyError
from pgsync.node import Node
from pgsync.querybuilder import QueryBuilder


@pytest.mark.usefixtures("table_creator")
class TestQueryBuilder(object):
    """QueryBuilder tests."""

    def test__json_build_object(self, connection):
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder(pg_base)

        with pytest.raises(RuntimeError) as excinfo:
            query_builder._json_build_object([])
        assert "invalid expression" == str(excinfo.value)
        node = Node(
            base=pg_base,
            table="book",
            schema="public",
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
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_1, book_1.isbn) AS "
            "JSONB) || CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_2, "
            "book_1.title) AS JSONB) || CAST(JSON_BUILD_OBJECT(:"
            "JSON_BUILD_OBJECT_3, book_1.description) AS JSONB) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_4, book_1.copyright) "
            "AS JSONB) || CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_5, "
            "book_1.publisher_id) AS JSONB) || CAST(JSON_BUILD_OBJECT("
            ":JSON_BUILD_OBJECT_6, book_1.buyer_id) AS JSONB) || "
            "CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_7, book_1.seller_id) "
            "AS JSONB) || CAST(JSON_BUILD_OBJECT(:JSON_BUILD_OBJECT_8, "
            "book_1.tags) AS JSONB)"
        )
        assert str(expression) == expected

    def test__get_foreign_keys(self, connection):
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder(pg_base)
        book = Node(
            base=pg_base,
            table="book",
            schema="public",
        )
        with pytest.raises(ForeignKeyError) as excinfo:
            query_builder._get_foreign_keys(book, book)

        expected = (
            'No foreign key relationship between "public.book" '
            'and "public.book"'
        )
        assert expected in str(excinfo.value)
        publisher = Node(
            base=pg_base,
            table="publisher",
            schema="public",
        )
        foreign_keys = query_builder._get_foreign_keys(book, publisher)
        assert foreign_keys == {
            "public.book": ["publisher_id"],
            "public.publisher": ["id"],
        }

        subject = Node(
            base=pg_base,
            table="subject",
            schema="public",
            relationship={
                "type": "one_to_many",
                "variant": "scalar",
                "through_tables": ["book_subject"],
            },
        )
        foreign_keys = query_builder._get_foreign_keys(book, subject)
        assert foreign_keys == {
            "public.book_subject": ["subject_id", "book_isbn"],
            "public.subject": ["id"],
            "public.book": ["isbn"],
        }

    def test__get_column_foreign_keys(self, connection):
        pg_base = Base(connection.engine.url.database)
        query_builder = QueryBuilder(pg_base)

        foreign_keys = {
            "public.subject": ["column_a", "column_b", "column_X"],
            "schema.table_b": ["column_x"],
        }

        subject = Node(
            base=pg_base,
            table="subject",
            schema="public",
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

    def test__get_child_keys(self):
        pass

    def test__root(self):
        pass

    def test__children(self):
        pass

    def test__through(self):
        pass

    def test__non_through(self):
        pass
