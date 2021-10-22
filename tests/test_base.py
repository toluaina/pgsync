"""Base tests."""

import pytest
from mock import patch

from pgsync.base import (
    Base,
    create_database,
    create_extension,
    drop_database,
    drop_extension,
)
from pgsync.exc import (
    InvalidPermissionError,
    LogicalSlotParseError,
    TableNotFoundError,
)


@pytest.mark.usefixtures("table_creator")
class TestBase(object):
    """Base tests."""

    def test_pg_settings(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.pg_settings("max_replication_slots")
        assert int(value) > 0
        assert pg_base.pg_settings("xyz") is None

    def test_has_permissions(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.has_permissions(
            connection.engine.url.username,
            ["usesuper"],
        )
        assert (
            pg_base.has_permissions(
                connection.engine.url.username,
                ["usesuper"],
            )
            is True
        )

        assert (
            pg_base.has_permissions(
                "spiderman",
                ["usesuper"],
            )
            is False
        )

        with pytest.raises(InvalidPermissionError) as excinfo:
            pg_base.has_permissions(
                connection.engine.url.username,
                ["sudo"],
            )

    def test_model(self, connection):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.model("book", "public")
        assert str(model.original) == "public.book"
        assert pg_base.models["public.book"] == model
        with pytest.raises(TableNotFoundError) as excinfo:
            pg_base.model("book", "bar")
            assert 'Table "bar.book" not found in registry' in str(
                excinfo.value
            )

    def test_database(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.database == "testdb"

    def test_schemas(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.schemas == ["public"]

    def test_tables(self, connection):
        pg_base = Base(connection.engine.url.database)
        tables = [
            "public.continent",
            "public.country",
            "public.publisher",
            "public.book",
            "public.city",
            "public.book_subject",
            "public.subject",
            "public.book_language",
            "public.language",
            "public.book_shelf",
            "public.shelf",
            "public.author",
            "public.book_author",
            "public.rating",
        ]
        assert sorted(pg_base.tables("public")) == sorted(tables)

    def test_replication_slots(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.replication_slots("noob") == []
        replication_slots = pg_base.replication_slots(
            f"{connection.engine.url.database}_testdb"
        )
        assert "testdb_testdb" == replication_slots[0][0]

    def test_create_replication_slot(self, connection):
        pg_base = Base(connection.engine.url.database)
        row = pg_base.create_replication_slot("slot_name")
        assert row[0] == "slot_name"
        assert row[1] is not None
        pg_base.drop_replication_slot("slot_name")

    def test_drop_replication_slot(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot("slot_name")
        pg_base.drop_replication_slot("slot_name")

    def test_get_schema(self, connection):
        pg_base = Base(connection.engine.url.database)

        pair1, pair2 = pg_base._get_schema("public", "public1.my_table")
        assert pair1 == "public1"
        assert pair2 == "my_table"

        pair1, pair2 = pg_base._get_schema("public", "my_table")
        assert pair1 == "public"
        assert pair2 == "my_table"

        with pytest.raises(ValueError) as excinfo:
            pg_base._get_schema("public", "public1.my_table.foo")
            assert "Invalid definition public1.my_table for public" == str(
                excinfo.value
            )

    @patch("pgsync.base.pg_execute")
    @patch("pgsync.base.pg_engine")
    @patch("pgsync.base.logger")
    def test_create_database(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        create_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f"Creating database: {database}")
        mock_logger.debug.assert_any_call(f"Created database: {database}")
        mock_pg_engine.assert_any_call(database="postgres", echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            f'CREATE DATABASE "{database}"',
        )

    @patch("pgsync.base.pg_execute")
    @patch("pgsync.base.pg_engine")
    @patch("pgsync.base.logger")
    def test_drop_database(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        drop_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f"Dropping database: {database}")
        mock_logger.debug.assert_any_call(f"Dropped database: {database}")
        mock_pg_engine.assert_any_call(database="postgres", echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            f'DROP DATABASE IF EXISTS "{database}"',
        )

    @patch("pgsync.base.pg_execute")
    @patch("pgsync.base.pg_engine")
    @patch("pgsync.base.logger")
    def test_create_extension(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        create_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Creating extension: my_ext")
        mock_logger.debug.assert_any_call("Created extension: my_ext")
        mock_pg_engine.assert_any_call(database=database, echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'CREATE EXTENSION IF NOT EXISTS "my_ext"',
        )

    @patch("pgsync.base.pg_execute")
    @patch("pgsync.base.pg_engine")
    @patch("pgsync.base.logger")
    def test_drop_extension(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        drop_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Dropping extension: my_ext")
        mock_logger.debug.assert_any_call("Dropped extension: my_ext")
        mock_pg_engine.assert_any_call(database=database, echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'DROP EXTENSION IF EXISTS "my_ext"',
        )

    def test_parse_logical_slot(
        self,
        connection,
    ):
        pg_base = Base(connection.engine.url.database)
        with pytest.raises(LogicalSlotParseError) as excinfo:
            pg_base.parse_logical_slot("")
            assert "No match for row:" in str(excinfo.value)

        row = """
        table public."B1_XYZ": INSERT: "ID"[integer]:5 "CREATED_TIMESTAMP"[bigint]:222 "ADDRESS"[character varying]:'from3' "SOME_FIELD_KEY"[character varying]:'key3' "SOME_OTHER_FIELD_KEY"[character varying]:'issue3' "CHANNEL_ID"[integer]:3 "CHANNEL_NAME"[character varying]:'channel3' "ITEM_ID"[integer]:3 "MESSAGE"[character varying]:'message3' "RETRY"[integer]:4 "STATUS"[character varying]:'status' "SUBJECT"[character varying]:'sub3' "TIMESTAMP"[bigint]:33
        """
        values = pg_base.parse_logical_slot(row)
        assert values == {
            "new": {
                "CHANNEL_ID": 3,
                "CHANNEL_NAME": "channel3",
                "CREATED_TIMESTAMP": 222,
                "ADDRESS": "from3",
                "ID": 5,
                "ITEM_ID": 3,
                "MESSAGE": "message3",
                "RETRY": 4,
                "SOME_FIELD_KEY": "key3",
                "SOME_OTHER_FIELD_KEY": "issue3",
                "STATUS": "status",
                "SUBJECT": "sub3",
                "TIMESTAMP": 33,
            },
            "old": {},
            "schema": "public",
            "table": "B1_XYZ",
            "tg_op": "INSERT",
        }
