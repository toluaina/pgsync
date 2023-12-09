"""Base tests."""

import pytest
import sqlalchemy as sa
from mock import call, patch

from pgsync.base import (
    _pg_engine,
    Base,
    create_database,
    create_extension,
    create_schema,
    drop_database,
    drop_extension,
    pg_execute,
)
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.exc import (
    LogicalSlotParseError,
    ReplicationSlotError,
    TableNotFoundError,
)
from pgsync.view import CreateView, DropView


@pytest.mark.usefixtures("table_creator")
class TestBase(object):
    """Base tests."""

    def test_pg_settings(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.pg_settings("max_replication_slots")
        assert int(value) > 0
        assert pg_base.pg_settings("xyz") is None

    @patch("pgsync.base.logger")
    def test__can_create_replication_slot(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)

        pg_base.create_replication_slot("foo")
        with patch("pgsync.base.Base.drop_replication_slot") as mock_slot:
            with patch("pgsync.base.Base.create_replication_slot"):
                pg_base._can_create_replication_slot("foo")
                mock_logger.exception.assert_called_once_with(
                    "Replication slot foo already exists"
                )
        assert mock_slot.call_args_list == [
            call("foo"),
            call("foo"),
        ]

        with patch("pgsync.base.Base.drop_replication_slot") as mock_slot:
            pg_base._can_create_replication_slot("bar")
            mock_slot.assert_called_once_with("bar")

        with patch(
            "pgsync.base.Base.create_replication_slot", side_effect=Exception
        ) as mock_slot:
            with pytest.raises(ReplicationSlotError) as excinfo:
                pg_base._can_create_replication_slot("barx")
            mock_slot.assert_called_once_with("barx")
            msg = (
                f'PG_USER "{pg_base.engine.url.username}" needs to be '
                f"superuser or have permission to read, create and destroy "
                f"replication slots to perform this action."
            )
            assert msg in str(excinfo.value)

    def test_model(self, connection):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.models("book", "public")
        assert str(model.original) == "public.book"
        with pytest.raises(TableNotFoundError) as excinfo:
            pg_base.models("book", "bar")
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
            "continent",
            "country",
            "group",
            "publisher",
            "book",
            "city",
            "book_subject",
            "subject",
            "book_language",
            "language",
            "book_shelf",
            "shelf",
            "author",
            "book_author",
            "book_group",
            "rating",
            "contact",
            "contact_item",
            "user",
        ]
        assert sorted(pg_base.tables("public")) == sorted(tables)

    def test_indices(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.indices("contact_item", "public") == [
            {
                "name": "contact_item_contact_id_key",
                "unique": True,
                "column_names": ["contact_id"],
                "include_columns": [],
                "duplicates_constraint": "contact_item_contact_id_key",
                "dialect_options": {"postgresql_include": []},
            },
            {
                "name": "contact_item_name_key",
                "unique": True,
                "column_names": ["name"],
                "include_columns": [],
                "duplicates_constraint": "contact_item_name_key",
                "dialect_options": {"postgresql_include": []},
            },
        ]

    def test_columns(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.columns("public", "book") == [
            "buyer_id",
            "copyright",
            "description",
            "isbn",
            "publisher_id",
            "seller_id",
            "tags",
            "title",
        ]
        assert pg_base.columns("public", "shelf") == ["id", "shelf"]
        assert pg_base.columns("public", "book_author") == [
            "author_id",
            "book_isbn",
            "id",
        ]

    @patch("pgsync.base.logger")
    @patch("pgsync.sync.Base.execute")
    @patch("pgsync.base.sa.text")
    def test_truncate_table(
        self, mock_text, mock_execute, mock_logger, connection
    ):
        pg_base = Base(connection.engine.url.database)
        pg_base.truncate_table("book")
        calls = [
            call("Truncating table: public.book"),
            call("Truncated table: public.book"),
        ]
        assert mock_logger.debug.call_args_list == calls
        mock_execute.assert_called_once()
        mock_text.assert_called_once_with(
            'TRUNCATE TABLE "public"."book" CASCADE'
        )

    @patch("pgsync.base.logger")
    @patch("pgsync.sync.Base.truncate_table")
    def test_truncate_tables(
        self, mock_truncate_table, mock_logger, connection
    ):
        pg_base = Base(connection.engine.url.database)
        pg_base.truncate_tables(["book", "user"])
        calls = [
            call("Truncating tables: ['book', 'user']"),
            call("Truncated tables: ['book', 'user']"),
        ]
        assert mock_logger.debug.call_args_list == calls
        calls = [
            call("book", schema="public"),
            call("user", schema="public"),
        ]
        assert mock_truncate_table.call_args_list == calls

    @patch("pgsync.base.logger")
    @patch("pgsync.sync.Base.truncate_tables")
    def test_truncate_schema(
        self, mock_truncate_tables, mock_logger, connection
    ):
        pg_base = Base(connection.engine.url.database)
        pg_base.truncate_schema("public")
        calls = [
            call("Truncating schema: public"),
            call("Truncated schema: public"),
        ]
        assert mock_logger.debug.call_args_list == calls
        mock_truncate_tables.assert_called_once_with(
            [
                "author",
                "book",
                "book_author",
                "book_group",
                "book_language",
                "book_shelf",
                "book_subject",
                "city",
                "contact",
                "contact_item",
                "continent",
                "country",
                "group",
                "language",
                "publisher",
                "rating",
                "shelf",
                "subject",
                "user",
            ],
            schema="public",
        )

    @patch("pgsync.sync.Base.truncate_schema")
    def test_truncate_schemas(self, mock_truncate_schema, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.truncate_schemas()
        mock_truncate_schema.assert_called_once_with("public")

    def test_replication_slots(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.replication_slots("noob") == []
        replication_slots = pg_base.replication_slots(
            f"{connection.engine.url.database}_testdb"
        )
        assert "testdb_testdb" == replication_slots[0][0]

    @patch("pgsync.base.logger")
    def test_create_replication_slot(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot("slot_name")
        pg_base.drop_replication_slot("slot_name")
        calls = [
            call("Creating replication slot: slot_name"),
            call("Created replication slot: slot_name"),
            call("Dropping replication slot: slot_name"),
            call("Dropped replication slot: slot_name"),
        ]
        assert mock_logger.debug.call_args_list == calls

        with pytest.raises(sa.exc.ProgrammingError):
            pg_base.drop_replication_slot(1)

    @patch("pgsync.base.logger")
    def test_drop_replication_slot(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot("slot_name")
        pg_base.drop_replication_slot("slot_name")
        calls = [
            call("Creating replication slot: slot_name"),
            call("Created replication slot: slot_name"),
            call("Dropping replication slot: slot_name"),
            call("Dropped replication slot: slot_name"),
        ]
        assert mock_logger.debug.call_args_list == calls

    @patch("pgsync.base.logger")
    @patch("pgsync.base.Base.execute")
    def test_create_trigger(self, mock_execute, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_function(DEFAULT_SCHEMA)
        pg_base.create_triggers(DEFAULT_SCHEMA, "book", join_queries=True)
        calls = [
            call("Creating trigger on table: public.book"),
            call("Dropping trigger on table: public.book"),
            call("Dropping trigger on table: public.book"),
        ]
        assert mock_logger.debug.call_args_list == calls
        assert mock_execute.call_count == 6

        pg_base.drop_function(DEFAULT_SCHEMA)
        pg_base.drop_triggers(DEFAULT_SCHEMA, "book", join_queries=True)

    @patch("pgsync.base.pg_execute")
    @patch("pgsync.base.pg_engine")
    @patch("pgsync.base.logger")
    def test_create_schema(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        schema: str = "myschema"
        database = connection.engine.url.database
        create_schema(database, schema, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f"Creating schema: {schema}")
        mock_logger.debug.assert_any_call(f"Created schema: {schema}")
        mock_pg_engine.assert_any_call(database, echo=True)
        calls = [
            call(mock_pg_engine, f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        ]
        mock_pg_execute.call_args_list == calls

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
        create_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f"Creating database: {database}")
        mock_logger.debug.assert_any_call(f"Created database: {database}")
        mock_pg_engine.assert_any_call("postgres", echo=True)
        calls = [call(mock_pg_engine, f'CREATE DATABASE "{database}"')]
        mock_pg_execute.call_args_list == calls

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
        drop_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f"Dropping database: {database}")
        mock_logger.debug.assert_any_call(f"Dropped database: {database}")
        mock_pg_engine.assert_any_call("postgres", echo=True)
        calls = [call(mock_pg_engine, f'DROP DATABASE "{database}"')]
        mock_pg_execute.call_args_list == calls

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
        create_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Creating extension: my_ext")
        mock_logger.debug.assert_any_call("Created extension: my_ext")
        mock_pg_engine.assert_any_call(database, echo=True)
        calls = [
            call(mock_pg_engine, 'CREATE EXTENSION IF NOT EXISTS "my_ext"')
        ]
        mock_pg_execute.call_args_list == calls

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
        drop_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Dropping extension: my_ext")
        mock_logger.debug.assert_any_call("Dropped extension: my_ext")
        mock_pg_engine.assert_any_call(database, echo=True)
        calls = [call(mock_pg_engine, 'DROP EXTENSION IF NOT EXISTS "my_ext"')]
        mock_pg_execute.call_args_list == calls

    @patch("pgsync.base.logger")
    def test_drop_view(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        with patch("pgsync.sync.Base.engine"):
            pg_base.drop_view("public")
            calls = [
                call("Dropping view: public._view"),
                call("Dropped view: public._view"),
            ]
            assert mock_logger.debug.call_args_list == calls

    @patch("pgsync.base.logger")
    def test_refresh_view(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        with patch("pgsync.sync.Base.engine"):
            pg_base.refresh_view("foo", "public", concurrently=True)
            calls = [
                call("Refreshing view: public.foo"),
                call("Refreshed view: public.foo"),
            ]
            assert mock_logger.debug.call_args_list == calls

    def test_parse_value(self, connection):
        pg_base = Base(connection.engine.url.database)
        value = pg_base.parse_value("str", "foo")
        assert value == "foo"

        with pytest.raises(ValueError) as excinfo:
            pg_base.parse_value("int", "foo")
        assert "invalid literal for int() with base 10: 'foo'" in str(
            excinfo.value
        )

        value = pg_base.parse_value("boolean", "foo")
        assert value is True

        with pytest.raises(ValueError) as excinfo:
            pg_base.parse_value("float4", "foo")
        assert "could not convert string to float: 'foo'" in str(excinfo.value)

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
        """  # noqa E501
        payload = pg_base.parse_logical_slot(row)
        assert payload.data == {
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
        }
        assert payload.old == {}
        assert payload.schema == "public"
        assert payload.table == "B1_XYZ"
        assert payload.tg_op == "INSERT"
        row = """
        table public."B1_XYZ": UNKNOWN: "ID"[integer]:5 "CREATED_TIMESTAMP"[bigint]:222 "ADDRESS"[character varying]:'from3' "SOME_FIELD_KEY"[character varying]:'key3' "SOME_OTHER_FIELD_KEY"[character varying]:'issue3' "CHANNEL_ID"[integer]:3 "CHANNEL_NAME"[character varying]:'channel3' "ITEM_ID"[integer]:3 "MESSAGE"[character varying]:'message3' "RETRY"[integer]:4 "STATUS"[character varying]:'status' "SUBJECT"[character varying]:'sub3' "TIMESTAMP"[bigint]:33
        """  # noqa E501
        with pytest.raises(Exception) as excinfo:
            pg_base.parse_logical_slot(row)
            assert '"Unknown UNKNOWN operation for row:' in str(excinfo.value)

    def test_fetchone(self, connection):
        pg_base = Base(connection.engine.url.database, verbose=True)
        with patch("pgsync.base.compiled_query") as mock_compiled_query:
            statement = sa.text("SELECT 1")
            row = pg_base.fetchone(statement, label="foo", literal_binds=True)
            assert row == (1,)
            mock_compiled_query.assert_called_once_with(
                statement, label="foo", literal_binds=True
            )

        with pytest.raises(sa.exc.ProgrammingError):
            with patch("pgsync.base.compiled_query") as mock_compiled_query:
                pg_base.fetchone(sa.select(sa.text("x")))
                mock_compiled_query.assert_not_called()

    def test_fetchall(self, connection):
        pg_base = Base(connection.engine.url.database, verbose=True)
        with patch("pgsync.base.compiled_query") as mock_compiled_query:
            statement = sa.text("SELECT 1")
            row = pg_base.fetchall(statement, label="foo", literal_binds=True)
            assert row == [(1,)]
            mock_compiled_query.assert_called_once_with(
                statement, label="foo", literal_binds=True
            )

        with pytest.raises(sa.exc.ProgrammingError):
            with patch("pgsync.base.compiled_query") as mock_compiled_query:
                pg_base.fetchall(sa.select(sa.text("x")))
                mock_compiled_query.assert_not_called()

    def test_count(self, connection, book_cls):
        pg_base = Base(connection.engine.url.database)
        count = pg_base.fetchcount(sa.select(book_cls).alias())
        assert count == 1

    def test_views(self, connection):
        pg_base = Base(connection.engine.url.database)
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA,
                    "mymatview",
                    sa.select(1),
                    materialized=True,
                )
            )
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, "myview", sa.select(1), materialized=False
                )
            )
            conn.commit()
        views = pg_base._views(DEFAULT_SCHEMA)
        assert views == ["myview"]
        with connection.engine.connect() as conn:
            conn.execute(
                DropView(DEFAULT_SCHEMA, "mymatview", materialized=True)
            )
            conn.execute(
                DropView(DEFAULT_SCHEMA, "myview", materialized=False)
            )
            conn.commit()

    def test_materialized_views(self, connection):
        pg_base = Base(connection.engine.url.database)
        with connection.engine.connect() as conn:
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA,
                    "mymatview",
                    sa.select(1),
                    materialized=True,
                )
            )
            conn.execute(
                CreateView(
                    DEFAULT_SCHEMA, "myview", sa.select(1), materialized=False
                )
            )
            conn.commit()
        views = pg_base._materialized_views(DEFAULT_SCHEMA)
        assert views == ["mymatview"]
        with connection.engine.connect() as conn:
            conn.execute(
                DropView(DEFAULT_SCHEMA, "mymatview", materialized=True)
            )
            conn.execute(
                DropView(DEFAULT_SCHEMA, "myview", materialized=False)
            )
            conn.commit()

    def test_pg_execute(self, connection):
        with patch("pgsync.base.logger") as mock_logger:
            pg_execute(
                connection.engine,
                sa.select(1),
                options={"isolation_level": "AUTOCOMMIT"},
            )
            mock_logger.exception.assert_not_called()

        with pytest.raises(Exception) as excinfo:
            pg_execute(
                connection.engine,
                sa.select(1),
                options={None: "AUTOCOMMIT"},
            )
        assert "must be strings" in str(excinfo.value)

    def test_pg_engine(self, connection):
        with pytest.raises(ValueError) as excinfo:
            _pg_engine("mydb", sslmode="foo")
        assert 'Invalid sslmode: "foo"' == str(excinfo.value)

        with pytest.raises(IOError) as excinfo:
            _pg_engine("mydb", sslrootcert="/tmp/foo")
        assert (
            "Provide a valid file containing SSL certificate authority (CA)"
            in str(excinfo.value)
        )

        _pg_engine("mydb", sslmode="allow", sslrootcert=__file__)
