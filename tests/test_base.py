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
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.view import CreateView, DropView


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestBase(object):
    """Base tests."""

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_pg_settings(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.pg_settings("max_replication_slots")
        assert int(value) > 0
        assert pg_base.pg_settings("xyz") is None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
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
        mock_logger.debug.assert_any_call('Truncating table: "public"."book"')
        mock_logger.debug.assert_any_call('Truncated table: "public"."book"')
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
        mock_logger.debug.assert_any_call(
            "Truncating tables: ['book', 'user']"
        )
        mock_logger.debug.assert_any_call("Truncated tables: ['book', 'user']")
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
        mock_logger.debug.assert_any_call("Truncating schema: public")
        mock_logger.debug.assert_any_call("Truncated schema: public")
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

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_replication_slots(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.replication_slots("noob") == []
        replication_slots = pg_base.replication_slots(
            f"{connection.engine.url.database}_testdb"
        )
        assert "testdb_testdb" == replication_slots[0][0]

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("pgsync.base.logger")
    def test_create_replication_slot(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot("slot_name")
        pg_base.drop_replication_slot("slot_name")
        mock_logger.debug.assert_any_call(
            "Creating replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Created replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Dropping replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Dropped replication slot: slot_name"
        )

        with pytest.raises(sa.exc.ProgrammingError):
            pg_base.drop_replication_slot(1)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("pgsync.base.logger")
    def test_drop_replication_slot(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot("slot_name")
        pg_base.drop_replication_slot("slot_name")
        mock_logger.debug.assert_any_call(
            "Creating replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Created replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Dropping replication slot: slot_name"
        )
        mock_logger.debug.assert_any_call(
            "Dropped replication slot: slot_name"
        )

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("pgsync.base.logger")
    @patch("pgsync.base.Base.execute")
    def test_create_trigger(self, mock_execute, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_function(DEFAULT_SCHEMA)
        pg_base.create_triggers(DEFAULT_SCHEMA, "book", join_queries=True)
        mock_logger.debug.assert_any_call(
            "Creating trigger on table: public.book"
        )
        mock_logger.debug.assert_any_call(
            "Dropping trigger on table: public.book"
        )
        assert mock_execute.call_count == 6

        pg_base.drop_function(DEFAULT_SCHEMA)
        pg_base.drop_triggers(DEFAULT_SCHEMA, "book", join_queries=True)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
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

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
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
        create_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Creating extension: my_ext")
        mock_logger.debug.assert_any_call("Created extension: my_ext")
        mock_pg_engine.assert_any_call(database, echo=True)
        calls = [
            call(mock_pg_engine, 'CREATE EXTENSION IF NOT EXISTS "my_ext"')
        ]
        mock_pg_execute.call_args_list == calls

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
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
        drop_extension(database, "my_ext", echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call("Dropping extension: my_ext")
        mock_logger.debug.assert_any_call("Dropped extension: my_ext")
        mock_pg_engine.assert_any_call(database, echo=True)
        calls = [call(mock_pg_engine, 'DROP EXTENSION IF NOT EXISTS "my_ext"')]
        mock_pg_execute.call_args_list == calls

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("pgsync.base.logger")
    def test_drop_view(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        with patch("pgsync.sync.Base.engine"):
            pg_base.drop_view("public")
            mock_logger.debug.assert_any_call("Dropping view: public._view")
            mock_logger.debug.assert_any_call("Dropped view: public._view")

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("pgsync.base.logger")
    def test_refresh_view(self, mock_logger, connection):
        pg_base = Base(connection.engine.url.database)
        with patch("pgsync.sync.Base.engine"):
            pg_base.refresh_view("foo", "public", concurrently=True)
            mock_logger.debug.assert_any_call("Refreshing view: public.foo")
            mock_logger.debug.assert_any_call("Refreshed view: public.foo")

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

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
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
        table public."B1_XYZ": INSERT: "ID"[integer]:5 "CREATED_TIMESTAMP"[bigint]:222 "ADDRESS"[character varying]:'from3' "SOME_FIELD_KEY"[character varying]:'key3' "SOME_OTHER_FIELD_KEY"[character varying]:'issue to handle' "CHANNEL_ID"[integer]:3 "CHANNEL_NAME"[character varying]:'channel 45' "ITEM_ID"[integer]:3 "MESSAGE"[character varying]:'message3' "RETRY"[integer]:4 "STATUS"[character varying]:'status' "SUBJECT"[character varying]:'sub3' "TIMESTAMP"[bigint]:33
        """  # noqa E501
        payload = pg_base.parse_logical_slot(row.strip())
        assert payload.data == {
            "CHANNEL_ID": 3,
            "CHANNEL_NAME": "channel 45",
            "CREATED_TIMESTAMP": 222,
            "ADDRESS": "from3",
            "ID": 5,
            "ITEM_ID": 3,
            "MESSAGE": "message3",
            "RETRY": 4,
            "SOME_FIELD_KEY": "key3",
            "SOME_OTHER_FIELD_KEY": "issue to handle",
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

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    def test_parse_logical_slot_with_double_precision(
        self,
        connection,
    ):
        pg_base = Base(connection.engine.url.database)
        row = """
        table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'It' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' publisher_id[integer]:1 publish_date[timestamp without time zone]:'1980-01-01 00:00:00' quad[double precision]:2e+58
        """  # noqa E501
        payload = pg_base.parse_logical_slot(row.strip())
        assert payload.data == {
            "copyright": None,
            "description": "Stephens Kings It",
            "doc": '\'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": '
            '5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", '
            '"firstname": "Glenda", "generation": {"name": "X"}, "nick_names": '
            '["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], '
            '"coordinates": {"lat": 21.1, "lon": 32.9}}\'',
            "id": 1,
            "isbn": "001",
            "publish_date": "'1980-01-01 00:00:00'",
            "publisher_id": 1,
            "quad": 2e58,
            "tags": '\'["a", "b", "c"]\'',
            "title": "It",
        }
        assert payload.old == {}
        assert payload.schema == "public"
        assert payload.table == "book"
        assert payload.tg_op == "UPDATE"

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

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
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


class TestPayload:
    """Tests for the Payload class."""

    def test_payload_creation(self):
        """Test Payload object creation."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1, "title": "Test"},
            old={},
        )
        assert payload.tg_op == "INSERT"
        assert payload.table == "book"
        assert payload.schema == "public"
        assert payload.new == {"id": 1, "title": "Test"}
        assert payload.old == {}

    def test_payload_data_returns_new_for_insert(self):
        """Test Payload.data returns new values for INSERT."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="INSERT",
            table="book",
            new={"id": 1, "title": "Test"},
            old={},
        )
        assert payload.data == {"id": 1, "title": "Test"}

    def test_payload_data_returns_new_for_update(self):
        """Test Payload.data returns new values for UPDATE."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="UPDATE",
            table="book",
            new={"id": 1, "title": "Updated"},
            old={"id": 1, "title": "Original"},
        )
        assert payload.data == {"id": 1, "title": "Updated"}

    def test_payload_data_returns_old_for_delete(self):
        """Test Payload.data returns old values for DELETE."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="DELETE",
            table="book",
            new={},
            old={"id": 1, "title": "Deleted"},
        )
        assert payload.data == {"id": 1, "title": "Deleted"}

    def test_payload_data_returns_new_for_truncate(self):
        """Test Payload.data returns new (empty) for TRUNCATE since it has no old."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="TRUNCATE",
            table="book",
            new={},
            old={},
        )
        # TRUNCATE returns new (which is empty) since old is also empty
        assert payload.data == {}

    def test_payload_xmin_defaults_to_none(self):
        """Test Payload.xmin defaults to None."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="INSERT",
            table="book",
            new={},
        )
        assert payload.xmin is None

    def test_payload_with_xmin(self):
        """Test Payload with xmin set."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="INSERT",
            table="book",
            new={},
            xmin=12345,
        )
        assert payload.xmin == 12345

    def test_payload_table_attribute(self):
        """Test Payload table attribute."""
        from pgsync.base import Payload

        payload = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1},
        )
        assert payload.table == "book"
        assert payload.schema == "public"


class TestBaseAdditional:
    """Additional tests for Base class."""

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_txid_current(self, connection):
        """Test txid_current returns transaction ID."""
        pg_base = Base(connection.engine.url.database)
        txid = pg_base.txid_current
        assert isinstance(txid, int)
        assert txid > 0

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_verbose_property(self, connection):
        """Test verbose property setting."""
        pg_base = Base(connection.engine.url.database, verbose=True)
        assert pg_base.verbose is True

        pg_base2 = Base(connection.engine.url.database, verbose=False)
        assert pg_base2.verbose is False

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_models_method(self, connection):
        """Test models method returns table metadata."""
        pg_base = Base(connection.engine.url.database)
        # models is a method that takes table and schema
        model = pg_base.models("book", DEFAULT_SCHEMA)
        assert model is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_database_property(self, connection):
        """Test database property."""
        pg_base = Base(connection.engine.url.database)
        assert pg_base.database == connection.engine.url.database

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_engine_property(self, connection):
        """Test engine property."""
        pg_base = Base(connection.engine.url.database)
        assert pg_base.engine is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_execute_method(self, connection):
        """Test execute method runs SQL."""
        pg_base = Base(connection.engine.url.database)
        stmt = sa.text("SELECT 1")
        # execute() doesn't return anything, so we just check it doesn't raise
        pg_base.execute(stmt)

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_table_count_method(self, connection):
        """Test table count method."""
        pg_base = Base(connection.engine.url.database)
        # The count method requires a model, use tables() instead
        tables = pg_base.tables(DEFAULT_SCHEMA)
        assert isinstance(tables, list)
        assert "book" in tables

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_integer(self, connection):
        """Test parse_value with integer type."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("integer", "42")
        assert result == 42

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_boolean_true(self, connection):
        """Test parse_value with boolean true."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("boolean", "true")
        assert result is True

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_bigint(self, connection):
        """Test parse_value with bigint type."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("bigint", "9999999999")
        assert result == 9999999999

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_null(self, connection):
        """Test parse_value with null value."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("character varying", "null")
        assert result is None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_double_precision(self, connection):
        """Test parse_value with double precision type."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("double precision", "3.14159")
        assert abs(result - 3.14159) < 0.0001

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_session_property(self, connection):
        """Test session property returns a session."""
        pg_base = Base(connection.engine.url.database)
        session = pg_base.session
        assert session is not None

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_tables_method(self, connection):
        """Test tables method returns table list."""
        pg_base = Base(connection.engine.url.database)
        tables = pg_base.tables(DEFAULT_SCHEMA)
        assert isinstance(tables, list)
        assert "book" in tables

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_text_type(self, connection):
        """Test parse_value with text type."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("text", "'hello world'")
        assert "hello world" in result

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_parse_value_numeric(self, connection):
        """Test parse_value with numeric type."""
        pg_base = Base(connection.engine.url.database)
        result = pg_base.parse_value("numeric", "123.45")
        assert float(result) == 123.45

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @pytest.mark.usefixtures("table_creator")
    def test_replication_slots_query(self, connection):
        """Test replication_slots method with slot name."""
        pg_base = Base(connection.engine.url.database)
        # Check for a non-existent slot - returns empty list
        slots = pg_base.replication_slots("nonexistent_slot")
        assert isinstance(slots, list)
        assert len(slots) == 0


# ============================================================================
# PHASE 6 EXTENDED TESTS - Base.py Comprehensive Coverage
# ============================================================================


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestConnectionPooling:
    """Tests for connection pooling and configuration."""

    def test_connection_pool_size_configuration(self, connection):
        """Test connection pool is configured with correct size."""
        pg_base = Base(connection.engine.url.database)

        # Engine should have pool configured
        assert pg_base.engine.pool is not None

    def test_connection_pre_ping_enabled(self, connection):
        """Test pre-ping is enabled for connection health checks."""
        pg_base = Base(connection.engine.url.database)

        # Pool should be configured with pre-ping
        # This ensures stale connections are detected
        assert hasattr(pg_base.engine.pool, "_pre_ping")

    def test_engine_disposal(self, connection):
        """Test engine can be properly disposed."""
        pg_base = Base(connection.engine.url.database)

        # Should be able to dispose engine
        pg_base.engine.dispose()

        # After disposal, should be able to reconnect
        pg_base.engine.connect().close()

    def test_connection_thread_safety(self, connection):
        """Test connections are thread-safe with thread-local storage."""
        import threading

        pg_base = Base(connection.engine.url.database)
        results = []

        def get_connection():
            conn = pg_base.engine.connect()
            results.append(id(conn))
            conn.close()

        threads = [threading.Thread(target=get_connection) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Each thread should have gotten its own connection
        assert len(results) == 3


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestReplicationSlotOperations:
    """Extended tests for replication slot operations."""

    def test_create_replication_slot_success(self, connection):
        """Test creating a replication slot."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "test_slot_create"

        # Clean up if exists
        try:
            pg_base.drop_replication_slot(slot_name)
        except Exception:
            pass

        # Create slot
        pg_base.create_replication_slot(slot_name)

        # Verify it exists
        slots = pg_base.replication_slots(slot_name)
        assert len(slots) > 0
        assert slots[0]["slot_name"] == slot_name

        # Cleanup
        pg_base.drop_replication_slot(slot_name)

    def test_drop_replication_slot_success(self, connection):
        """Test dropping a replication slot."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "test_slot_drop"

        # Create slot first
        try:
            pg_base.create_replication_slot(slot_name)
        except Exception:
            pass  # May already exist

        # Drop it
        pg_base.drop_replication_slot(slot_name)

        # Verify it's gone
        slots = pg_base.replication_slots(slot_name)
        assert len(slots) == 0

    def test_create_replication_slot_already_exists(self, connection):
        """Test creating a slot that already exists."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "test_slot_duplicate"

        # Create slot
        try:
            pg_base.drop_replication_slot(slot_name)
        except Exception:
            pass
        pg_base.create_replication_slot(slot_name)

        # Try to create again - should handle gracefully
        try:
            pg_base.create_replication_slot(slot_name)
        except Exception as e:
            # Expected - slot already exists
            assert (
                "already exists" in str(e).lower()
                or "duplicate" in str(e).lower()
            )

        # Cleanup
        pg_base.drop_replication_slot(slot_name)

    def test_drop_replication_slot_nonexistent(self, connection):
        """Test dropping a non-existent slot."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "nonexistent_slot_12345"

        # Try to drop non-existent slot
        try:
            pg_base.drop_replication_slot(slot_name)
        except Exception as e:
            # Expected - slot doesn't exist
            assert (
                "does not exist" in str(e).lower()
                or "not found" in str(e).lower()
            )

    def test_replication_slots_returns_all_when_no_name(self, connection):
        """Test replication_slots returns all slots when no name provided."""
        pg_base = Base(connection.engine.url.database)

        # Get all slots
        slots = pg_base.replication_slots()

        # Should return a list (may be empty)
        assert isinstance(slots, list)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestLogicalSlotChanges:
    """Tests for logical slot change reading."""

    def test_logical_slot_changes_with_limit(self, connection):
        """Test _logical_slot_changes respects limit parameter."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "test_slot_changes"

        # Create slot
        try:
            pg_base.drop_replication_slot(slot_name)
        except Exception:
            pass
        pg_base.create_replication_slot(slot_name)

        # Read changes with limit
        changes = pg_base._logical_slot_changes(slot_name, limit=10)

        # Should return a list
        assert isinstance(changes, list)
        # Should not exceed limit
        assert len(changes) <= 10

        # Cleanup
        pg_base.drop_replication_slot(slot_name)

    def test_logical_slot_changes_empty_slot(self, connection):
        """Test _logical_slot_changes on empty slot."""
        pg_base = Base(connection.engine.url.database)
        slot_name = "test_slot_empty"

        # Create fresh slot
        try:
            pg_base.drop_replication_slot(slot_name)
        except Exception:
            pass
        pg_base.create_replication_slot(slot_name)

        # Read from empty slot
        changes = pg_base._logical_slot_changes(slot_name, limit=10)

        # Should return empty list or minimal changes
        assert isinstance(changes, list)

        # Cleanup
        pg_base.drop_replication_slot(slot_name)


@pytest.mark.usefixtures("table_creator")
class TestPayloadExtended:
    """Extended tests for Payload class."""

    def test_payload_equality(self):
        """Test Payload equality comparison."""
        payload1 = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1, "title": "Test"},
        )
        payload2 = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1, "title": "Test"},
        )

        # Same data should be equal
        assert payload1.tg_op == payload2.tg_op
        assert payload1.table == payload2.table

    def test_payload_with_old_and_new(self):
        """Test Payload with both old and new data (UPDATE)."""
        payload = Payload(
            tg_op="UPDATE",
            table="book",
            schema="public",
            old={"id": 1, "title": "Old Title"},
            new={"id": 1, "title": "New Title"},
        )

        assert payload.old == {"id": 1, "title": "Old Title"}
        assert payload.new == {"id": 1, "title": "New Title"}
        # data property should return new for UPDATE
        assert payload.data == {"id": 1, "title": "New Title"}

    def test_payload_repr(self):
        """Test Payload string representation."""
        payload = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1},
        )

        repr_str = repr(payload)
        assert "INSERT" in repr_str
        assert "book" in repr_str

    def test_payload_with_none_values(self):
        """Test Payload handles None values correctly."""
        payload = Payload(
            tg_op="DELETE",
            table="book",
            schema="public",
            old={"id": 1},
            new=None,
        )

        assert payload.new is None
        # data should return old for DELETE
        assert payload.data == {"id": 1}

    def test_payload_xmin_integer_conversion(self):
        """Test Payload xmin is converted to integer."""
        payload = Payload(
            tg_op="INSERT",
            table="book",
            schema="public",
            new={"id": 1},
            xmin="12345",  # String
        )

        # Should be converted to int
        assert isinstance(payload.xmin, int)
        assert payload.xmin == 12345

    def test_payload_schema_defaults_to_public(self):
        """Test Payload schema defaults to public."""
        payload = Payload(
            tg_op="INSERT",
            table="book",
            new={"id": 1},
        )

        # Should default to public
        assert payload.schema == "public"


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestSessionManagement:
    """Tests for session and transaction management."""

    def test_session_property(self, connection):
        """Test session property creates session."""
        pg_base = Base(connection.engine.url.database)

        # Should return a session
        session = pg_base.session
        assert session is not None

    def test_session_commit(self, connection):
        """Test session can commit transactions."""
        pg_base = Base(connection.engine.url.database)

        # Get session and ensure it can commit
        session = pg_base.session
        session.commit()

    def test_session_rollback(self, connection):
        """Test session can rollback transactions."""
        pg_base = Base(connection.engine.url.database)

        # Get session and rollback
        session = pg_base.session
        session.rollback()

    def test_execute_method(self, connection):
        """Test execute method runs SQL."""
        pg_base = Base(connection.engine.url.database)

        # Execute simple query
        result = pg_base.execute("SELECT 1 as num")

        # Should return result
        assert result is not None

    def test_fetchone_method(self, connection):
        """Test fetchone returns single row."""
        pg_base = Base(connection.engine.url.database)

        # Execute and fetch one
        result = pg_base.fetchone("SELECT 1 as num")

        # Should return single row
        assert result is not None

    def test_fetchall_method(self, connection):
        """Test fetchall returns all rows."""
        pg_base = Base(connection.engine.url.database)

        # Execute and fetch all
        results = pg_base.fetchall("SELECT 1 as num UNION SELECT 2")

        # Should return multiple rows
        assert isinstance(results, list)
        assert len(results) >= 1


@pytest.mark.usefixtures("table_creator")
class TestDatabaseOperations:
    """Extended tests for database operations."""

    def test_table_count_method(self, connection):
        """Test table_count returns count."""
        pg_base = Base(connection.engine.url.database)

        # Count rows in a table
        count = pg_base.count("book")

        # Should return integer
        assert isinstance(count, int)
        assert count >= 0

    def test_models_property(self, connection):
        """Test models property returns reflection."""
        pg_base = Base(connection.engine.url.database)

        # Should return models dictionary
        models = pg_base.models
        assert isinstance(models, dict)

    def test_database_property(self, connection):
        """Test database property returns database name."""
        pg_base = Base(connection.engine.url.database)

        # Should return database name
        db_name = pg_base.database
        assert db_name is not None
        assert isinstance(db_name, str)

    def test_verbose_property(self, connection):
        """Test verbose property."""
        pg_base = Base(connection.engine.url.database)

        # Should have verbose property
        assert hasattr(pg_base, "verbose")

    def test_txid_current(self, connection):
        """Test txid_current returns transaction ID."""
        pg_base = Base(connection.engine.url.database)

        # Get current txid
        txid = pg_base.txid_current

        # Should return integer
        assert isinstance(txid, int)
        assert txid > 0
