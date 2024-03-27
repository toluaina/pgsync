"""Utils tests."""

import os
from urllib.parse import ParseResult, urlparse

import pytest
import sqlalchemy as sa
from freezegun import freeze_time
from mock import call, patch

from pgsync.base import Base
from pgsync.exc import SchemaError
from pgsync.urls import get_postgres_url, get_redis_url, get_search_url
from pgsync.utils import (
    compiled_query,
    config_loader,
    exception,
    get_config,
    get_redacted_url,
    show_settings,
    threaded,
    timeit,
    Timer,
)


@pytest.mark.usefixtures("table_creator")
class TestUtils(object):
    """Utils tests."""

    def test_get_config(self):
        with pytest.raises(SchemaError) as excinfo:
            get_config()
        assert "Schema config not set" in str(excinfo.value)

        with pytest.raises(FileNotFoundError) as excinfo:
            get_config("non_existent")
        assert 'Schema config "non_existent" not found' in str(excinfo.value)
        config: str = get_config("tests/fixtures/schema.json")
        assert config == "tests/fixtures/schema.json"

    def test_config_loader(self):
        os.environ["foo"] = "mydb"
        os.environ["bar"] = "myindex"
        config: str = get_config("tests/fixtures/schema.json")
        data = config_loader(config)
        assert next(data) == {
            "database": "fakedb",
            "index": "fake_index",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title", "description"],
            },
        }
        assert next(data) == {
            "database": "mydb",
            "index": "myindex",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title", "description"],
            },
        }

    @patch("pgsync.utils.logger")
    def test_show_settings(self, mock_logger):
        show_settings(schema="tests/fixtures/schema.json")
        calls = [
            call("\x1b[4mSettings\x1b[0m:"),
            call("Schema    : tests/fixtures/schema.json"),
            call("\x1b[4mCheckpoint\x1b[0m:"),
            call("Path: ./"),
            call("\x1b[4mPostgres\x1b[0m:"),
            call("URL: {get_postgres_url}"),
            call("\x1b[4mElasticsearch\x1b[0m:"),
            call(f"URL: {get_search_url()}"),
            call("\x1b[4mRedis\x1b[0m:"),
            call(f"URL: {get_redis_url}"),
        ]
        assert mock_logger.info.call_args_list[0] == calls[0]
        assert mock_logger.info.call_args_list[1] == calls[1]
        assert mock_logger.info.call_args_list[4] == calls[3]
        assert mock_logger.info.call_args_list[5] == calls[4]
        assert mock_logger.info.call_args_list[7] == calls[6]
        assert mock_logger.info.call_args_list[8] == calls[7]

    @patch("pgsync.utils.sys")
    @freeze_time("2022-01-14")
    def test_timeit(self, mock_sys):
        @timeit
        def fib(n):
            if n == 0:
                return 0
            elif n == 1:
                return 1
            else:
                return fib(n - 1) + fib(n - 2)

        fib(3)
        assert mock_sys.stdout.write.call_count == 5
        assert mock_sys.stdout.write.call_args_list[1] == call(
            "fib: 0.0 secs\n"
        )

    @freeze_time("2020-01-14")
    @patch("pgsync.utils.sys")
    def test_Timer(self, mock_sys):
        def fib(n):
            if n == 0:
                return 0
            elif n == 1:
                return 1
            else:
                return fib(n - 1) + fib(n - 2)

        with Timer("hey"):
            fib(2)
        assert mock_sys.stdout.write.call_count == 1

    @patch("pgsync.utils.sys")
    @patch("pgsync.utils.logger")
    def test_compiled_query_with_label(
        self, mock_logger, mock_sys, connection
    ):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.models("book", "public")
        statement = sa.select(*[model.c.isbn]).select_from(model)
        compiled_query(statement, label="foo", literal_binds=True)
        mock_logger.debug.assert_called_once_with(
            "\x1b[4mfoo:\x1b[0m\nSELECT book_1.isbn\n"
            "FROM public.book AS book_1"
        )
        assert mock_sys.stdout.write.call_count == 3

    @patch("pgsync.utils.sys")
    @patch("pgsync.utils.logger")
    def test_compiled_query_without_label(
        self, mock_logger, mock_sys, connection
    ):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.models("book", "public")
        statement = sa.select(*[model.c.isbn]).select_from(model)
        compiled_query(statement, literal_binds=True)
        mock_logger.debug.assert_called_once_with(
            "SELECT book_1.isbn\nFROM public.book AS book_1"
        )
        assert mock_sys.stdout.write.call_count == 3

    def test_get_redacted_url(self):
        result: ParseResult = get_redacted_url(
            urlparse(get_postgres_url("postgres"))
        )
        assert result.scheme == "postgresql+psycopg2"
        assert result.path == "/postgres"
        assert result.params == ""
        assert result.query == ""
        assert result.fragment == ""

        result: ParseResult = get_redacted_url(
            urlparse(get_postgres_url("postgres", user="root", password="bar"))
        )
        assert result.scheme == "postgresql+psycopg2"
        assert result.path == "/postgres"
        assert result.netloc == "root:***@localhost"
        assert result.params == ""
        assert result.query == ""
        assert result.fragment == ""

    def test_threaded(self):
        @threaded
        def fib(n):
            if n == 0:
                return 0
            elif n == 1:
                return 1
            else:
                return fib(n - 1) + fib(n - 2)

        fib(1)

    @patch("pgsync.utils.sys")
    def test_exception(self, mock_sys):
        @exception
        def foo(n):
            raise RuntimeError

        @exception
        def bar(n):
            pass

        with patch("pgsync.utils.os._exit", return_value=None):
            foo(1)
            mock_sys.stdout.write.assert_called_once_with(
                "Exception in foo() for thread MainThread: ()\nExiting...\n"
            )

        bar(1)
