"""Utils tests."""

import os

import pytest
import sqlalchemy as sa
from freezegun import freeze_time
from mock import call, patch

from pgsync.base import Base
from pgsync.settings import ELASTICSEARCH, IS_MYSQL_COMPAT
from pgsync.urls import get_database_url, get_redis_url, get_search_url
from pgsync.utils import (
    compiled_query,
    config_loader,
    exception,
    get_redacted_url,
    show_settings,
    threaded,
    timeit,
    Timer,
    validate_config,
)


@pytest.mark.usefixtures("table_creator")
class TestUtils(object):
    """Utils tests."""

    @classmethod
    def setup_class(cls):
        cls.schema = "testdb" if IS_MYSQL_COMPAT else "public"

    def test_validate_config(self):
        # Test: neither config nor s3_schema_url provided
        with pytest.raises(ValueError) as excinfo:
            validate_config()
        assert (
            "You must provide either a local config path, a valid URL or an S3 URL"
            in str(excinfo.value)
        )

        # Test: non-existent local config file
        with pytest.raises(FileNotFoundError) as excinfo:
            validate_config(config="non_existent.json")
        assert 'Schema config "non_existent.json" not found' in str(
            excinfo.value
        )

        # Test: invalid S3 URL
        with pytest.raises(ValueError) as excinfo:
            validate_config(s3_schema_url="http://example.com/schema.json")
        assert 'Invalid S3 URL: "http://example.com/schema.json"' in str(
            excinfo.value
        )

        # Test: invalid URL
        with pytest.raises(ValueError) as excinfo:
            validate_config(schema_url="ftp://example.com/schema.json")
        assert 'Invalid URL: "ftp://example.com/schema.json"' in str(
            excinfo.value
        )

        # Test: valid local config file (assumes it exists)
        test_config_path = "tests/fixtures/schema.json"
        assert os.path.exists(
            test_config_path
        ), f"Test file missing: {test_config_path}"
        # Should not raise an error
        validate_config(config=test_config_path)

        # Test: valid S3 URL
        validate_config(
            s3_schema_url="s3://bucket/schema.json"
        )  # Should not raise

        # Test: valid URL
        validate_config(
            schema_url="http://example.com/schema.json"
        )  # Should not raise

    def test_config_loader(self):
        os.environ["foo"] = "mydb"
        os.environ["bar"] = "myindex"
        config: str = "tests/fixtures/schema.json"
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
        show_settings(config="tests/fixtures/schema.json")
        calls = [
            call("\x1b[4mSettings\x1b[0m:"),
            call("Schema    : tests/fixtures/schema.json"),
            call("\x1b[4mCheckpoint\x1b[0m:"),
            call("Path: ./"),
            call("\x1b[4mDatabase\x1b[0m:"),
            call(f"URL: {get_database_url(self.schema)}"),
            call(
                f"\x1b[4m{'Elasticsearch' if ELASTICSEARCH else 'OpenSearch'}\x1b[0m:"
            ),
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
        model = pg_base.models("book", self.schema)
        statement = sa.select(*[model.c.isbn]).select_from(model)
        compiled_query(statement, label="foo", literal_binds=True)
        mock_logger.debug.assert_called_once_with(
            f"\x1b[4mfoo:\x1b[0m\nSELECT book_1.isbn\n"
            f"FROM {self.schema}.book AS book_1"
        )
        assert mock_sys.stdout.write.call_count == 3

    @patch("pgsync.utils.sys")
    @patch("pgsync.utils.logger")
    def test_compiled_query_without_label(
        self, mock_logger, mock_sys, connection
    ):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.models("book", self.schema)
        statement = sa.select(*[model.c.isbn]).select_from(model)
        compiled_query(statement, literal_binds=True)
        mock_logger.debug.assert_called_once_with(
            f"SELECT book_1.isbn\nFROM {self.schema}.book AS book_1"
        )
        assert mock_sys.stdout.write.call_count == 3

    def test_get_redacted_url(self):
        url: str = get_redacted_url(
            get_database_url("postgres", user="root", password="bar")
        )
        if IS_MYSQL_COMPAT:
            assert url == "mysql+pymysql://root:***@localhost:3306/postgres"
        else:
            assert (
                url == "postgresql+psycopg2://root:***@localhost:5432/postgres"
            )

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
