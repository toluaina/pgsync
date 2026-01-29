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

    def test_get_redacted_url_no_password(self):
        """Test get_redacted_url with URL that has no password."""
        url = get_redacted_url("postgresql+psycopg2://user@localhost:5432/db")
        assert "***" not in url
        assert "user" in url

    def test_get_redacted_url_with_special_chars(self):
        """Test get_redacted_url with special characters in password."""
        url = get_redacted_url(
            get_database_url(
                "postgres", user="admin", password="p@ss!word#123"
            )
        )
        assert "***" in url
        assert "p@ss" not in url

    def test_config_loader_nonexistent_file(self):
        """Test config_loader with non-existent file."""
        with pytest.raises(FileNotFoundError):
            list(config_loader("nonexistent_config.json"))

    def test_validate_config_with_valid_https_url(self):
        """Test validate_config accepts HTTPS URLs."""
        # Should not raise
        validate_config(schema_url="https://example.com/schema.json")

    def test_validate_config_with_valid_s3_url(self):
        """Test validate_config accepts S3 URLs."""
        # Should not raise
        validate_config(s3_schema_url="s3://my-bucket/path/to/schema.json")


class TestChunks:
    """Tests for the chunks utility function."""

    def test_chunks_basic(self):
        """Test chunks splits list correctly."""
        from pgsync.utils import chunks

        data = [1, 2, 3, 4, 5]
        result = list(chunks(data, 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunks_exact_split(self):
        """Test chunks with exact multiple of chunk size."""
        from pgsync.utils import chunks

        data = [1, 2, 3, 4, 5, 6]
        result = list(chunks(data, 2))
        assert result == [[1, 2], [3, 4], [5, 6]]

    def test_chunks_larger_than_list(self):
        """Test chunks when chunk size is larger than list."""
        from pgsync.utils import chunks

        data = [1, 2, 3]
        result = list(chunks(data, 10))
        assert result == [[1, 2, 3]]

    def test_chunks_empty_list(self):
        """Test chunks with empty list."""
        from pgsync.utils import chunks

        data = []
        result = list(chunks(data, 2))
        assert result == []

    def test_chunks_single_item(self):
        """Test chunks with single item."""
        from pgsync.utils import chunks

        data = [1]
        result = list(chunks(data, 2))
        assert result == [[1]]


@pytest.mark.usefixtures("table_creator")
class TestQname:
    """Tests for the qname utility function."""

    def test_qname_basic(self, connection):
        """Test qname builds qualified name."""
        from pgsync.utils import qname

        result = qname(connection.engine, "myschema", "mytable")
        assert "mytable" in result
        assert "myschema" in result

    def test_qname_default_schema(self, connection):
        """Test qname with default public schema."""
        from pgsync.utils import qname

        result = qname(connection.engine, "public", "mytable")
        assert "mytable" in result

    def test_qname_no_schema(self, connection):
        """Test qname without schema."""
        from pgsync.utils import qname

        result = qname(connection.engine, None, "mytable")
        assert "mytable" in result


class TestFormatNumber:
    """Tests for the format_number utility function."""

    def test_format_number_zero(self):
        """Test format_number with zero."""
        from pgsync.utils import format_number

        result = format_number(0)
        assert result == "0"

    def test_format_number_small(self):
        """Test format_number with small number."""
        from pgsync.utils import format_number

        result = format_number(999)
        assert result == "999"

    def test_format_number_comma_separated(self):
        """Test format_number returns comma-separated numbers."""
        from pgsync.utils import format_number

        result = format_number(1500)
        # The function uses comma formatting
        assert result == "1,500"

    def test_format_number_large(self):
        """Test format_number with large number."""
        from pgsync.utils import format_number

        result = format_number(2500000)
        assert result == "2,500,000"


class TestMutuallyExclusiveOption:
    """Tests for MutuallyExclusiveOption."""

    def test_mutually_exclusive_option_import(self):
        """Test MutuallyExclusiveOption can be imported."""
        from pgsync.utils import MutuallyExclusiveOption

        assert MutuallyExclusiveOption is not None

    def test_mutually_exclusive_init(self):
        """Test MutuallyExclusiveOption initialization."""
        import click

        from pgsync.utils import MutuallyExclusiveOption

        @click.command()
        @click.option(
            "--opt1",
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["opt2"],
        )
        @click.option("--opt2")
        def cmd(opt1, opt2):
            pass

        # Should be created successfully
        assert cmd is not None


class TestTimerContext:
    """Tests for Timer context manager."""

    def test_timer_basic(self):
        """Test Timer context manager."""
        import time

        from pgsync.utils import Timer

        with patch("pgsync.utils.sys") as mock_sys:
            with Timer("test_operation"):
                time.sleep(0.01)
            mock_sys.stdout.write.assert_called()

    def test_timer_nested(self):
        """Test nested Timer contexts."""
        from pgsync.utils import Timer

        with patch("pgsync.utils.sys"):
            with Timer("outer"):
                with Timer("inner"):
                    pass


class TestExceptionDecorator:
    """Tests for exception decorator."""

    def test_exception_no_error(self):
        """Test exception decorator with no error."""
        from pgsync.utils import exception

        @exception
        def no_error_func():
            return "success"

        result = no_error_func()
        assert result == "success"

    def test_exception_with_args(self):
        """Test exception decorator preserves args."""
        from pgsync.utils import exception

        @exception
        def func_with_args(a, b):
            return a + b

        result = func_with_args(1, 2)
        assert result == 3


class TestThreadedDecorator:
    """Tests for threaded decorator."""

    def test_threaded_basic(self):
        """Test threaded decorator creates thread."""
        import threading

        from pgsync.utils import threaded

        @threaded
        def threaded_func():
            return threading.current_thread().name

        thread = threaded_func()
        assert isinstance(thread, threading.Thread)

    def test_threaded_with_args(self):
        """Test threaded decorator with arguments."""
        from pgsync.utils import threaded

        results = []

        @threaded
        def threaded_func(value):
            results.append(value)

        thread = threaded_func(42)
        thread.join(timeout=1)
        assert 42 in results


class TestCompiledQuery:
    """Tests for compiled_query function."""

    @pytest.mark.usefixtures("table_creator")
    def test_compiled_query_literal_binds_false(self, connection):
        """Test compiled_query without literal_binds."""
        from pgsync.utils import compiled_query

        pg_base = Base(connection.engine.url.database)
        model = pg_base.models(
            "book", "public" if not IS_MYSQL_COMPAT else "testdb"
        )
        statement = sa.select(model.c.isbn)

        with patch("pgsync.utils.logger"):
            with patch("pgsync.utils.sys"):
                compiled_query(statement, literal_binds=False)


class TestGetRedactedUrl:
    """Additional tests for get_redacted_url."""

    def test_redacted_url_empty_password(self):
        """Test redacted URL with empty password."""
        from pgsync.utils import get_redacted_url

        url = get_redacted_url("postgresql://user:@localhost/db")
        assert "***" not in url or url.count("***") == 0

    def test_redacted_url_complex(self):
        """Test redacted URL with complex URL."""
        from pgsync.utils import get_redacted_url

        url = get_redacted_url(
            "postgresql://admin:secret123@db.example.com:5432/mydb?sslmode=require"
        )
        assert "secret123" not in url
        assert "***" in url
