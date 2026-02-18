"""Utils tests."""

import os

import pytest
import sqlalchemy as sa
from freezegun import freeze_time
from mock import call, patch

from pgsync import settings
from pgsync.base import Base
from pgsync.settings import IS_MYSQL_COMPAT
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
        search_backend = (
            "Elasticsearch" if settings.ELASTICSEARCH else "OpenSearch"
        )
        calls = [
            call("\x1b[4mSettings\x1b[0m:"),
            call("Schema    : tests/fixtures/schema.json"),
            call("\x1b[4mCheckpoint\x1b[0m:"),
            call("Path: ./"),
            call("\x1b[4mDatabase\x1b[0m:"),
            call(f"URL: {get_database_url(self.schema)}"),
            call(f"\x1b[4m{search_backend}\x1b[0m:"),
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


# ============================================================================
# PHASE 8 EXTENDED TESTS - Utils.py Comprehensive Coverage
# ============================================================================


class TestRemapUnknown:
    """Tests for remap_unknown MySQL column name remapping."""

    def test_remap_unknown_cols(self):
        """Test remap_unknown remaps UNKNOWN_COL keys to real names."""
        from pgsync.utils import remap_unknown

        values = {"UNKNOWN_COL0": "v1", "UNKNOWN_COL1": "v2"}
        with patch("pgsync.utils._cols", return_value=["isbn", "title"]):
            result = remap_unknown(None, "public", "book", values)
        assert result == {"isbn": "v1", "title": "v2"}

    def test_remap_unknown_passthrough_normal_keys(self):
        """Test remap_unknown returns values unchanged for normal keys."""
        from pgsync.utils import remap_unknown

        values = {"isbn": "001", "title": "Test"}
        result = remap_unknown(None, "public", "book", values)
        assert result == {"isbn": "001", "title": "Test"}

    def test_remap_unknown_empty_values(self):
        """Test remap_unknown returns empty dict for empty input."""
        from pgsync.utils import remap_unknown

        result = remap_unknown(None, "public", "book", {})
        assert result == {}

    def test_remap_unknown_none_values(self):
        """Test remap_unknown returns None for None input."""
        from pgsync.utils import remap_unknown

        result = remap_unknown(None, "public", "book", None)
        assert result is None

    def test_remap_unknown_fallback_index(self):
        """Test remap_unknown uses fallback for out-of-range index."""
        from pgsync.utils import remap_unknown

        values = {"UNKNOWN_COL0": "v1", "UNKNOWN_COL5": "v2"}
        with patch("pgsync.utils._cols", return_value=["isbn"]):
            result = remap_unknown(None, "public", "book", values)
        assert result == {"isbn": "v1", "@6": "v2"}

    def test_remap_unknown_mixed_keys_no_remap(self):
        """Test remap_unknown skips remapping with mixed key types."""
        from pgsync.utils import remap_unknown

        values = {"UNKNOWN_COL0": "v1", "real_col": "v2"}
        result = remap_unknown(None, "public", "book", values)
        assert result == {"UNKNOWN_COL0": "v1", "real_col": "v2"}


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="PostgreSQL-specific qname tests",
)
@pytest.mark.usefixtures("table_creator")
class TestQnameExtended:
    """Extended tests for qname function with different dialects."""

    def test_qname_with_schema_and_table(self, connection):
        """Test qname with both schema and table."""
        from pgsync.utils import qname

        result = qname(connection.engine, schema="public", table="book")
        assert "public" in result
        assert "book" in result

    def test_qname_table_only(self, connection):
        """Test qname with table only."""
        from pgsync.utils import qname

        result = qname(connection.engine, table="book")
        assert "book" in result

    def test_qname_quoted_identifiers(self, connection):
        """Test qname handles identifiers that need quoting."""
        from pgsync.utils import qname

        # Table with special characters
        result = qname(connection.engine, schema="public", table="my-table")
        assert isinstance(result, str)


class TestMutuallyExclusiveOptionExtended:
    """Extended tests for MutuallyExclusiveOption Click option."""

    def test_mutually_exclusive_option_instantiation(self):
        """Test MutuallyExclusiveOption can be instantiated."""
        from pgsync.utils import MutuallyExclusiveOption

        option = MutuallyExclusiveOption(
            param_decls=["--option"],
            mutually_exclusive=["other_option"],
        )

        assert option is not None
        assert hasattr(option, "mutually_exclusive")

    def test_mutually_exclusive_option_with_empty_list(self):
        """Test MutuallyExclusiveOption with empty exclusion list."""
        from pgsync.utils import MutuallyExclusiveOption

        option = MutuallyExclusiveOption(
            param_decls=["--option"],
            mutually_exclusive=[],
        )

        assert option.mutually_exclusive == set()

    def test_mutually_exclusive_option_multiple_exclusions(self):
        """Test MutuallyExclusiveOption with multiple exclusions."""
        from pgsync.utils import MutuallyExclusiveOption

        option = MutuallyExclusiveOption(
            param_decls=["--option"],
            mutually_exclusive=["option1", "option2", "option3"],
        )

        assert len(option.mutually_exclusive) == 3


class TestChunksExtended:
    """Extended tests for chunks generator."""

    def test_chunks_generator_protocol(self):
        """Test chunks returns a generator."""
        from pgsync.utils import chunks

        result = chunks([1, 2, 3, 4, 5], 2)

        # Should be iterable
        assert hasattr(result, "__iter__")

    def test_chunks_preserves_order(self):
        """Test chunks preserves element order."""
        from pgsync.utils import chunks

        data = [1, 2, 3, 4, 5, 6]
        result = list(chunks(data, 2))

        # Should maintain order
        flattened = [item for chunk in result for item in chunk]
        assert flattened == data

    def test_chunks_with_strings(self):
        """Test chunks works with string sequences."""
        from pgsync.utils import chunks

        result = list(chunks("abcdefgh", 3))

        assert len(result) == 3
        assert result[0] == "abc"
        assert result[1] == "def"
        assert result[2] == "gh"

    def test_chunks_with_tuples(self):
        """Test chunks works with tuples."""
        from pgsync.utils import chunks

        data = (1, 2, 3, 4, 5)
        result = list(chunks(data, 2))

        assert len(result) == 3
        assert result[0] == (1, 2)
        assert result[1] == (3, 4)
        assert result[2] == (5,)


class TestUtilityHelpers:
    """Extended tests for utility helper functions."""

    def test_format_number_negative(self):
        """Test format_number with negative numbers."""
        from pgsync.utils import format_number

        result = format_number(-12345)
        assert "-" in str(result)

    @patch("pgsync.utils.logger")
    def test_show_settings_output(self, mock_logger):
        """Test show_settings logs all settings."""
        from pgsync.utils import show_settings

        show_settings()

        # Should have logged something
        assert mock_logger.info.called

    def test_compiled_query_with_bind_parameters(self):
        """Test compiled_query handles bind parameters."""
        import sqlalchemy as sa

        from pgsync.utils import compiled_query

        query = sa.select(sa.literal(1).label("num")).where(
            sa.literal(1) == sa.bindparam("value")
        )

        # Should not raise an exception (returns None, just logs/prints)
        result = compiled_query(query)
        assert result is None

    def test_timer_context_manager(self):
        """Test Timer context manager."""
        import time

        from pgsync.utils import Timer

        with Timer("test_operation"):
            time.sleep(0.01)

        # Should complete without error


class TestExceptionHandling:
    """Extended tests for exception decorator."""

    def test_exception_decorator_with_function(self):
        """Test exception decorator on regular function."""
        from pgsync.utils import exception

        @exception
        def test_func():
            return "success"

        result = test_func()
        assert result == "success"

    def test_exception_decorator_catches_exception(self):
        """Test exception decorator catches and logs exceptions."""
        from pgsync.utils import exception

        @exception
        def failing_func():
            raise ValueError("Test error")

        # Should catch the exception and not raise (must patch os._exit)
        with patch("pgsync.utils.os._exit", return_value=None):
            result = failing_func()
            assert result is None

    def test_exception_decorator_with_kwargs(self):
        """Test exception decorator with keyword arguments."""
        from pgsync.utils import exception

        @exception
        def func_with_kwargs(a=1, b=2):
            return a + b

        result = func_with_kwargs(a=5, b=10)
        assert result == 15
