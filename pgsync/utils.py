"""PGSync utils."""

import json
import logging
import os
import re
import sys
import tempfile
import threading
import typing as t
from datetime import timedelta
from string import Template
from time import time
from urllib.parse import ParseResult, urlparse

import boto3
import click
import requests
import sqlalchemy as sa
import sqlparse

from . import settings
from .urls import get_database_url, get_redis_url, get_search_url

logger = logging.getLogger(__name__)

HIGHLIGHT_BEGIN = "\033[4m"
HIGHLIGHT_END = "\033[0m:"

# Regular expression to match placeholder column names (e.g., "UNKNOWN_COL1") used for remapping
_UNKNOWN_RE = re.compile(r"^UNKNOWN_COL(\d+)$")

# Cache for storing column name mappings for (database, table) pairs, used in MySQL column remapping
_col_cache: dict[tuple[str, str], list[str]] = {}


def chunks(sequence: t.Sequence, size: int) -> t.Iterable[t.Sequence]:
    """Yield successive n-sized chunks from sequence"""
    for i in range(0, len(sequence), size):
        yield sequence[i : i + size]


def timeit(func: t.Callable):
    def timed(*args, **kwargs):
        since: float = time()
        fn = func(*args, **kwargs)
        until: float = time()
        sys.stdout.write(f"{func.__name__}: {until-since} secs\n")
        return fn

    return timed


class Timer:
    def __init__(self, message: t.Optional[str] = None):
        self.message: str = message or ""

    def __enter__(self):
        self.start: float = time()
        return self

    def __exit__(self, *args):
        elapsed: float = time() - self.start
        sys.stdout.write(
            f"{self.message} {(timedelta(seconds=elapsed))} "
            f"({elapsed:2.2f} sec)\n"
        )


def threaded(func: t.Callable):
    """Decorator for threaded code execution."""

    def wrapper(*args, **kwargs) -> threading.Thread:
        thread: threading.Thread = threading.Thread(
            target=func, args=args, kwargs=kwargs
        )
        thread.start()
        return thread

    return wrapper


def exception(func: t.Callable):
    """Decorator for threaded exception handling."""

    def wrapper(*args, **kwargs) -> t.Callable:
        try:
            fn = func(*args, **kwargs)
        except Exception as e:
            name: str = threading.current_thread().name
            err = e.args[0] if len(e.args) > 0 else e.args
            sys.stdout.write(
                f"Exception in {func.__name__}() for thread {name}: {err}\n"
                f"Exiting...\n"
            )
            os._exit(-1)
        else:
            return fn

    return wrapper


def format_number(n: int) -> str:
    """
    Format a number with commas if the setting is enabled."""
    return f"{n:,}" if settings.FORMAT_WITH_COMMAS else f"{n}"


def get_redacted_url(url: str) -> str:
    """
    Returns a redacted version of the input URL, with the password replaced by asterisks.
    """
    parsed_url: ParseResult = urlparse(url)
    if parsed_url.password:
        username = parsed_url.username or ""
        hostname = parsed_url.hostname or ""
        port = f":{parsed_url.port}" if parsed_url.port else ""
        redacted_password = "*" * len(parsed_url.password)
        netloc: str = f"{username}:{redacted_password}@{hostname}{port}"
        parsed_url = parsed_url._replace(netloc=netloc)
    return parsed_url.geturl()


def show_settings(
    config: t.Optional[str] = None,
    schema_url: t.Optional[str] = None,
    s3_schema_url: t.Optional[str] = None,
) -> None:
    """Show settings."""
    logger.info(f"{HIGHLIGHT_BEGIN}Settings{HIGHLIGHT_END}")
    logger.info(f'{"Schema":<10s}: {config or schema_url or s3_schema_url}')
    logger.info("-" * 65)
    logger.info(f"{HIGHLIGHT_BEGIN}Checkpoint{HIGHLIGHT_END}")
    logger.info(f"Path: {settings.CHECKPOINT_PATH}")
    logger.info(f"{HIGHLIGHT_BEGIN}Database{HIGHLIGHT_END}")

    database: str = (
        "information_schema" if settings.IS_MYSQL_COMPAT else "postgres"
    )
    url: str = get_database_url(database)
    redacted_url: str = get_redacted_url(url)
    logger.info(f"URL: {redacted_url}")

    url: str = get_search_url()
    redacted_url: str = get_redacted_url(url)
    logger.info(
        f"{HIGHLIGHT_BEGIN}{'Elasticsearch' if settings.ELASTICSEARCH else 'OpenSearch'}{HIGHLIGHT_END}"
    )
    logger.info(f"URL: {redacted_url}")
    logger.info(f"{HIGHLIGHT_BEGIN}Redis{HIGHLIGHT_END}")

    url: str = get_redis_url()
    redacted_url: str = get_redacted_url(url)
    logger.info(f"URL: {redacted_url}")
    logger.info("-" * 65)

    logger.info(f"{HIGHLIGHT_BEGIN}Replication slots{HIGHLIGHT_END}")
    if (
        config is not None
        and schema_url is not None
        and s3_schema_url is not None
    ):
        for doc in config_loader(
            config=config,
            schema_url=schema_url,
            s3_schema_url=s3_schema_url,
        ):
            index: str = doc.get("index") or doc["database"]
            database: str = doc.get("database", index)
            slot_name: str = re.sub(
                "[^0-9a-zA-Z_]+", "", f"{database.lower()}_{index}"
            )
        logger.info(f"Slot: {slot_name}")

    logger.info("-" * 65)


def validate_config(
    config: t.Optional[str] = None,
    schema_url: t.Optional[str] = None,
    s3_schema_url: t.Optional[str] = None,
) -> str:
    """Ensure there is a valid schema config."""

    if config:
        if not os.path.exists(config):
            raise FileNotFoundError(f'Schema config "{config}" not found')

    if schema_url:
        parsed = urlparse(schema_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f'Invalid URL: "{schema_url}"')

    if s3_schema_url:
        if not s3_schema_url.startswith("s3://"):
            raise ValueError(f'Invalid S3 URL: "{s3_schema_url}"')

    if not config and not schema_url and not s3_schema_url:
        raise ValueError(
            "You must provide either a local config path, a valid URL or an S3 URL"
        )


def config_loader(
    config: t.Optional[str] = None,
    schema_url: t.Optional[str] = None,
    s3_schema_url: t.Optional[str] = None,
) -> t.Generator[dict, None, None]:
    """
    Loads a configuration file from a local path or S3 URL or URL and yields each document.
    """

    def is_s3_url(url: str) -> bool:
        return url.lower().startswith("s3://")

    def download_from_s3(s3_url: str) -> str:
        parsed = urlparse(s3_url)
        if not parsed.netloc or not parsed.path:
            raise ValueError(f"Invalid S3 URL: {s3_url}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3 = boto3.client("s3")
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        s3.download_file(bucket, key, temp_file.name)
        return temp_file.name

    def is_url(url: str) -> bool:
        parsed_url: ParseResult = urlparse(url)
        return parsed_url.scheme in ("http", "https")

    def download_from_url(url: str) -> str:
        """Download JSON from a URL, save to a temp .json file, and return its path."""
        response: requests.Response = requests.get(
            url, headers={"Accept": "application/json"}, timeout=(10, 60)
        )
        response.raise_for_status()
        # Ensure it's valid JSON (raises ValueError if not)
        try:
            data: dict = response.json()
        except ValueError as e:
            content_type = response.headers.get("Content-Type", "unknown")
            raise ValueError(
                f"Expected JSON from {url} (got Content-Type: {content_type})"
            ) from e

        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            return path
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise

    if not config and not schema_url and not s3_schema_url:
        raise ValueError(
            "You must provide either a local config path, a valid URL or an S3 URL"
        )

    config_path: str = None
    is_temp_file: bool = False

    if config:
        if not os.path.exists(config):
            raise FileNotFoundError(f'Local config "{config}" not found')
        config_path = config
    elif schema_url and is_url(schema_url):
        config_path = download_from_url(schema_url)
        is_temp_file = True
    elif s3_schema_url and is_s3_url(s3_schema_url):
        config_path = download_from_s3(s3_schema_url)
        is_temp_file = True
    else:
        raise ValueError(
            "Invalid input: schema must be a file path, a valid S3 URL or a valid URL."
        )

    try:
        with open(config_path, "r") as f:
            data = json.load(f)
            for doc in data:
                for key, value in doc.items():
                    try:
                        doc[key] = Template(value).safe_substitute(os.environ)
                    except TypeError:
                        pass
                yield doc
    finally:
        if is_temp_file and os.path.exists(config_path):
            os.remove(config_path)


def compiled_query(
    query: sa.sql.Select,
    label: t.Optional[str] = None,
    literal_binds: bool = settings.QUERY_LITERAL_BINDS,
) -> None:
    """Compile an SQLAlchemy query with an optional label."""
    query = str(
        query.compile(
            dialect=sa.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": literal_binds},
        )
    )
    query = sqlparse.format(query, reindent=True, keyword_case="upper")
    if label:
        logger.debug(f"\033[4m{label}:\033[0m\n{query}")
        sys.stdout.write(f"\033[4m{label}:\033[0m\n{query}\n")
    else:
        logger.debug(f"{query}")
        sys.stdout.write(f"{query}\n")
    sys.stdout.write("-" * 79)
    sys.stdout.write("\n")


class MutuallyExclusiveOption(click.Option):
    """
    A custom Click option that allows for mutually exclusive arguments.

    Args:
        click.Option: The base class for Click options.

    Attributes:
        mutually_exclusive (set): A set of argument names that are mutually exclusive with this option.
    """

    def __init__(self, *args, **kwargs):
        self.mutually_exclusive: t.Set = set(
            kwargs.pop("mutually_exclusive", [])
        )
        help: str = kwargs.get("help", "")
        if self.mutually_exclusive:
            kwargs["help"] = help + (
                f" NOTE: This argument is mutually exclusive with "
                f" arguments: [{', '.join(self.mutually_exclusive)}]."
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(
        self,
        ctx: click.Context,
        opts: t.Mapping[str, t.Any],
        args: t.List[str],
    ) -> t.Tuple[t.Any, t.List[str]]:
        """
        Handles the parsing of the command-line arguments.

        Args:
            ctx (click.Context): The Click context.
            opts (dict): The dictionary of parsed options.
            args (list): The list of parsed arguments.

        Returns:
            The result of the base class's `handle_parse_result` method.
        """
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise click.UsageError(
                f"Illegal usage: `{self.name}` is mutually exclusive with "
                f"arguments `{', '.join(self.mutually_exclusive)}`."
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx, opts, args
        )


def qname(engine_or_conn, schema: str = None, table: str = None) -> str:
    """
    Return a dialect-correct, quoted table name.

    Examples:
    Postgres:  qname(engine, "public", "users")  ->  "public"."users"
    MySQL:     qname(engine, "mydb",  "users")   ->  `mydb`.`users`
                (or just `users` if schema is None)
    """
    dialect = getattr(engine_or_conn, "dialect", engine_or_conn.engine.dialect)
    quote = dialect.identifier_preparer.quote_identifier

    if schema and schema.strip():
        return f"{quote(schema)}.{quote(table)}"
    return quote(table)


# mysql related helper methods
def _cols(engine: sa.Engine, schema: str, table: str) -> list[str]:
    key = (schema, table)
    if key in _col_cache:
        return _col_cache[key]
    insp = sa.inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
    _col_cache[key] = cols
    return cols


def remap_unknown(
    engine: sa.Engine, schema: str, table: str, values: dict
) -> dict:
    if not values:
        return values
    # only remap if *all* keys are UNKNOWN_COL*
    if not all(
        isinstance(k, str) and _UNKNOWN_RE.match(k) for k in values.keys()
    ):
        return values
    cols = _cols(engine, schema, table)
    remapped: dict = {}
    # keys may be 0-based (UNKNOWN_COL0), so use the numeric suffix
    for k, v in values.items():
        idx = int(_UNKNOWN_RE.match(k).group(1))  # type: ignore
        if idx < len(cols):
            remapped[cols[idx]] = v
        else:
            remapped[f"@{idx+1}"] = v  # fallback
    return remapped
