"""PGSync utils."""

import json
import logging
import os
import sys
import threading
import typing as t
from datetime import timedelta
from string import Template
from time import time
from urllib.parse import ParseResult, urlparse

import click
import sqlalchemy as sa
import sqlparse

from . import settings
from .exc import SchemaError
from .urls import get_postgres_url, get_redis_url, get_search_url

logger = logging.getLogger(__name__)

HIGHLIGHT_BEGIN = "\033[4m"
HIGHLIGHT_END = "\033[0m:"


def chunks(value: list, size: int) -> list:
    """Yield successive n-sized chunks from l"""
    for i in range(0, len(value), size):
        yield value[i : i + size]


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


def get_redacted_url(result: ParseResult) -> ParseResult:
    """
    Returns a redacted version of the input URL, with the password replaced by asterisks.

    Args:
        result (ParseResult): The parsed URL to redact.

    Returns:
        ParseResult: The redacted URL.
    """
    if result.password:
        username: t.Optional[str] = result.username
        hostname: t.Optional[str] = result.hostname
        if username and hostname:
            result = result._replace(
                netloc=f"{username}:{'*' * len(result.password)}@{hostname}"
            )
    return result


def show_settings(schema: t.Optional[str] = None) -> None:
    """Show settings."""
    logger.info(f"{HIGHLIGHT_BEGIN}Settings{HIGHLIGHT_END}")
    logger.info(f'{"Schema":<10s}: {schema or settings.SCHEMA}')
    logger.info("-" * 65)
    logger.info(f"{HIGHLIGHT_BEGIN}Checkpoint{HIGHLIGHT_END}")
    logger.info(f"Path: {settings.CHECKPOINT_PATH}")
    logger.info(f"{HIGHLIGHT_BEGIN}Postgres{HIGHLIGHT_END}")
    result: ParseResult = get_redacted_url(
        urlparse(get_postgres_url("postgres"))
    )
    logger.info(f"URL: {result.geturl()}")
    result = get_redacted_url(urlparse(get_search_url()))
    if settings.ELASTICSEARCH:
        logger.info(f"{HIGHLIGHT_BEGIN}Elasticsearch{HIGHLIGHT_END}")
    else:
        logger.info(f"{HIGHLIGHT_BEGIN}OpenSearch{HIGHLIGHT_END}")
    logger.info(f"URL: {result.geturl()}")
    logger.info(f"{HIGHLIGHT_BEGIN}Redis{HIGHLIGHT_END}")
    result = get_redacted_url(urlparse(get_redis_url()))
    logger.info(f"URL: {result.geturl()}")
    logger.info("-" * 65)


def get_config(config: t.Optional[str] = None) -> str:
    """Return the schema config for PGSync."""
    config = config or settings.SCHEMA
    if not config:
        raise SchemaError(
            "Schema config not set\n. "
            "Set env SCHEMA=/path/to/schema.json or "
            "provide args --config /path/to/schema.json"
        )
    if not os.path.exists(config):
        raise FileNotFoundError(f'Schema config "{config}" not found')
    return config


def config_loader(config: str) -> t.Generator:
    """
    Loads a configuration file and yields each document in the file as a dictionary.
    The values in the dictionary are processed as templates, with environment variables
    substituted for placeholders. If a value cannot be processed as a template, it is
    left unchanged.

    Args:
        config (str): The path to the configuration file.

    Yields:
        dict: A dictionary representing a document in the configuration file.
    """
    with open(config, "r") as docs:
        for doc in json.load(docs):
            for key, value in doc.items():
                try:
                    doc[key] = Template(value).safe_substitute(os.environ)
                except TypeError:
                    pass
            yield doc


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
