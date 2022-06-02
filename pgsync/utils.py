"""PGSync utils."""
import logging
import os
import sys
import threading
from datetime import timedelta
from time import time
from typing import Optional
from urllib.parse import ParseResult, urlparse

from .exc import SchemaError
from .settings import CHECKPOINT_PATH, SCHEMA
from .urls import get_elasticsearch_url, get_postgres_url, get_redis_url

logger = logging.getLogger(__name__)

HIGHLIGHT_START = "\033[4m"
HIGHLIGHT_END = "\033[0m:"


def timeit(func):
    def timed(*args, **kwargs):
        since: float = time()
        retval = func(*args, **kwargs)
        until: float = time()
        sys.stdout.write(f"{func.__name__}: {until-since} secs\n")
        return retval

    return timed


class Timer:
    def __init__(self, message: Optional[str] = None):
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


def threaded(func):
    """Decorator for threaded code execution."""

    def wrapper(*args, **kwargs) -> threading.Thread:
        thread: threading.Thread = threading.Thread(
            target=func, args=args, kwargs=kwargs
        )
        thread.start()
        return thread

    return wrapper


def exception(func):
    """Decorator for threaded exception handling."""

    def wrapper(*args, **kwargs):
        try:
            fn = func(*args, **kwargs)
        except Exception as e:
            name: str = threading.currentThread().getName()
            sys.stdout.write(
                f"Exception in {func.__name__}() for thread {name}: {e}\n"
                f"Exiting...\n"
            )
            os._exit(-1)
        return fn

    return wrapper


def get_redacted_url(result: ParseResult) -> ParseResult:
    if result.password:
        username: str = result.username
        hostname: str = result.hostname
        result = result._replace(
            netloc=f"{username}:{'*' * len(result.password)}@{hostname}"
        )
    return result


def show_settings(schema: Optional[str] = None) -> None:
    """Show settings."""
    logger.info(f"{HIGHLIGHT_START}Settings{HIGHLIGHT_END}")
    logger.info(f'{"Schema":<10s}: {schema or SCHEMA}')
    logger.info("-" * 65)
    logger.info(f"{HIGHLIGHT_START}Checkpoint{HIGHLIGHT_END}")
    logger.info(f"Path: {CHECKPOINT_PATH}")
    logger.info(f"{HIGHLIGHT_START}Postgres{HIGHLIGHT_END}")
    result: ParseResult = get_redacted_url(
        urlparse(get_postgres_url("postgres"))
    )
    logger.info(f"URL: {result.geturl()}")
    result: ParseResult = get_redacted_url(urlparse(get_elasticsearch_url()))
    logger.info(f"{HIGHLIGHT_START}Elasticsearch{HIGHLIGHT_END}")
    logger.info(f"URL: {result.geturl()}")
    logger.info(f"{HIGHLIGHT_START}Redis{HIGHLIGHT_END}")
    result: ParseResult = get_redacted_url(urlparse(get_redis_url()))
    logger.info(f"URL: {result.geturl()}")
    logger.info("-" * 65)


def get_config(config: Optional[str] = None) -> str:
    """Return the schema config for PGSync."""
    config: str = config or SCHEMA
    if not config:
        raise SchemaError(
            "Schema config not set\n. "
            "Set env SCHEMA=/path/to/schema.json or "
            "provide args --config /path/to/schema.json"
        )
    if not os.path.exists(config):
        raise IOError(f'Schema config "{config}" not found')
    return config
