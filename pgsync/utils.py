"""PGSync utils."""
import logging
import os
import sys
import threading
from datetime import timedelta
from time import time
from typing import Optional

from .exc import SchemaError
from .settings import (
    CHECKPOINT_PATH,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_USER,
    PG_HOST,
    PG_PORT,
    PG_USER,
    REDIS_AUTH,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_SCHEME,
    SCHEMA,
)

logger = logging.getLogger(__name__)


def timeit(func):
    def timed(*args, **kwargs):
        since: float = time()
        retval = func(*args, **kwargs)
        until: float = time()
        sys.stdout.write(
            f"{func.__name__} ({args}, {kwargs}) {until-since} secs\n"
        )
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


def exit_handler(func):
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


def show_settings(schema: Optional[str] = None, **kwargs) -> None:
    """Show settings."""
    logger.info("\033[4mSettings\033[0m:")
    logger.info(f'{"Schema":<10s}: {schema or SCHEMA}')
    logger.info("-" * 65)
    logger.info("\033[4mCheckpoint\033[0m:")
    logger.info(f"Path: {CHECKPOINT_PATH}")
    logger.info("\033[4mPostgres\033[0m:")
    logger.info(
        f'URL: postgresql://{kwargs.get("user", PG_USER)}:*****@'
        f'{kwargs.get("host", PG_HOST)}:'
        f'{kwargs.get("port", PG_PORT)}'
    )
    for key in kwargs:
        if key == "password":
            continue
        logger.info(f"{key}: {kwargs[key]}")
    logger.info("\033[4mElasticsearch\033[0m:")
    if ELASTICSEARCH_USER:
        logger.info(
            f"URL: {ELASTICSEARCH_SCHEME}://{ELASTICSEARCH_USER}:*****@"
            f"{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"
        )
    else:
        logger.info(
            f"URL: {ELASTICSEARCH_SCHEME}://"
            f"{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"
        )
    logger.info("\033[4mRedis\033[0m:")
    if REDIS_AUTH:
        logger.info(
            f"URL: {REDIS_SCHEME}://:****@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        )
    else:
        logger.info(
            f"URL: {REDIS_SCHEME}://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        )
    logger.info("-" * 65)


def get_config(config: Optional[str] = None) -> str:
    """
    Return the schema config for PGSync.
    """
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
