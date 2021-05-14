"""PGSync utils."""
import logging
import os
import sys
import threading
import time
from datetime import timedelta
from urllib.parse import quote_plus

from .exc import SchemaError
from .settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_USER,
    PG_HOST,
    PG_PASSWORD,
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
        since = time.time()
        retval = func(*args, **kwargs)
        until = time.time()
        sys.stdout.write(
            f"{func.__name__} ({args}, {kwargs}) {until-since} secs\n"
        )
        return retval

    return timed


class Timer:
    def __init__(self, message=None):
        self._message = message or ""

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.elapsed = self.end - self.start
        sys.stdout.write(
            f"{self._message} {str(timedelta(seconds=self.elapsed))} "
            f"({self.elapsed:2.2f} sec)\n"
        )


def show_settings(schema=None, params={}):
    """Show configuration."""
    logger.info("\033[4mSettings\033[0m:")
    logger.info(f'{"Schema":<10s}: {schema or SCHEMA}')
    logger.info("-" * 65)
    logger.info("\033[4mPostgres\033[0m:")
    logger.info(
        f'URL: postgresql://{params.get("user", PG_USER)}:*****@'
        f'{params.get("host", PG_HOST)}:'
        f'{params.get("port", PG_PORT)}'
    )
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
    logger.info(f"URL: {REDIS_SCHEME}://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    logger.info("-" * 65)


def threaded(fn):
    """Decorator for threaded code execution."""

    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread

    return wrapper


def get_elasticsearch_url(
    scheme=None,
    user=None,
    host=None,
    password=None,
    port=None,
):
    """
    Return the URL to connect to Elasticsearch.
    """
    scheme = scheme or ELASTICSEARCH_SCHEME
    host = host or ELASTICSEARCH_HOST
    port = port or ELASTICSEARCH_PORT
    user = user or ELASTICSEARCH_USER
    password = password or ELASTICSEARCH_PASSWORD
    if user:
        return f"{scheme}://{user}:{quote_plus(password)}@{host}:{port}"
    logger.debug("Connecting to Elasticsearch without authentication.")
    return f"{scheme}://{host}:{port}"


def get_postgres_url(
    database,
    user=None,
    host=None,
    password=None,
    port=None,
):
    """
    Return the URL to connect to Postgres.
    """
    user = user or PG_USER
    host = host or PG_HOST
    password = password or PG_PASSWORD
    port = port or PG_PORT
    if not password:
        logger.debug("Connecting to Postgres without password.")
        return f"postgresql://{user}@{host}:{port}/{database}"
    return (
        f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    )


def get_redis_url(scheme=None, host=None, password=None, port=None, db=None):
    """
    Return the URL to connect to Redis.
    """
    host = host or REDIS_HOST
    password = password or REDIS_AUTH
    port = port or REDIS_PORT
    db = db or REDIS_DB
    scheme = scheme or REDIS_SCHEME
    if password:
        return f"{scheme}://:{quote_plus(password)}@{host}:{port}/{db}"
    logger.debug("Connecting to Redis without password.")
    return f"{scheme}://{host}:{port}/{db}"


def get_config(config=None):
    """
    Return the schema config for PGSync.
    """
    config = config or SCHEMA
    if not config:
        raise SchemaError(
            "Schema config not set\n. "
            "Set env SCHEMA=/path/to/schema.json or "
            "provide args --config /path/to/schema.json"
        )
    if not os.path.exists(config):
        raise IOError(f'Schema config "{config}" not found')
    return config
