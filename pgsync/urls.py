"""PGSync urls."""

import logging
import typing as t
from urllib.parse import quote_plus

from .plugin import Plugins
from .settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_USER,
    PG_DRIVER,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    REDIS_AUTH,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_SCHEME,
    REDIS_USER,
)

logger = logging.getLogger(__name__)


def _get_auth(key: str) -> t.Optional[str]:
    """
    Get authentication key from plugins.

    Args:
        key (str): The authentication key.

    Returns:
        Optional[str]: The authentication key if found, otherwise None.
    """
    try:
        plugins: Plugins = Plugins("plugins", ["Auth"])
        return plugins.auth(key)
    except ModuleNotFoundError:
        return None


def get_search_url(
    scheme: t.Optional[str] = None,
    user: t.Optional[str] = None,
    host: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
) -> str:
    """
    Return the URL to connect to Elasticsearch/OpenSearch.

    Args:
        scheme (Optional[str]): The scheme to use for the connection. Defaults to None.
        user (Optional[str]): The username to use for the connection. Defaults to None.
        host (Optional[str]): The host to connect to. Defaults to None.
        password (Optional[str]): The password to use for the connection. Defaults to None.
        port (Optional[int]): The port to use for the connection. Defaults to None.

    Returns:
        str: The URL to connect to Elasticsearch/OpenSearch.
    """
    scheme = scheme or ELASTICSEARCH_SCHEME
    host = host or ELASTICSEARCH_HOST
    port = port or ELASTICSEARCH_PORT
    user = user or ELASTICSEARCH_USER
    password = (
        _get_auth("ELASTICSEARCH_PASSWORD")
        or password
        or ELASTICSEARCH_PASSWORD
    )
    if user and password:
        return f"{scheme}://{user}:{quote_plus(password)}@{host}:{port}"
    logger.debug("Connecting to Search without password.")
    return f"{scheme}://{host}:{port}"


def get_postgres_url(
    database: str,
    user: t.Optional[str] = None,
    host: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    driver: t.Optional[str] = None,
) -> str:
    """
    Return the URL to connect to Postgres.

    Args:
        database (str): The name of the database to connect to.
        user (str, optional): The username to use for authentication. Defaults to None.
        host (str, optional): The hostname of the database server. Defaults to None.
        password (str, optional): The password to use for authentication. Defaults to None.
        port (int, optional): The port number to use for the database connection. Defaults to None.
        driver (str, optional): The name of the driver to use for the connection. Defaults to None.

    Returns:
        str: The URL to connect to the Postgres database.
    """
    user = user or PG_USER
    host = host or PG_HOST
    password = _get_auth("PG_PASSWORD") or password or PG_PASSWORD
    port = port or PG_PORT
    driver = driver or PG_DRIVER
    if password:
        return (
            f"postgresql+{driver}://{user}:{quote_plus(password)}@"
            f"{host}:{port}/{database}"
        )
    logger.debug("Connecting to Postgres without password.")
    return f"postgresql+{driver}://{user}@{host}:{port}/{database}"


def get_redis_url(
    scheme: t.Optional[str] = None,
    host: t.Optional[str] = None,
    username: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    db: t.Optional[str] = None,
) -> str:
    """
    Return the URL to connect to Redis.

    Args:
        scheme (Optional[str]): The scheme to use for the Redis connection. Defaults to None.
        host (Optional[str]): The Redis host to connect to. Defaults to None.
        username (Optional[str]): The Redis username to use for authentication. Defaults to None.
        password (Optional[str]): The Redis password to use for authentication. Defaults to None.
        port (Optional[int]): The Redis port to connect to. Defaults to None.
        db (Optional[str]): The Redis database to connect to. Defaults to None.

    Returns:
        str: The Redis connection URL.
    """
    host = host or REDIS_HOST
    username = username or REDIS_USER
    password = _get_auth("REDIS_AUTH") or password or REDIS_AUTH
    port = port or REDIS_PORT
    db = db or REDIS_DB
    scheme = scheme or REDIS_SCHEME
    if username and password:
        logger.debug("Connecting to Redis with custom username and password.")
        return f"{scheme}://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{db}"
    if password:
        logger.debug("Connecting to Redis with default password.")
        return f"{scheme}://:{quote_plus(password)}@{host}:{port}/{db}"
    logger.debug("Connecting to Redis without password.")
    return f"{scheme}://{host}:{port}/{db}"
