"""PGSync urls."""

import logging
import typing as t
from urllib.parse import ParseResult, quote, quote_plus, urlparse, urlunparse

from .plugin import Plugins
from .settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_USER,
    PG_DRIVER,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_URL,
    PG_USER,
    REDIS_AUTH,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_SCHEME,
    REDIS_URL,
    REDIS_USER,
    USE_UTF8MB4,
)

logger = logging.getLogger(__name__)

DIALECT = {
    "psycopg2": "postgresql",
    "pymysql": "mysql",
}


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
    # override the default URL if ELASTICSEARCH_URL is set
    if ELASTICSEARCH_URL:
        return ELASTICSEARCH_URL.strip()

    auth: str = ""
    if user and password:
        auth = f"{user}:{quote_plus(password)}@"
    else:
        logger.debug("Connecting to Search without password.")

    return f"{scheme}://{auth}{host}:{port}"


def get_database_url(
    database: str,
    user: t.Optional[str] = None,
    host: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    driver: t.Optional[str] = None,
) -> str:
    """
    Return the URL to connect to the database.

    Args:
        database (str): The name of the database to connect to.
        user (str, optional): The username to use for authentication. Defaults to None.
        host (str, optional): The hostname of the database server. Defaults to None.
        password (str, optional): The password to use for authentication. Defaults to None.
        port (int, optional): The port number to use for the database connection. Defaults to None.
        driver (str, optional): The name of the driver to use for the connection. Defaults to None.

    Returns:
        str: The URL to connect to the database.
    """
    user = user or PG_USER
    host = host or PG_HOST
    password = _get_auth("PG_PASSWORD") or password or PG_PASSWORD
    port = port or PG_PORT
    driver = driver or PG_DRIVER
    # override the default URL if PG_URL is set
    if PG_URL:
        parsed_url: ParseResult = urlparse(PG_URL.strip())
        # keep existing scheme/netloc/query/fragment; swap just the path
        new_path: str = "/" + quote(database)
        return urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                new_path,
                parsed_url.params,
                parsed_url.query,
                parsed_url.fragment,
            )
        )

    auth: str = f"{user}:{quote_plus(password)}" if password else user
    if not password:
        logger.debug("Connecting to database without password.")

    protocol: str = DIALECT.get(PG_DRIVER)
    if not protocol:
        raise ValueError(
            f"Unsupported PG_DRIVER={PG_DRIVER!r}; expected 'psycopg2' or 'pymysql'."
        )

    charset_qs: str = (
        "?charset=utf8mb4" if (protocol == "mysql" and USE_UTF8MB4) else ""
    )

    return f"{protocol}+{driver}://{auth}@{host}:{port}/{database}{charset_qs}"


def get_redis_url(
    scheme: t.Optional[str] = None,
    host: t.Optional[str] = None,
    username: t.Optional[str] = None,
    password: t.Optional[str] = None,
    port: t.Optional[int] = None,
    db: t.Optional[str] = None,
) -> str:
    """
    Return the URL to connect to Redis/Valkey.

    Args:
        scheme (Optional[str]): The scheme to use for the Redis/Valkey connection. Defaults to None.
        host (Optional[str]): The Redis/Valkey host to connect to. Defaults to None.
        username (Optional[str]): The Redis/Valkey username to use for authentication. Defaults to None.
        password (Optional[str]): The Redis/Valkey password to use for authentication. Defaults to None.
        port (Optional[int]): The Redis/Valkey port to connect to. Defaults to None.
        db (Optional[str]): The Redis/Valkey database to connect to. Defaults to None.

    Returns:
        str: The Redis/Valkey connection URL.
    """
    host = host or REDIS_HOST
    username = username or REDIS_USER
    password = _get_auth("REDIS_AUTH") or password or REDIS_AUTH
    port = port or REDIS_PORT
    db = db or REDIS_DB
    scheme = scheme or REDIS_SCHEME
    # override the default URL if REDIS_URL is set
    if REDIS_URL:
        return REDIS_URL.strip()

    auth: str = ""
    if username and password:
        auth = f"{quote_plus(username)}:{quote_plus(password)}@"
        logger.debug("Connecting to Redis with custom username and password.")
    elif password:
        auth = f":{quote_plus(password)}@"
        logger.debug("Connecting to Redis with default password.")
    else:
        logger.debug("Connecting to Redis without password.")

    return f"{scheme}://{auth}{host}:{port}/{db}"
