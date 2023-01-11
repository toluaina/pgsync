"""PGSync urls."""

import logging
from typing import Optional
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
)

logger = logging.getLogger(__name__)


def _get_auth(key: str) -> Optional[str]:
    try:
        plugins: Plugins = Plugins("plugins", ["Auth"])
        return plugins.auth(key)
    except ModuleNotFoundError:
        return None


def get_search_url(
    scheme: Optional[str] = None,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> str:
    """Return the URL to connect to Elasticsearch/OpenSearch."""
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
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    driver: Optional[str] = None,
) -> str:
    """Return the URL to connect to Postgres."""
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
    scheme: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
) -> str:
    """Return the URL to connect to Redis."""
    host = host or REDIS_HOST
    password = _get_auth("REDIS_AUTH") or password or REDIS_AUTH
    port = port or REDIS_PORT
    db = db or REDIS_DB
    scheme = scheme or REDIS_SCHEME
    if password:
        return f"{scheme}://:{quote_plus(password)}@{host}:{port}/{db}"
    logger.debug("Connecting to Redis without password.")
    return f"{scheme}://{host}:{port}/{db}"
