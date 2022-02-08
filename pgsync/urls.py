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


def get_elasticsearch_url(
    scheme: Optional[str] = None,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> str:
    """Return the URL to connect to Elasticsearch."""
    scheme: str = scheme or ELASTICSEARCH_SCHEME
    host: str = host or ELASTICSEARCH_HOST
    port: str = port or ELASTICSEARCH_PORT
    user: str = user or ELASTICSEARCH_USER
    password: str = (
        _get_auth("ELASTICSEARCH_PASSWORD")
        or password
        or ELASTICSEARCH_PASSWORD
    )
    if user:
        return f"{scheme}://{user}:{quote_plus(password)}@{host}:{port}"
    logger.debug("Connecting to Elasticsearch without authentication.")
    return f"{scheme}://{host}:{port}"


def get_postgres_url(
    database: str,
    user: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> str:
    """Return the URL to connect to Postgres."""
    user: str = user or PG_USER
    host: str = host or PG_HOST
    password: str = _get_auth("PG_PASSWORD") or password or PG_PASSWORD
    port: str = port or PG_PORT
    if not password:
        logger.debug("Connecting to Postgres without password.")
        return f"postgresql://{user}@{host}:{port}/{database}"
    return (
        f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    )


def get_redis_url(
    scheme: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
) -> str:
    """Return the URL to connect to Redis."""
    host: str = host or REDIS_HOST
    password: str = _get_auth("REDIS_AUTH") or password or REDIS_AUTH
    port: str = port or REDIS_PORT
    db: str = db or REDIS_DB
    scheme: str = scheme or REDIS_SCHEME
    if password:
        return f"{scheme}://:{quote_plus(password)}@{host}:{port}/{db}"
    logger.debug("Connecting to Redis without password.")
    return f"{scheme}://{host}:{port}/{db}"
