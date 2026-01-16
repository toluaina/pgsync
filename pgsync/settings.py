"""PGSync settings.

Configuration management via environment variables loaded from a .env file.
Each setting has a default value that can be overridden.

Settings are organized into the following categories:
- PGSync: sync behavior (block size, polling, workers, query chunking)
- SQLAlchemy: connection pooling configuration
- Elasticsearch/OpenSearch: client and bulk indexing options
- PostgreSQL/MySQL/MariaDB: database connection parameters
- Redis/Valkey: queue and checkpoint storage
- Logging: log levels and handlers
"""

import logging
import logging.config
import os
import typing as t

from environs import Env

logger = logging.getLogger(__name__)

env = Env()
env.read_env(path=os.path.join(os.getcwd(), ".env"))

# =============================================================================
# PGSync
# =============================================================================

# Page block size for parallel sync
BLOCK_SIZE = env.int("BLOCK_SIZE", default=2048 * 10)
# Directory for checkpoint files
CHECKPOINT_PATH = env.str("CHECKPOINT_PATH", default="./")
# Store checkpoints in Redis instead of files
REDIS_CHECKPOINT = env.bool("REDIS_CHECKPOINT", default=False)
# Use JOIN queries instead of subqueries
JOIN_QUERIES = env.bool("JOIN_QUERIES", default=False)
# Batch size for logical slot changes (minimizes temp file disk usage)
LOGICAL_SLOT_CHUNK_SIZE = env.int("LOGICAL_SLOT_CHUNK_SIZE", default=5000)
# Stdout log interval in seconds
LOG_INTERVAL = env.float("LOG_INTERVAL", default=0.5)
# Number of workers for handling events
NUM_WORKERS = env.int("NUM_WORKERS", default=2)
# Database driver: psycopg2, psycopg3, pymysql, etc.
PG_DRIVER = env.str("PG_DRIVER", default="psycopg2")
# Poll timeout in seconds (reduce to increase throughput)
POLL_TIMEOUT = env.float("POLL_TIMEOUT", default=0.1)
# Render SQL with literal binds for debugging
QUERY_LITERAL_BINDS = env.bool("QUERY_LITERAL_BINDS", default=False)
# Records to fetch per database query
QUERY_CHUNK_SIZE = env.int("QUERY_CHUNK_SIZE", default=10000)
# Records per filter chunk
FILTER_CHUNK_SIZE = env.int("FILTER_CHUNK_SIZE", default=5000)
# Replication slot cleanup interval in seconds
REPLICATION_SLOT_CLEANUP_INTERVAL = env.float(
    "REPLICATION_SLOT_CLEANUP_INTERVAL",
    default=180.0,
)
# Path to schema config file
SCHEMA = env.str("SCHEMA", default=None)
# S3 URL for schema config
S3_SCHEMA_URL = env.str("S3_SCHEMA_URL", default=None)
# HTTP URL for schema config
SCHEMA_URL = env.str("SCHEMA_URL", default=None)
# Use async database operations
USE_ASYNC = env.bool("USE_ASYNC", default=False)
# Stream query results instead of fetching all at once
STREAM_RESULTS = env.bool("STREAM_RESULTS", default=True)
# Polling interval in seconds
POLL_INTERVAL = env.float("POLL_INTERVAL", default=0.1)
# Format numbers with commas in logs
FORMAT_WITH_COMMAS = env.bool("FORMAT_WITH_COMMAS", default=True)
# Use polling mode instead of triggers
POLLING = env.bool("POLLING", default=False)
# Use WAL streaming mode
WAL = env.bool("WAL", default=False)

# =============================================================================
# SQLAlchemy
# =============================================================================

# Use NullPool (no pooling) - useful for testing
SQLALCHEMY_USE_NULLPOOL = env.bool("SQLALCHEMY_USE_NULLPOOL", default=False)
# Persistent connections in pool (20 for better concurrency)
SQLALCHEMY_POOL_SIZE = env.int("SQLALCHEMY_POOL_SIZE", default=20)
# Extra connections allowed beyond pool_size (40 for high load)
SQLALCHEMY_MAX_OVERFLOW = env.int("SQLALCHEMY_MAX_OVERFLOW", default=40)
# Ping connections before checkout to verify they're alive (enabled for production stability)
SQLALCHEMY_POOL_PRE_PING = env.bool("SQLALCHEMY_POOL_PRE_PING", default=True)
# Recycle connections after N seconds (-1 = never)
SQLALCHEMY_POOL_RECYCLE = env.int("SQLALCHEMY_POOL_RECYCLE", default=-1)
# Seconds to wait for available connection before timeout
SQLALCHEMY_POOL_TIMEOUT = env.int("SQLALCHEMY_POOL_TIMEOUT", default=30)

# =============================================================================
# Elasticsearch/OpenSearch
# =============================================================================

# Authentication
ELASTICSEARCH_API_KEY = env.str("ELASTICSEARCH_API_KEY", default=None)
ELASTICSEARCH_API_KEY_ID = env.str("ELASTICSEARCH_API_KEY_ID", default=None)
ELASTICSEARCH_BASIC_AUTH = env.str("ELASTICSEARCH_BASIC_AUTH", default=None)
ELASTICSEARCH_BEARER_AUTH = env.str("ELASTICSEARCH_BEARER_AUTH", default=None)
ELASTICSEARCH_HTTP_AUTH = env.list("ELASTICSEARCH_HTTP_AUTH", default=None)
if ELASTICSEARCH_HTTP_AUTH:
    ELASTICSEARCH_HTTP_AUTH = tuple(ELASTICSEARCH_HTTP_AUTH)
ELASTICSEARCH_USER = env.str("ELASTICSEARCH_USER", default=None)
ELASTICSEARCH_PASSWORD = env.str("ELASTICSEARCH_PASSWORD", default=None)

# Connection
ELASTICSEARCH_HOST = env.str("ELASTICSEARCH_HOST", default="localhost")
ELASTICSEARCH_PORT = env.int("ELASTICSEARCH_PORT", default=9200)
ELASTICSEARCH_SCHEME = env.str("ELASTICSEARCH_SCHEME", default="http")
ELASTICSEARCH_CLOUD_ID = env.str("ELASTICSEARCH_CLOUD_ID", default=None)
ELASTICSEARCH_OPAQUE_ID = env.str("ELASTICSEARCH_OPAQUE_ID", default=None)
# Full URL (overrides host/port/scheme)
ELASTICSEARCH_URL = env.str("ELASTICSEARCH_URL", default=None)

# AWS
ELASTICSEARCH_AWS_HOSTED = env.bool("ELASTICSEARCH_AWS_HOSTED", default=False)
ELASTICSEARCH_AWS_REGION = env.str("ELASTICSEARCH_AWS_REGION", default=None)

# SSL/TLS
ELASTICSEARCH_USE_SSL = env.bool("ELASTICSEARCH_USE_SSL", default=False)
ELASTICSEARCH_VERIFY_CERTS = env.bool(
    "ELASTICSEARCH_VERIFY_CERTS", default=True
)
ELASTICSEARCH_CA_CERTS = env.str("ELASTICSEARCH_CA_CERTS", default=None)
ELASTICSEARCH_CLIENT_CERT = env.str("ELASTICSEARCH_CLIENT_CERT", default=None)
ELASTICSEARCH_CLIENT_KEY = env.str("ELASTICSEARCH_CLIENT_KEY", default=None)
ELASTICSEARCH_SSL_CONTEXT = env.str("ELASTICSEARCH_SSL_CONTEXT", default=None)
ELASTICSEARCH_SSL_VERSION = env.int("ELASTICSEARCH_SSL_VERSION", default=None)
ELASTICSEARCH_SSL_ASSERT_FINGERPRINT = env.str(
    "ELASTICSEARCH_SSL_ASSERT_FINGERPRINT", default=None
)
ELASTICSEARCH_SSL_ASSERT_HOSTNAME = env.str(
    "ELASTICSEARCH_SSL_ASSERT_HOSTNAME", default=None
)
ELASTICSEARCH_SSL_SHOW_WARN = env.bool(
    "ELASTICSEARCH_SSL_SHOW_WARN", default=False
)

# Bulk indexing
ELASTICSEARCH_CHUNK_SIZE = env.int("ELASTICSEARCH_CHUNK_SIZE", default=5000)
ELASTICSEARCH_MAX_CHUNK_BYTES = env.int(
    "ELASTICSEARCH_MAX_CHUNK_BYTES", default=104857600  # 100MB
)
ELASTICSEARCH_STREAMING_BULK = env.bool(
    "ELASTICSEARCH_STREAMING_BULK", default=False
)
ELASTICSEARCH_HTTP_COMPRESS = env.bool(
    "ELASTICSEARCH_HTTP_COMPRESS", default=True
)

# Retry behavior
ELASTICSEARCH_MAX_RETRIES = env.int("ELASTICSEARCH_MAX_RETRIES", default=0)
ELASTICSEARCH_INITIAL_BACKOFF = env.float(
    "ELASTICSEARCH_INITIAL_BACKOFF", default=2
)
ELASTICSEARCH_MAX_BACKOFF = env.float("ELASTICSEARCH_MAX_BACKOFF", default=600)

# Performance
ELASTICSEARCH_TIMEOUT = env.float("ELASTICSEARCH_TIMEOUT", default=10)
ELASTICSEARCH_THREAD_COUNT = env.int("ELASTICSEARCH_THREAD_COUNT", default=4)
ELASTICSEARCH_QUEUE_SIZE = env.int("ELASTICSEARCH_QUEUE_SIZE", default=4)
ELASTICSEARCH_POOL_MAXSIZE = env.int("ELASTICSEARCH_POOL_MAXSIZE", default=10)

# Error handling
ELASTICSEARCH_RAISE_ON_ERROR = env.bool(
    "ELASTICSEARCH_RAISE_ON_ERROR", default=True
)
ELASTICSEARCH_RAISE_ON_EXCEPTION = env.bool(
    "ELASTICSEARCH_RAISE_ON_EXCEPTION", default=True
)
ELASTICSEARCH_IGNORE_STATUS = env.list(
    "ELASTICSEARCH_IGNORE_STATUS", default=[404]
)
ELASTICSEARCH_IGNORE_STATUS = tuple(map(int, ELASTICSEARCH_IGNORE_STATUS))

# Backend selection (mutually exclusive)
ELASTICSEARCH = env.bool("ELASTICSEARCH", default=None)
OPENSEARCH = env.bool("OPENSEARCH", default=None)

if ELASTICSEARCH is None and OPENSEARCH is None:
    ELASTICSEARCH, OPENSEARCH = True, False
elif ELASTICSEARCH is None:
    ELASTICSEARCH = not OPENSEARCH
elif OPENSEARCH is None:
    OPENSEARCH = not ELASTICSEARCH

if ELASTICSEARCH and OPENSEARCH:
    raise ValueError("Cannot enable both ELASTICSEARCH and OPENSEARCH")
if not ELASTICSEARCH and not OPENSEARCH:
    raise ValueError("Enable one search backend: ELASTICSEARCH or OPENSEARCH")

ELASTICSEARCH = bool(ELASTICSEARCH)
OPENSEARCH = bool(OPENSEARCH)

# OpenSearch-specific
OPENSEARCH_AWS_HOSTED = env.bool("OPENSEARCH_AWS_HOSTED", default=False)
OPENSEARCH_AWS_SERVERLESS = env.bool(
    "OPENSEARCH_AWS_SERVERLESS", default=False
)

# =============================================================================
# PostgreSQL/MySQL/MariaDB
# =============================================================================

# Primary connection (full URL or individual components)
PG_URL = env.str("PG_URL", default=None)
PG_HOST = env.str("PG_HOST", default="localhost")
PG_PORT = env.int("PG_PORT", default=5432)
PG_USER = env.str("PG_USER", default=None) if PG_URL else env.str("PG_USER")
PG_PASSWORD = env.str("PG_PASSWORD", default=None)
PG_SSLMODE = env.str("PG_SSLMODE", default=None)
PG_SSLROOTCERT = env.str("PG_SSLROOTCERT", default=None)

if PG_URL:
    # URL takes precedence; clear individual components
    PG_HOST = None
    PG_PASSWORD = None
    PG_PORT = None
    PG_SSLMODE = None
    PG_SSLROOTCERT = None

# Default database name (e.g., postgres or defaultdb)
PG_DATABASE = env.str("PG_DATABASE", default="postgres")
MYSQL_DATABASE = env.str("MYSQL_DATABASE", default="information_schema")

# Read-only connection (for consumers without replication slots/triggers)
PG_URL_RO = env.str("PG_URL_RO", default=None)
PG_HOST_RO = env.str("PG_HOST_RO", default=None)
PG_PORT_RO = env.int("PG_PORT_RO", default=None)
PG_USER_RO = env.str("PG_USER_RO", default=None)
PG_PASSWORD_RO = env.str("PG_PASSWORD_RO", default=None)
PG_SSLMODE_RO = env.str("PG_SSLMODE_RO", default=None)
PG_SSLROOTCERT_RO = env.str("PG_SSLROOTCERT_RO", default=None)

if PG_URL_RO:
    # URL takes precedence; clear individual components
    PG_HOST_RO = None
    PG_PASSWORD_RO = None
    PG_PORT_RO = None
    PG_SSLMODE_RO = None
    PG_SSLROOTCERT_RO = None

# MySQL/MariaDB
USE_UTF8MB4 = env.bool("USE_UTF8MB4", default=False)
MYSQL_DRIVERS = ("pymysql", "mysqldb", "mariadbconnector")
POSTGRES_DRIVERS = ("psycopg", "psycopg2", "psycopg3", "asyncpg", "pg8000")
IS_MYSQL_COMPAT = PG_DRIVER in MYSQL_DRIVERS

# PostgreSQL session settings
# work_mem controls memory for sort/hash operations before spilling to disk.
# Complex sync queries with LATERAL JOINs may need 12-16MB to avoid temp files.
# Set to None to use PostgreSQL server default.
PG_WORK_MEM = env.str("PG_WORK_MEM", default=None)

# =============================================================================
# Redis/Valkey
# =============================================================================

# Connection
REDIS_URL = env.str("REDIS_URL", default=None)
REDIS_HOST = env.str("REDIS_HOST", default="localhost")
REDIS_PORT = env.int("REDIS_PORT", default=6379)
REDIS_DB = env.int("REDIS_DB", default=0)
REDIS_SCHEME = env.str("REDIS_SCHEME", default="redis")

# Authentication
REDIS_AUTH = env.str("REDIS_AUTH", default=None)
REDIS_USER = env.str("REDIS_USER", default=None)

# Performance
REDIS_POLL_INTERVAL = env.float("REDIS_POLL_INTERVAL", default=0.01)
REDIS_READ_CHUNK_SIZE = env.int("REDIS_READ_CHUNK_SIZE", default=1000)
REDIS_WRITE_CHUNK_SIZE = env.int("REDIS_WRITE_CHUNK_SIZE", default=500)
REDIS_SOCKET_TIMEOUT = env.int("REDIS_SOCKET_TIMEOUT", default=5)
REDIS_RETRY_ON_TIMEOUT = env.bool("REDIS_RETRY_ON_TIMEOUT", default=False)

# =============================================================================
# Logging
# =============================================================================


def _get_logging_config(
    silent_loggers: t.Optional[t.List[str]] = None,
) -> dict:
    """Return logging configuration based on environment variables."""
    config: dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": env.str(
                    "CONSOLE_LOGGING_HANDLER_MIN_LEVEL", default="WARNING"
                ),
                "formatter": "simple",
            },
        },
        "loggers": {
            "": {
                "handlers": env.list("LOG_HANDLERS", default=["console"]),
                "level": env.str("GENERAL_LOGGING_LEVEL", default="DEBUG"),
                "propagate": True,
            },
        },
    }

    if silent_loggers:
        for silent_logger in silent_loggers:
            config["loggers"][silent_logger] = {"level": "INFO"}

    for logger_config in env.list("CUSTOM_LOGGING", default=[]):
        name, level = logger_config.split("=")
        config["loggers"][name] = {"level": level}

    return config


LOGGING = _get_logging_config(
    silent_loggers=[
        "urllib3.connectionpool",
        "urllib3.util.retry",
        "elasticsearch",
        "elastic_transport.transport",
    ]
)

logging.config.dictConfig(LOGGING)
