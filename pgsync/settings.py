"""PGSync settings

This module contains the settings for PGSync.
It reads environment variables from a .env file and sets default values for each variable.
The variables are used to configure various parameters such as block size, checkpoint path, polling interval, etc.
"""

import logging
import logging.config
import os
import typing as t

from environs import Env

logger = logging.getLogger(__name__)

env = Env()
env.read_env(path=os.path.join(os.getcwd(), ".env"))

# PGSync:
# page block size
BLOCK_SIZE = env.int("BLOCK_SIZE", default=2048 * 10)
CHECKPOINT_PATH = env.str("CHECKPOINT_PATH", default="./")
REDIS_CHECKPOINT = env.bool("REDIS_CHECKPOINT", default=False)
JOIN_QUERIES = env.bool("JOIN_QUERIES", default=False)
# batch size for LOGICAL_SLOT_CHANGES for minimizing tmp file disk usage
LOGICAL_SLOT_CHUNK_SIZE = env.int("LOGICAL_SLOT_CHUNK_SIZE", default=5000)
# stdout log interval (in secs)
LOG_INTERVAL = env.float("LOG_INTERVAL", default=0.5)
# number of workers to spawn for handling events
NUM_WORKERS = env.int("NUM_WORKERS", default=2)
# database driver psycopg2 or pymysql
PG_DRIVER = env.str("PG_DRIVER", default="psycopg2")
# poll db interval (consider reducing this duration to increase throughput)
POLL_TIMEOUT = env.float("POLL_TIMEOUT", default=0.1)
QUERY_LITERAL_BINDS = env.bool("QUERY_LITERAL_BINDS", default=False)
# db query chunk size (how many records to fetch at a time)
QUERY_CHUNK_SIZE = env.int("QUERY_CHUNK_SIZE", default=10000)
FILTER_CHUNK_SIZE = env.int("FILTER_CHUNK_SIZE", default=5000)
# replication slot cleanup interval (in secs)
REPLICATION_SLOT_CLEANUP_INTERVAL = env.float(
    "REPLICATION_SLOT_CLEANUP_INTERVAL",
    default=180.0,
)
# path to the application schema config
SCHEMA = env.str("SCHEMA", default=None)
S3_SCHEMA_URL = env.str("S3_SCHEMA_URL", default=None)
SCHEMA_URL = env.str("SCHEMA_URL", default=None)
USE_ASYNC = env.bool("USE_ASYNC", default=False)
STREAM_RESULTS = env.bool("STREAM_RESULTS", default=True)
# db polling interval
POLL_INTERVAL = env.float("POLL_INTERVAL", default=0.1)
FORMAT_WITH_COMMAS = env.bool("FORMAT_WITH_COMMAS", default=True)

# SQLAlchemy Settings:
# Use NullPool (no connection pooling) - useful for testing or when you want to close connections immediately
SQLALCHEMY_USE_NULLPOOL = env.bool("SQLALCHEMY_USE_NULLPOOL", default=False)
# This is the number of connections that will be persistently maintained in the pool.
SQLALCHEMY_POOL_SIZE = env.int("SQLALCHEMY_POOL_SIZE", default=5)
# This is the number of connections that can be opened beyond the pool_size when all connections in the pool are in use.
SQLALCHEMY_MAX_OVERFLOW = env.int("SQLALCHEMY_MAX_OVERFLOW", default=10)
# When set to True, a "ping" will be performed on connections before they are checked out of the pool to ensure they are still live.
SQLALCHEMY_POOL_PRE_PING = env.bool("SQLALCHEMY_POOL_PRE_PING", default=False)
# This means connections are not recycled based on a timeout. If set to a positive integer, connections will be recycled after that many seconds. For example, 3600 recycles connections after one hour.
SQLALCHEMY_POOL_RECYCLE = env.int("SQLALCHEMY_POOL_RECYCLE", default=-1)
# This is the number of seconds to wait for a connection to become available from the pool before raising a TimeoutError.
SQLALCHEMY_POOL_TIMEOUT = env.int("SQLALCHEMY_POOL_TIMEOUT", default=30)

# Elasticsearch/OpenSearch:
ELASTICSEARCH_API_KEY = env.str("ELASTICSEARCH_API_KEY", default=None)
ELASTICSEARCH_API_KEY_ID = env.str("ELASTICSEARCH_API_KEY_ID", default=None)
ELASTICSEARCH_AWS_HOSTED = env.bool("ELASTICSEARCH_AWS_HOSTED", default=False)
ELASTICSEARCH_AWS_REGION = env.str("ELASTICSEARCH_AWS_REGION", default=None)
ELASTICSEARCH_BASIC_AUTH = env.str("ELASTICSEARCH_BASIC_AUTH", default=None)
ELASTICSEARCH_BEARER_AUTH = env.str("ELASTICSEARCH_BEARER_AUTH", default=None)
# provide a path to CA certs on disk
ELASTICSEARCH_CA_CERTS = env.str("ELASTICSEARCH_CA_CERTS", default=None)
# Elasticsearch index chunk size (how many documents to index at a time)
ELASTICSEARCH_CHUNK_SIZE = env.int("ELASTICSEARCH_CHUNK_SIZE", default=5000)
# PEM formatted SSL client certificate
ELASTICSEARCH_CLIENT_CERT = env.str("ELASTICSEARCH_CLIENT_CERT", default=None)
# PEM formatted SSL client key
ELASTICSEARCH_CLIENT_KEY = env.str("ELASTICSEARCH_CLIENT_KEY", default=None)
ELASTICSEARCH_CLOUD_ID = env.str("ELASTICSEARCH_CLOUD_ID", default=None)
ELASTICSEARCH_HOST = env.str("ELASTICSEARCH_HOST", default="localhost")
ELASTICSEARCH_HTTP_AUTH = env.list("ELASTICSEARCH_HTTP_AUTH", default=None)
if ELASTICSEARCH_HTTP_AUTH:
    ELASTICSEARCH_HTTP_AUTH = tuple(ELASTICSEARCH_HTTP_AUTH)
ELASTICSEARCH_HTTP_COMPRESS = env.bool(
    "ELASTICSEARCH_HTTP_COMPRESS", default=True
)
# number of seconds we should wait before the first retry.
# Any subsequent retries will be powers of initial_backoff * 2**retry_number
ELASTICSEARCH_INITIAL_BACKOFF = env.float(
    "ELASTICSEARCH_INITIAL_BACKOFF", default=2
)
# maximum number of seconds a retry will wait
ELASTICSEARCH_MAX_BACKOFF = env.float("ELASTICSEARCH_MAX_BACKOFF", default=600)
# the maximum size of the request in bytes (default: 100MB)
ELASTICSEARCH_MAX_CHUNK_BYTES = env.int(
    "ELASTICSEARCH_MAX_CHUNK_BYTES",
    default=104857600,
)
# maximum number of times a document will be retried when 429 is received,
# set to 0 (default) for no retries on 429
ELASTICSEARCH_MAX_RETRIES = env.int("ELASTICSEARCH_MAX_RETRIES", default=0)
ELASTICSEARCH_OPAQUE_ID = env.str("ELASTICSEARCH_OPAQUE_ID", default=None)
ELASTICSEARCH_PASSWORD = env.str("ELASTICSEARCH_PASSWORD", default=None)
ELASTICSEARCH_PORT = env.int("ELASTICSEARCH_PORT", default=9200)
# the size of the task queue between the main thread
# (producing chunks to send) and the processing threads.
ELASTICSEARCH_QUEUE_SIZE = env.int("ELASTICSEARCH_QUEUE_SIZE", default=4)
ELASTICSEARCH_RAISE_ON_ERROR = env.bool(
    "ELASTICSEARCH_RAISE_ON_ERROR", default=True
)
# if ``False`` then don't propagate exceptions from call to elasticsearch bulk
ELASTICSEARCH_RAISE_ON_EXCEPTION = env.bool(
    "ELASTICSEARCH_RAISE_ON_EXCEPTION", default=True
)
ELASTICSEARCH_SCHEME = env.str("ELASTICSEARCH_SCHEME", default="http")
ELASTICSEARCH_SSL_ASSERT_FINGERPRINT = env.str(
    "ELASTICSEARCH_SSL_ASSERT_FINGERPRINT", default=None
)
ELASTICSEARCH_SSL_ASSERT_HOSTNAME = env.str(
    "ELASTICSEARCH_SSL_ASSERT_HOSTNAME", default=None
)
ELASTICSEARCH_SSL_CONTEXT = env.str("ELASTICSEARCH_SSL_CONTEXT", default=None)
# don't show warnings about ssl certs verification
ELASTICSEARCH_SSL_SHOW_WARN = env.bool(
    "ELASTICSEARCH_SSL_SHOW_WARN",
    default=False,
)
ELASTICSEARCH_SSL_VERSION = env.int("ELASTICSEARCH_SSL_VERSION", default=None)
ELASTICSEARCH_STREAMING_BULK = env.bool(
    "ELASTICSEARCH_STREAMING_BULK", default=False
)
# the size of the threadpool to use for the bulk requests
ELASTICSEARCH_THREAD_COUNT = env.int("ELASTICSEARCH_THREAD_COUNT", default=4)
# increase this if you are getting read request timeouts
ELASTICSEARCH_TIMEOUT = env.float("ELASTICSEARCH_TIMEOUT", default=10)
ELASTICSEARCH_USER = env.str("ELASTICSEARCH_USER", default=None)
# turn on SSL
ELASTICSEARCH_USE_SSL = env.bool("ELASTICSEARCH_USE_SSL", default=False)
ELASTICSEARCH_VERIFY_CERTS = env.bool(
    "ELASTICSEARCH_VERIFY_CERTS", default=True
)

# Number of concurrent connections that are
# able to be open at one time for this node.
# Having multiple connections per node allows
# for higher concurrency of requests
ELASTICSEARCH_POOL_MAXSIZE = env.int("ELASTICSEARCH_POOL_MAXSIZE", default=10)

# when using multiple threads for poll_db we need to account for other
# threads performing deletions.
ELASTICSEARCH_IGNORE_STATUS = env.list(
    "ELASTICSEARCH_IGNORE_STATUS", default=[404]
)
ELASTICSEARCH_IGNORE_STATUS = tuple(map(int, ELASTICSEARCH_IGNORE_STATUS))

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

OPENSEARCH_AWS_HOSTED = env.bool("OPENSEARCH_AWS_HOSTED", default=False)
OPENSEARCH_AWS_SERVERLESS = env.bool(
    "OPENSEARCH_AWS_SERVERLESS", default=False
)
# full Elasticsearch/OpenSearch url including user, password, host, port and dbname
ELASTICSEARCH_URL = env.str("ELASTICSEARCH_URL", default=None)

# Postgres/MySQL/MariaDB:
# full database url including user, password, host, port and dbname
PG_URL = env.str("PG_URL", default=None)
PG_HOST = env.str("PG_HOST", default="localhost")
PG_PASSWORD = env.str("PG_PASSWORD", default=None)
PG_PORT = env.int("PG_PORT", default=5432)
PG_SSLMODE = env.str("PG_SSLMODE", default=None)
PG_SSLROOTCERT = env.str("PG_SSLROOTCERT", default=None)
if PG_URL:
    # If PG_URL is set, we don't need to use the other PG_* variables
    PG_HOST = None
    PG_PASSWORD = None
    PG_PORT = None
    PG_SSLMODE = None
    PG_SSLROOTCERT = None
    PG_USER = env.str("PG_USER", default=None)
else:
    # If PG_URL is not set, we need to use the other PG_* variables
    PG_USER = env.str("PG_USER")
# The default database name e.g postges or defaultdb
PG_DATABASE = env.str("PG_DATABASE", default="postgres")
MYSQL_DATABASE = env.str("MYSQL_DATABASE", default="information_schema")

# Read-only Postgres:
# This is used for read-only consumers that do not require replication slots or triggers.
# full database url including user, password, host, port and dbname
PG_URL_RO = env.str("PG_URL_RO", default=None)
PG_HOST_RO = env.str("PG_HOST_RO", default=None)
PG_PASSWORD_RO = env.str("PG_PASSWORD_RO", default=None)
PG_PORT_RO = env.int("PG_PORT_RO", default=None)
PG_SSLMODE_RO = env.str("PG_SSLMODE_RO", default=None)
PG_SSLROOTCERT_RO = env.str("PG_SSLROOTCERT_RO", default=None)
PG_USER_RO = env.str("PG_USER_RO", default=None)
if PG_URL_RO:
    # If PG_URL_RO is set, we don't need to use the other PG_*_RO variables
    PG_HOST_RO = None
    PG_PASSWORD_RO = None
    PG_PORT_RO = None
    PG_SSLMODE_RO = None
    PG_SSLROOTCERT_RO = None
USE_UTF8MB4 = env.bool("USE_UTF8MB4", default=False)

MYSQL_DRIVERS = ("pymysql", "mysqldb", "mariadbconnector")
POSTGRES_DRIVERS = ("psycopg", "psycopg2", "psycopg3", "asyncpg", "pg8000")

# True for MySQL or MariaDB drivers
IS_MYSQL_COMPAT = PG_DRIVER in MYSQL_DRIVERS

# Redis/Valkey
REDIS_AUTH = env.str("REDIS_AUTH", default=None)
REDIS_USER = env.str("REDIS_USER", default=None)
REDIS_DB = env.int("REDIS_DB", default=0)
REDIS_HOST = env.str("REDIS_HOST", default="localhost")
# redis/valkey poll interval (in secs)
REDIS_POLL_INTERVAL = env.float("REDIS_POLL_INTERVAL", default=0.01)
REDIS_PORT = env.int("REDIS_PORT", default=6379)
# number of items to read from Redis/Valkey at a time
REDIS_READ_CHUNK_SIZE = env.int("REDIS_READ_CHUNK_SIZE", default=1000)
REDIS_SCHEME = env.str("REDIS_SCHEME", default="redis")
# redis/valkey socket connection timeout
REDIS_SOCKET_TIMEOUT = env.int("REDIS_SOCKET_TIMEOUT", default=5)
# number of items to write to Redis/Valkey at a time
REDIS_WRITE_CHUNK_SIZE = env.int("REDIS_WRITE_CHUNK_SIZE", default=500)
REDIS_URL = env.str("REDIS_URL", default=None)
REDIS_RETRY_ON_TIMEOUT = env.bool(
    "REDIS_RETRY_ON_TIMEOUT",
    default=False,
)


# Logging:
def _get_logging_config(silent_loggers: t.Optional[str] = None):
    """Return the logging configuration based on environment variables."""
    config: dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s",  # noqa E501
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": env.str(
                    "CONSOLE_LOGGING_HANDLER_MIN_LEVEL",
                    default="WARNING",
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
            config["loggers"][silent_logger] = {
                "level": "INFO",
            }

    for logger_config in env.list("CUSTOM_LOGGING", default=[]):
        logger, level = logger_config.split("=")
        config["loggers"][logger] = {
            "level": level,
        }
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
