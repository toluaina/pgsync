"""PGSync Settings."""
import logging
import logging.config

from environs import Env

logger = logging.getLogger(__name__)

env = Env()
env.read_env()

# PGSync:
# path to the application schema config
SCHEMA = env.str('SCHEMA', default=None)
# db query chunk size (how many records to fetch at a time)
QUERY_CHUNK_SIZE = env.int('QUERY_CHUNK_SIZE', default=10000)
# poll db interval (consider reducing this duration to increase throughput)
POLL_TIMEOUT = env.float('POLL_TIMEOUT', default=0.1)
# replication slot cleanup interval (in secs)
REPLICATION_SLOT_CLEANUP_INTERVAL = env.float(
    'REPLICATION_SLOT_CLEANUP_INTERVAL',
    default=3600.0,
)

# Elasticsearch:
ELASTICSEARCH_SCHEME = env.str('ELASTICSEARCH_SCHEME', default='http')
ELASTICSEARCH_HOST = env.str('ELASTICSEARCH_HOST', default='localhost')
ELASTICSEARCH_PORT = env.int('ELASTICSEARCH_PORT', default=9200)
ELASTICSEARCH_USER = env.str('ELASTICSEARCH_USER', default=None)
ELASTICSEARCH_PASSWORD = env.str('ELASTICSEARCH_PASSWORD', default=None)
# increase this if you are getting read request timeouts
ELASTICSEARCH_TIMEOUT = env.int('ELASTICSEARCH_TIMEOUT', default=10)
# Elasticsearch index chunk size (how many documents to index at a time)
ELASTICSEARCH_CHUNK_SIZE = env.int('ELASTICSEARCH_CHUNK_SIZE', default=2000)
# the maximum size of the request in bytes (default: 100MB)
ELASTICSEARCH_MAX_CHUNK_BYTES = env.int(
    'ELASTICSEARCH_MAX_CHUNK_BYTES',
    default=104857600,
)
# the size of the threadpool to use for the bulk requests
ELASTICSEARCH_THREAD_COUNT = env.int('ELASTICSEARCH_THREAD_COUNT', default=4)
# the size of the task queue between the main thread
# (producing chunks to send) and the processing threads.
ELASTICSEARCH_QUEUE_SIZE = env.int('ELASTICSEARCH_QUEUE_SIZE', default=4)
ELASTICSEARCH_VERIFY_CERTS = env.bool(
    'ELASTICSEARCH_VERIFY_CERTS',
    default=True,
)

# Postgres:
PG_HOST = env.str('PG_HOST', default='localhost')
PG_USER = env.str('PG_USER')
PG_PORT = env.int('PG_PORT', default=5432)
PG_PASSWORD = env.str('PG_PASSWORD', default=None)
PG_SSLMODE = env.str('PG_SSLMODE', default=None)
PG_SSLROOTCERT = env.str('PG_SSLROOTCERT', default=None)

# Redis:
REDIS_HOST = env.str('REDIS_HOST', default='localhost')
REDIS_PORT = env.int('REDIS_PORT', default=6379)
REDIS_DB = env.int('REDIS_DB', default=0)
REDIS_AUTH = env.str('REDIS_AUTH', default=None)
# number of items to read from Redis at a time
REDIS_CHUNK_SIZE = env.int('REDIS_CHUNK_SIZE', default=1000)
# redis socket connection timeout
REDIS_SOCKET_TIMEOUT = env.int('REDIS_SOCKET_TIMEOUT', default=5)
# redis poll interval (in secs)
REDIS_POLL_INTERVAL = env.float('REDIS_POLL_INTERVAL', default=0.01)


# Logging:
def get_logging_config(silent_loggers=None):
    """
    Return the python logging configuration based on environment variables.
    """
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format':
                '%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s: %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': env.str(
                    'CONSOLE_LOGGING_HANDLER_MIN_LEVEL',
                    default='WARNING',
                ),
                'formatter': 'simple',
            },
        },
        'loggers': {
            '': {
                'handlers': env.list('LOG_HANDLERS', default=['console']),
                'level': env.str('GENERAL_LOGGING_LEVEL', default='DEBUG'),
                'propagate': True,
            },
        },
    }
    if silent_loggers:
        for silent_logger in silent_loggers:
            logging_config['loggers'][silent_logger] = {
                'level': 'INFO',
            }

    for logger_config in env.list('CUSTOM_LOGGING', default=[]):
        logger, level = logger_config.split('=')
        logging_config['loggers'][logger] = {
            'level': level,
        }
    return logging_config


LOGGING = get_logging_config(
    silent_loggers=[
        'urllib3.connectionpool',
        'urllib3.util.retry',
        'elasticsearch',
    ]
)

logging.config.dictConfig(LOGGING)
