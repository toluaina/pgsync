"""PGSync Demo settings."""

from environs import Env

env = Env()

env.read_env()

MAX_RESULTS = env.int("MAX_RESULTS", default=100)
VECTOR_SEARCH = env.bool("VECTOR_SEARCH", default=False)

ELASTICSEARCH_URL = env.str("ELASTICSEARCH_URL")
ELASTICSEARCH_INDEX = env.str("ELASTICSEARCH_INDEX")
ELASTICSEARCH_TIMEOUT = env.int("ELASTICSEARCH_TIMEOUT", default=1000)
ELASTICSEARCH_VERIFY_CERTS = env.bool(
    "ELASTICSEARCH_VERIFY_CERTS",
    default=True,
)
