"""PGSync RedisQueue."""
import json
import logging
from typing import List, Optional

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import (
    REDIS_READ_CHUNK_SIZE,
    REDIS_SOCKET_TIMEOUT,
    REDIS_SSL,
    REDIS_SSL_CA_CERT,
    REDIS_SSL_CERT_REQS,
    REDIS_SSL_CERTFILE,
    REDIS_SSL_KEYFILE,
)
from .urls import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis Backend."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        """Init Simple Queue with Redis Backend."""
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"
        self.ssl: bool = REDIS_SSL
        self.redis_ssl_args: dict = {}

        if self.ssl:
            self.redis_ssl_args = dict(
                ssl_keyfile=REDIS_SSL_KEYFILE,
                ssl_certfile=REDIS_SSL_CERTFILE,
                ssl_cert_reqs=REDIS_SSL_CERT_REQS,
                ssl_ca_certs=REDIS_SSL_CA_CERT,
            )

        logger.debug(f"Redis SSL Arguments: {self.redis_ssl_args}")

        try:
            self.__db: Redis = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                **self.redis_ssl_args,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def bulk_pop(self, chunk_size: Optional[int] = None) -> List[dict]:
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        if self.qsize > 0:
            pipeline = self.__db.pipeline()
            pipeline.lrange(self.key, 0, chunk_size - 1)
            pipeline.ltrim(self.key, chunk_size, -1)
            items: List = pipeline.execute()
            logger.debug(f"bulk_pop size: {len(items[0])}")
            return list(map(lambda value: json.loads(value), items[0]))

    def bulk_push(self, items: List) -> None:
        """Push multiple items onto the queue."""
        self.__db.rpush(self.key, *map(json.dumps, items))

    def delete(self) -> None:
        """Delete all items from the named queue."""
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)
