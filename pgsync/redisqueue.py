"""PGSync RedisQueue."""
import json
import logging
from typing import List, Optional

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_READ_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT
from .urls import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis Backend."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        """The default connection parameters are:

        host = 'localhost', port = 6379, db = 0
        """
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"
        try:
            self.__db = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self.qsize == 0

    def push(self, item) -> None:
        """Push item into the queue."""
        self.__db.rpush(self.key, json.dumps(item))

    def pop(self, block: bool = True, timeout: int = None) -> dict:
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available.
        """
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)
        if item:
            item = item[1]
        return json.loads(item)

    def bulk_pop(self, chunk_size: Optional[int] = None) -> List[dict]:
        """Remove and return multiple items from the queue."""
        chunk_size: int = chunk_size or REDIS_READ_CHUNK_SIZE
        pipeline = self.__db.pipeline()
        pipeline.lrange(self.key, 0, chunk_size - 1)
        pipeline.ltrim(self.key, chunk_size, -1)
        items: List[List[bytes], bool] = pipeline.execute()
        logger.debug(f"bulk_pop nsize: {len(items[0])}")
        return list(map(lambda x: json.loads(x), items[0]))

    def bulk_push(self, items: List) -> None:
        """Push multiple items onto the queue."""
        self.__db.rpush(self.key, *map(json.dumps, items))

    def pop_nowait(self):
        """Equivalent to pop(False)."""
        return self.pop(False)

    def _delete(self) -> None:
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)


def redis_engine(
    scheme: Optional[str] = None,
    host: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
):
    url: str = get_redis_url(
        scheme=scheme, host=host, password=password, port=port, db=db
    )
    try:
        conn = Redis.from_url(
            url,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
        )
        conn.ping()
    except ConnectionError as e:
        logger.exception(f"Redis server is not running: {e}")
        raise
    return conn
