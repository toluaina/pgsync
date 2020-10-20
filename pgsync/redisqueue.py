"""PGSync RedisQueue."""
import json
import logging

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT
from .utils import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis Backend."""

    def __init__(self, name, namespace='queue', **kwargs):
        """
        The default connection parameters are:
        host = 'localhost', port = 6379, db = 0
        """
        url = get_redis_url(**kwargs)
        self.key = f'{namespace}:{name}'
        try:
            self.__db = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f'Redis server is not running: {e}')
            raise

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def push(self, item):
        """Push item into the queue."""
        self.__db.rpush(self.key, json.dumps(item))

    def pop(self, block=True, timeout=None):
        """
        Remove and return an item from the queue.

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

    def bulk_pop(self, chunk_size=None):
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_CHUNK_SIZE
        items = []
        while self.__db.llen(self.key) != 0:
            if len(items) > chunk_size:
                break
            item = self.__db.lpop(self.key)
            items.append(json.loads(item))
        return items

    def bulk_push(self, items):
        """Push multiple items onto the queue."""
        self.__db.rpush(self.key, *map(json.dumps, items))

    def pop_nowait(self):
        """Equivalent to pop(False)."""
        return self.pop(False)

    def _delete(self):
        logger.info(f'Deleting redis key: {self.key}')
        self.__db.delete(self.key)


def redis_engine(host=None, password=None, port=None, db=None):
    url = get_redis_url(host=host, password=password, port=port, db=db)
    try:
        conn = Redis.from_url(
            url,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
        )
        conn.ping()
    except ConnectionError as e:
        logger.exception(f'Redis server is not running: {e}')
        raise
    return conn
