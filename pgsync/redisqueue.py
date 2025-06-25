"""PGSync RedisQueue."""

import json
import logging
import time
import typing as t

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_READ_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT
from .urls import get_redis_url

# Pick a MULTIPLIER > max timestamp_ms (~1.7e12).
# 10**13 is safe for now.
_MULTIPLIER = 10**13


logger = logging.getLogger(__name__)


class RedisQueue:
    """A Redis‐backed queue where items become poppable only once ready is True."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"
        self._meta_key: str = f"{self.key}:meta"
        try:
            self.__db: Redis = Redis.from_url(
                url, socket_timeout=REDIS_SOCKET_TIMEOUT
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Number of items currently in the ZSET (regardless of ready/not)."""
        return self.__db.zcard(self.key)

    def push(self, items: t.List[dict], weight: float = 0.0) -> None:
        """
        Push a batch of items with the given numeric weight.

        - Higher weight -> higher priority.
        - Among equal weight, FIFO order.
        """
        now_ms: int = int(time.time() * 1_000)
        mapping: dict = {}
        for item in items:
            # score = -weight*M + timestamp
            score = -weight * _MULTIPLIER + now_ms
            mapping[json.dumps(item)] = score
        # ZADD will add/update each member's score
        self.__db.zadd(self.key, mapping)

    def pop(self, chunk_size: int = REDIS_READ_CHUNK_SIZE) -> t.List[dict]:
        """
        Pop up to chunk_size highest priority items (by weight, then FIFO).
        """
        # ZPOPMIN pulls the entries with the smallest score first
        popped: t.List[t.Tuple[bytes, float]] = self.__db.zpopmin(
            self.key, chunk_size
        )
        results: t.List[dict] = [
            json.loads(member) for member, score in popped
        ]
        logger.debug(f"popped {len(results)} items (by priority)")
        return results

    def delete(self) -> None:
        """Delete all items from the named queue including its metadata."""
        logger.info(f"Deleting redis key: {self.key} and {self._meta_key}")
        self.__db.delete(self.key)
        self.__db.delete(self._meta_key)

    def set_meta(self, value: t.Any) -> None:
        """Store an arbitrary JSON‐serializable value in a dedicated key."""
        self.__db.set(self._meta_key, json.dumps(value))

    def get_meta(self, default: t.Any = None) -> t.Any:
        """Retrieve the stored metadata (or *default* if nothing is set)."""
        raw = self.__db.get(self._meta_key)
        return json.loads(raw) if raw is not None else default
