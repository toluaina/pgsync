"""PGSync RedisQueue."""

import json
import logging
import typing as t

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import (
    REDIS_READ_CHUNK_SIZE,
    REDIS_RETRY_ON_TIMEOUT,
    REDIS_SOCKET_TIMEOUT,
)
from .urls import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis/Valkey Backend."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        """Init Simple Queue with Redis/Valkey Backend."""
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"
        self._meta_key: str = f"{self.key}:meta"
        try:
            self.__db: Redis = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def pop(self, chunk_size: t.Optional[int] = None) -> t.List[dict]:
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        if self.qsize > 0:
            pipeline = self.__db.pipeline()
            pipeline.lrange(self.key, 0, chunk_size - 1)
            pipeline.ltrim(self.key, chunk_size, -1)
            items: t.List = pipeline.execute()
            logger.debug(f"pop size: {len(items[0])}")
            return list(map(lambda value: json.loads(value), items[0]))

    def pop_visible_in_snapshot(
        self,
        pg_visible_in_snapshot: t.Callable[[t.List[int]], dict],
        chunk_size: t.Optional[int] = None,
    ) -> t.List[dict]:
        """
        Pop items in the queue that are visible in the current snapshot.
        Uses the provided pg_visible_in_snapshot function to determine visibility.
        This function is useful for read-only consumers that need to process items
        that are visible in the current PostgreSQL snapshot.
        """
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        items: t.List = self.__db.lrange(self.key, 0, chunk_size - 1)
        if not items:
            return []
        payloads = [json.loads(i) for i in items]
        visible_map: dict = pg_visible_in_snapshot()(
            [payload.get("xmin") for payload in payloads if "xmin" in payload]
        )
        visible: t.List[dict] = []
        for item, payload in zip(items, payloads):
            if "xmin" not in payload:
                logger.warning(
                    f"Skipping payload without 'xmin' key: {payload}"
                )
                continue
            if visible_map.get(payload["xmin"]):
                # Claim atomically
                removed = self.__db.lrem(self.key, 1, item)
                if removed:
                    visible.append(payload)
        return visible

    def push(self, items: t.Iterable[dict]) -> None:
        """Push multiple items onto the queue."""
        self.__db.rpush(self.key, *map(json.dumps, items))

    def delete(self) -> None:
        """Delete all items from the named queue."""
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)
        self.__db.delete(self._meta_key)
        logger.info(f"Deleted redis key: {self.key}")

    def set_meta(self, value: t.Any) -> None:
        """Store an arbitrary JSON-serialisable value in a dedicated key."""
        self.__db.set(self._meta_key, json.dumps(value))

    def get_meta(self, default: t.Any = None) -> t.Any:
        """Retrieve the stored value (or *default* if nothing is set)."""
        raw: t.Optional[str] = self.__db.get(self._meta_key)
        return json.loads(raw) if raw is not None else default
