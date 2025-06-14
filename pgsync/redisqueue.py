"""PGSync RedisQueue."""

import json
import logging
import time
import typing as t

from redis import Redis
from redis.exceptions import ConnectionError

from .settings import REDIS_READ_CHUNK_SIZE, REDIS_SOCKET_TIMEOUT
from .urls import get_redis_url

logger = logging.getLogger(__name__)


# sentinel 300 years in ms
_FAR_FUTURE = int((time.time() + 300 * 365 * 24 * 3600) * 1_000)


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

    def push(self, items: t.List[dict], ready: bool = True) -> None:
        """
        Push a batch of items.
        If ready=True score = now (ms), so pop_ready() can retrieve immediately.
        If ready=False score = FAR_FUTURE, so pop_ready() ignores it until mark_ready().
        """
        now_ms: int = int(time.time() * 1_000)
        score: int = now_ms if ready else _FAR_FUTURE
        mapping: dict = {json.dumps(item): score for item in items}
        self.__db.zadd(self.key, mapping)

    def pop_ready(
        self, chunk_size: int = REDIS_READ_CHUNK_SIZE
    ) -> t.List[dict]:
        """
        Atomically pull up to chunk_size items whose score ≤ now_ms.
        These are the ready items.
        """
        now_ms: int = int(time.time() * 1_000)
        # fetch members ready to run
        values = self.__db.zrangebyscore(
            self.key, 0, now_ms, start=0, num=chunk_size
        )
        if not values:
            return []
        # remove them in one pipeline
        pipeline = self.__db.pipeline()
        pipeline.zrem(self.key, *values)
        pipeline.execute()
        return [json.loads(value) for value in values]

    def pop(
        self, chunk_size: int = REDIS_READ_CHUNK_SIZE, auto_ready: bool = False
    ) -> t.List[dict]:
        """
        Pop up to chunk_size ready items.
        If auto_ready=True and none are ready, will flip up
        to chunk_size delayed items to ready and retry once.
        """
        items: t.List[dict] = self.pop_ready(chunk_size)
        logger.debug(f"pop size: {len(items[0])}")
        if not items and auto_ready:
            flipped = self._mark_next_n_ready(chunk_size)
            if flipped:
                items = self.pop_ready(chunk_size)
        return items

    def mark_ready(self, items: t.List[dict]) -> None:
        """
        Flip previously-pushed, delayed items to ready by
        updating their score to now_ms.
        """
        now_ms: int = int(time.time() * 1_000)
        mapping: dict = {json.dumps(item): now_ms for item in items}
        self.__db.zadd(self.key, mapping)

    def mark_all_ready(self) -> None:
        """
        Find every queue member whose score is still in the future
        and set its score to now, so pop_ready() will pick it up.
        """
        now_ms: int = int(time.time() * 1_000)
        # grab everything with score > now
        pending = self.__db.zrangebyscore(self.key, now_ms + 1, "+inf")
        if not pending:
            return

        # map each member to new score
        update: dict = {member: now_ms for member in pending}
        self.__db.zadd(self.key, update)

    def _mark_next_n_ready(self, nsize: int) -> int:
        """
        Find up to nsize members whose score > now and set their score to now.
        Returns how many were flipped.
        """
        now_ms: int = int(time.time() * 1_000)
        # get at most nsize pending items (score > now)
        pending = self.__db.zrangebyscore(
            self.key, now_ms + 1, "+inf", start=0, num=nsize
        )
        if not pending:
            return 0
        update: dict = {member: now_ms for member in pending}
        self.__db.zadd(self.key, update)
        return len(pending)

    def delete(self) -> None:
        """Delte all items from the named queue including its metadata."""
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
