"""RedisQueues tests."""

import json
import time
import typing as t

import pytest
from freezegun import freeze_time
from mock import patch
from redis.exceptions import ConnectionError

from pgsync.redisqueue import _MULTIPLIER, RedisQueue


class TestRedisQueue(object):
    """Redis Queue tests."""

    @patch("pgsync.redisqueue.logger")
    def test_redis_conn(self, mock_logger, mocker):
        """Test the redis constructor."""
        mock_get_redis_url = mocker.patch(
            "pgsync.redisqueue.get_redis_url",
            return_value="redis://kermit:frog@some-host:6379/0",
        )
        mock_ping = mocker.patch("redis.Redis.ping", return_value=True)
        queue = RedisQueue("something", namespace="foo")
        assert queue.key == "foo:something"
        mock_get_redis_url.assert_called_once()
        mock_ping.assert_called_once()
        mock_logger.exception.assert_not_called()

    @patch("pgsync.redisqueue.logger")
    def test_redis_conn_fail(self, mock_logger, mocker):
        """Test the redis constructor fails."""
        mock_get_redis_url = mocker.patch(
            "pgsync.redisqueue.get_redis_url",
            return_value="redis://kermit:frog@some-host:6379/0",
        )
        mock_ping = mocker.patch(
            "redis.Redis.ping", side_effect=ConnectionError("pong")
        )
        with pytest.raises(ConnectionError):
            RedisQueue("something", namespace="foo")
        mock_get_redis_url.assert_called_once()
        mock_ping.assert_called_once()
        mock_logger.exception.assert_called_once_with(
            "Redis server is not running: pong"
        )

    def test_qsize(self, mocker):
        """Test the redis qsize."""
        queue = RedisQueue("something")
        queue.delete()
        assert queue.qsize == 0
        queue.push([1, 2])
        assert queue.qsize == 2
        queue.delete()
        assert queue.qsize == 0

    def test_push(self):
        """Test the redis push."""
        queue = RedisQueue("something")
        queue.delete()
        queue.push([1, 2])
        assert queue.qsize == 2
        queue.push([3, 4, 5])
        assert queue.qsize == 5
        queue.delete()

    @patch("pgsync.redisqueue.logger")
    def test_pop(self, mock_logger):
        """Test the redis pop."""
        queue = RedisQueue("something")
        queue.delete()
        queue.push([1, 2])
        items = queue.pop()
        mock_logger.debug.assert_called_once_with(
            "popped 2 items (by priority)"
        )
        assert items == [1, 2]
        queue.push([3, 4, 5])
        items = queue.pop()
        mock_logger.debug.assert_any_call("popped 3 items (by priority)")
        assert items == [3, 4, 5]
        queue.delete()

    @patch("pgsync.redisqueue.logger")
    def test_delete(self, mock_logger):
        queue = RedisQueue("something")
        queue.push([1])
        queue.push([2, 3])
        queue.push([4, 5, 6])
        assert queue.qsize == 6
        queue.delete()
        mock_logger.info.assert_called_once_with(
            "Deleting redis key: queue:something and queue:something:meta"
        )
        assert queue.qsize == 0

    @freeze_time("2025-06-25T12:00:00Z")
    def test_push_and_pop_respects_weight_and_fifo(self):
        queue: RedisQueue = RedisQueue("test")
        a: dict = {"id": "A"}
        b: dict = {"id": "B"}
        c: dict = {"id": "C"}
        # A has no explicit weight → default 0.0
        queue.push([a])
        # wait a millisecond for a different timestamp
        time.sleep(0.001)
        # B and C both weight=5
        queue.push([b], weight=5)
        time.sleep(0.001)
        queue.push([c], weight=5)
        # popping 3 items
        out = queue.pop(3)
        # B then C (both weight=5, FIFO), then A (weight=0)
        assert [x["id"] for x in out] == ["B", "C", "A"]

    @freeze_time("2024-06-25T12:00:00Z")
    def test_push_adds_correct_scores(self):
        queue: RedisQueue = RedisQueue("test")
        items: t.List[t.Dict] = [{"id": 1}, {"id": 2}]
        weight: float = 5.0
        with (
            patch.object(queue, "_RedisQueue__db") as mock_db,
            patch(
                "time.time", side_effect=[1_717_267_200.100, 1_717_267_200.200]
            ),
        ):
            queue.push(items, weight=weight)
            expected_mapping: dict = {
                json.dumps({"id": 1}, sort_keys=True): -weight * _MULTIPLIER
                + int(1_717_267_200.100 * 1_000),
                json.dumps({"id": 2}, sort_keys=True): -weight * _MULTIPLIER
                + int(1_717_267_200.200 * 1_000),
            }
            mock_db.zadd.assert_called_once_with(
                "queue:test", expected_mapping
            )
