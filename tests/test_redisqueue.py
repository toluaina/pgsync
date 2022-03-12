"""RedisQueues tests."""

import pytest
from mock import patch
from redis.exceptions import ConnectionError

from pgsync.redisqueue import RedisQueue


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
        with pytest.raises(ConnectionError) as excinfo:
            queue = RedisQueue("something", namespace="foo")
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
        queue.bulk_push([1, 2])
        assert queue.qsize == 2
        queue.delete()
        assert queue.qsize == 0

    def test_bulk_push(self):
        """Test the redis bulk_push."""
        queue = RedisQueue("something")
        queue.delete()
        queue.bulk_push([1, 2])
        assert queue.qsize == 2
        queue.bulk_push([3, 4, 5])
        assert queue.qsize == 5
        queue.delete()

    @patch("pgsync.redisqueue.logger")
    def test_bulk_pop(self, mock_logger):
        """Test the redis bulk_pop."""
        queue = RedisQueue("something")
        queue.delete()
        queue.bulk_push([1, 2])
        items = queue.bulk_pop()
        mock_logger.debug.assert_called_once_with("bulk_pop nsize: 2")
        assert items == [1, 2]
        queue.bulk_push([3, 4, 5])
        items = queue.bulk_pop()
        mock_logger.debug.assert_any_call("bulk_pop nsize: 3")
        assert items == [3, 4, 5]
        queue.delete()

    @patch("pgsync.redisqueue.logger")
    def test_delete(self, mock_logger):
        queue = RedisQueue("something")
        queue.bulk_push([1])
        queue.bulk_push([2, 3])
        queue.bulk_push([4, 5, 6])
        assert queue.qsize == 6
        queue.delete()
        mock_logger.info.assert_called_once_with(
            "Deleting redis key: queue:something"
        )
        assert queue.qsize == 0
