"""RedisQueues tests."""

import pytest
from mock import patch
from redis.exceptions import ConnectionError

from pgsync.redisqueue import RedisQueue
from pgsync.settings import (
    REDIS_SOCKET_TIMEOUT,
    REDIS_SSL_CA_CERT,
    REDIS_SSL_CERT_REQS,
    REDIS_SSL_CERTFILE,
    REDIS_SSL_KEYFILE,
)


class TestRedisQueue(object):
    """Redis Queue tests."""

    @patch("redis.Redis.ping")
    @patch("pgsync.redisqueue.get_redis_url")
    @patch("pgsync.redisqueue.logger")
    def test_redis_conn(self, mock_logger, mock_get_redis_url, mock_ping):
        """Test the redis constructor."""
        mock_get_redis_url.return_value = (
            "redis://kermit:frog@some-host:6379/0"
        )
        mock_ping.return_value = True
        queue = RedisQueue("something", namespace="foo")
        assert queue.key == "foo:something"
        mock_get_redis_url.assert_called_once()
        mock_ping.assert_called_once()
        mock_logger.exception.assert_not_called()

    @patch("redis.Redis.ping")
    @patch("pgsync.redisqueue.get_redis_url")
    @patch("pgsync.redisqueue.logger")
    def test_redis_conn_fail(self, mock_logger, mock_get_redis_url, mock_ping):
        """Test the redis constructor fails."""
        mock_get_redis_url.return_value = (
            "redis://kermit:frog@some-host:6379/0"
        )
        mock_ping.side_effect = ConnectionError("pong")
        with pytest.raises(ConnectionError):
            RedisQueue("something", namespace="foo")
        mock_get_redis_url.assert_called_once()
        mock_ping.assert_called_once()
        mock_logger.exception.assert_called_once_with(
            "Redis server is not running: pong"
        )

    @patch("redis.Redis.from_url")
    @patch("pgsync.redisqueue.get_redis_url")
    @patch("pgsync.redisqueue.logger")
    def test_redis_conn_ssl(
        self, mock_logger, mock_get_redis_url, mock_redis_from_url
    ):
        """Test the redis ssl constructor."""
        mock_redis_url = "rediss://tardis:doctor@some-host:6379/0"
        mock_get_redis_url.return_value = mock_redis_url
        queue = RedisQueue("something", namespace="foo", ssl=True)
        assert queue.key == "foo:something"
        mock_get_redis_url.assert_called_once()
        mock_redis_from_url.assert_called_once_with(
            mock_redis_url,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            ssl_keyfile=REDIS_SSL_KEYFILE,
            ssl_certfile=REDIS_SSL_CERTFILE,
            ssl_cert_reqs=REDIS_SSL_CERT_REQS,
            ssl_ca_certs=REDIS_SSL_CA_CERT,
        )
        mock_logger.exception.assert_not_called()

    def test_qsize(self):
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
        mock_logger.debug.assert_called_once_with("bulk_pop size: 2")
        assert items == [1, 2]
        queue.bulk_push([3, 4, 5])
        items = queue.bulk_pop()
        mock_logger.debug.assert_any_call("bulk_pop size: 3")
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
