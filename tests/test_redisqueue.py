"""RedisQueues tests."""

import json

import pytest
from mock import call, patch
from redis.exceptions import ConnectionError, ResponseError

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
        with pytest.raises(ConnectionError):
            RedisQueue("something", namespace="foo")
        mock_get_redis_url.assert_called_once()
        mock_ping.assert_called_once()
        mock_logger.exception.assert_called_once_with(
            "Redis server is not reachable when pinging."
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
        mock_logger.debug.assert_called_once_with("pop size: 2")
        assert items == [1, 2]
        queue.push([3, 4, 5])
        items = queue.pop()
        mock_logger.debug.assert_any_call("pop size: 3")
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
        assert mock_logger.info.call_args_list == [
            call("Deleting redis key: queue:something"),
            call("Deleted redis key: queue:something"),
        ]
        assert queue.qsize == 0

    def test_pop_visible_in_snapshot(self):
        queue: RedisQueue = RedisQueue("something")
        queue.delete()
        # prepare 3 payloads with different xmin values
        payloads: list[dict] = [
            {"xmin": 2001, "data": "alpha"},
            {"xmin": 2002, "data": "beta"},
            {"xmin": 2003, "data": "gamma"},
        ]
        # push into Redis
        for item in payloads:
            queue._RedisQueue__db.rpush(queue.key, json.dumps(item))

        # fake visibility function that marks 2001 and 2003 as visible
        def fake_pg_visible_in_snapshot():
            def visibility(xmins):
                return {2001: True, 2002: False, 2003: True}

            return visibility

        # call the method
        result = queue.pop_visible_in_snapshot(fake_pg_visible_in_snapshot)

        # assert correct visible items are returned
        assert len(result) == 2
        assert {r["xmin"] for r in result} == {2001, 2003}

        # check Redis only has the invisible item left
        remaining = queue._RedisQueue__db.lrange(queue.key, 0, -1)
        assert len(remaining) == 1
        assert json.loads(remaining[0])["xmin"] == 2002

    def test_set_meta_and_get_meta(self):
        """Test trivial get/set round trip"""
        queue = RedisQueue("something")
        queue.delete()

        queue.set_meta({"checkpoint": 42})
        assert queue.get_meta() == {"checkpoint": 42}
        queue.delete()

    def test_set_meta_merges_fields(self):
        """Test that set_meta merges fields correctly"""
        queue = RedisQueue("something")
        queue.delete()

        # run 2 partial key updates
        queue.set_meta({"checkpoint": 100})
        queue.set_meta({"txid_current": 200})

        # check that both are successful and don't overwrite each other
        meta = queue.get_meta(default={})
        assert meta.get("checkpoint") == 100
        assert meta.get("txid_current") == 200
        queue.delete()

    def test_get_meta_default_when_absent(self):
        """Test that get_meta returns the default value when the key is absent"""
        queue = RedisQueue("something")
        queue.delete()

        assert queue.get_meta(default={}) == {}
        assert queue.get_meta() is None
        queue.delete()

    def test_set_meta_scalar_roundtrip(self):
        """Test that a non-dict scalar is transparently wrapped and unwrapped."""
        queue = RedisQueue("something")
        queue.delete()

        for value in (42, 3.14, "hello", True, [1, 2, 3]):
            queue.set_meta(value)
            assert queue.get_meta() == value

        queue.delete()

    def test_get_meta_legacy_string_format(self):
        """Test that get_meta correctly handles the legacy string format"""
        queue = RedisQueue("something")
        queue.delete()

        # write the old format directly, bypassing set_meta.
        queue._RedisQueue__db.set(
            queue._meta_key,
            json.dumps({"checkpoint": 7, "txid_current": 99}),
        )
        meta = queue.get_meta(default={})
        assert meta == {"checkpoint": 7, "txid_current": 99}
        queue.delete()

    def test_set_meta_overwrites_legacy_string_key(self):
        """set_meta must succeed even when the key holds a legacy plain string."""
        queue = RedisQueue("something")
        queue.delete()

        # write legacy plain-string format directly
        queue._RedisQueue__db.set(
            queue._meta_key, json.dumps({"checkpoint": 1})
        )

        # set_meta should transparently replace the string key with a hash
        queue.set_meta({"checkpoint": 99})
        assert queue.get_meta() == {"checkpoint": 99}
        queue.delete()

    def test_get_meta_reraises_non_wrongtype_response_error(self, mocker):
        """get_meta must re-raise ResponseErrors that are not WRONGTYPE."""
        queue = RedisQueue("something")
        queue.delete()

        mocker.patch.object(
            queue._RedisQueue__db,
            "hgetall",
            side_effect=ResponseError("ERR some other error"),
        )
        with pytest.raises(ResponseError, match="ERR some other error"):
            queue.get_meta()
        queue.delete()

    def test_pop_visible_in_snapshot_none_visible(self):
        queue: RedisQueue = RedisQueue("something")
        queue.delete()
        # insert 3 items that should NOT be visible
        payloads: list[dict] = [
            {"xmin": 3001, "data": "invisible1"},
            {"xmin": 3002, "data": "invisible2"},
            {"xmin": 3003, "data": "invisible3"},
        ]
        for item in payloads:
            queue._RedisQueue__db.rpush(queue.key, json.dumps(item))

        # fake visibility function: all xmins are NOT visible
        def fake_pg_visible_in_snapshot():
            def visibility(xmins):
                return {xmin: False for xmin in xmins}

            return visibility

        # call method
        result = queue.pop_visible_in_snapshot(fake_pg_visible_in_snapshot)
        # expect no items to be returned
        assert result == []
        # all items should still be in Redis
        remaining = queue._RedisQueue__db.lrange(queue.key, 0, -1)
        assert len(remaining) == 3
        xmins = [json.loads(i)["xmin"] for i in remaining]
        assert set(xmins) == {3001, 3002, 3003}
