"""Tests for the settings module."""
import importlib

from pgsync import settings
from pgsync.base import pg_engine
from pgsync.redisqueue import RedisQueue


def test_redis_url(mocker):
    """Test the redis url is configured."""
    mock_get_redis_url = mocker.patch(
        "pgsync.redisqueue.get_redis_url",
        return_value="redis://kermit:frog@some-host:6379/0",
    )
    mocker.patch("redis.Redis.ping", return_value=True)
    mocker.patch("logging.config.dictConfig")
    RedisQueue("something")
    mock_get_redis_url.assert_called_once()


def test_postgres_url(mocker):
    """Test the postgres url is configured."""
    mock_get_postgres_url = mocker.patch(
        "pgsync.base.get_postgres_url",
        return_value="postgresql://kermit:frog@some-host:5432/wheel",
    )
    mocker.patch("logging.config.dictConfig")
    engine = pg_engine("wheel")
    mock_get_postgres_url.assert_called_once()
    url = "postgresql://kermit:frog@some-host:5432/wheel"
    assert str(engine.engine.url) == url


def test_elasticsearch_url(mocker):
    """Test the elasticsearch url is configured."""
    mock_get_elasticsearch_url = mocker.patch(
        "pgsync.urls.get_elasticsearch_url",
        return_value="http://some-domain:33",
    )
    mocker.patch("logging.config.dictConfig")
    importlib.reload(settings)
    assert mock_get_elasticsearch_url() == "http://some-domain:33"
    mock_get_elasticsearch_url.assert_called_once()
