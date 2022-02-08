"""URLS tests."""
import pytest
from mock import MagicMock, patch

from pgsync.exc import SchemaError
from pgsync.plugin import Plugins
from pgsync.urls import (
    _get_auth,
    get_elasticsearch_url,
    get_postgres_url,
    get_redis_url,
)
from pgsync.utils import get_config


@pytest.mark.usefixtures("table_creator")
class TestUrls(object):
    """URLS tests."""

    @patch("pgsync.urls.logger")
    def test_get_elasticsearch_url(self, mock_logger):
        assert get_elasticsearch_url() == "http://localhost:9200"
        mock_logger.debug.assert_called_once()
        assert (
            get_elasticsearch_url(scheme="https") == "https://localhost:9200"
        )
        assert mock_logger.debug.call_count == 2
        assert (
            get_elasticsearch_url(
                user="kermit",
                password="1234",
            )
            == "http://kermit:1234@localhost:9200"
        )
        assert get_elasticsearch_url(port=9999) == "http://localhost:9999"

    def test_get_postgres_url(self):
        url = get_postgres_url("mydb")
        assert url.endswith("@localhost:5432/mydb")
        assert (
            get_postgres_url("mydb", user="kermit", password="12345")
            == "postgresql://kermit:12345@localhost:5432/mydb"
        )
        url = get_postgres_url("mydb", port=9999)
        assert url.endswith("@localhost:9999/mydb")

    @patch("pgsync.urls.logger")
    def test_get_redis_url(self, mock_logger):
        assert get_redis_url() == "redis://localhost:6379/0"
        mock_logger.debug.assert_called_with(
            "Connecting to Redis without password."
        )
        assert (
            get_redis_url(
                password="1234",
                port=9999,
            )
            == "redis://:1234@localhost:9999/0"
        )
        assert get_redis_url(host="skynet") == "redis://skynet:6379/0"

    @patch("pgsync.urls.logger")
    def test_get_config(self, mock_logger):
        assert __file__ == get_config(config=__file__)
        with pytest.raises(SchemaError) as excinfo:
            get_config()
            assert "Schema config not set" in str(excinfo.value)
        with pytest.raises(IOError) as excinfo:
            get_config("/tmp/nonexistent")
            assert 'Schema config "/tmp/nonexistent" not found' in str(
                excinfo.value
            )

    @patch("pgsync.urls.logger")
    def test_get_auth(self, mock_logger):
        assert __file__ == get_config(config=__file__)
        with patch("pgsync.urls.Plugins", return_value=MagicMock()):
            auth = _get_auth("something")
            mock_logger.assert_not_called()

        with patch("pgsync.urls.Plugins", side_effect=ModuleNotFoundError):
            auth = _get_auth("something")
