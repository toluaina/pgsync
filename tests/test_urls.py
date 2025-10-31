"""URLS tests."""

import pytest
from mock import MagicMock, patch

from pgsync.settings import ELASTICSEARCH_PORT, IS_MYSQL_COMPAT
from pgsync.urls import (
    _get_auth,
    get_database_url,
    get_redis_url,
    get_search_url,
)
from pgsync.utils import validate_config


@pytest.mark.usefixtures("table_creator")
class TestUrls(object):
    """URLS tests."""

    @patch("pgsync.urls.logger")
    def test_get_search_url(self, mock_logger):
        assert (
            get_search_url(scheme="https")
            == f"https://localhost:{ELASTICSEARCH_PORT}"
        )
        mock_logger.debug.assert_called_once()
        assert (
            get_search_url(
                scheme="http",
                user="kermit",
                password="1234",
            )
            == f"http://kermit:1234@localhost:{ELASTICSEARCH_PORT}"
        )
        assert (
            get_search_url(scheme="http", port=9999) == "http://localhost:9999"
        )
        assert (
            get_search_url(scheme="http")
            == f"http://localhost:{ELASTICSEARCH_PORT}"
        )
        assert mock_logger.debug.call_count == 3

    def test_get_database_url(self):
        url = get_database_url("mydb")
        if IS_MYSQL_COMPAT:
            assert url.endswith("@localhost:3306/mydb")
            assert (
                get_database_url("mydb", user="kermit", password="12345")
                == "mysql+pymysql://kermit:12345@localhost:3306/mydb"
            )
        else:
            assert url.endswith("@localhost:5432/mydb")
            assert (
                get_database_url("mydb", user="kermit", password="12345")
                == "postgresql+psycopg2://kermit:12345@localhost:5432/mydb"
            )
        url = get_database_url("mydb", port=9999)
        assert url.endswith("@localhost:9999/mydb")
        # with patch("pgsync.urls.logger") as mock_logger:
        #     assert (
        #         get_database_url("mydb", user="kermit")
        #         == "postgresql+psycopg2://kermit@localhost:5432/mydb"
        #     )
        #     mock_logger.debug.assert_called_once_with(
        #         "Connecting to Postgres without password."
        #     )

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
    def test_get_redis_url_with_username_and_password(self, mock_logger):
        assert (
            get_redis_url(username="kermit", password="1234")
            == "redis://kermit:1234@localhost:6379/0"
        )
        mock_logger.debug.assert_called_with(
            "Connecting to Redis with custom username and password."
        )
        assert (
            get_redis_url(
                username="kermit",
                password="1234",
                port=9999,
            )
            == "redis://kermit:1234@localhost:9999/0"
        )

    @patch("pgsync.urls.logger")
    def test_validate_config_with_mock_logger(self, mock_logger):
        # Valid file path should not raise (uses current file)
        validate_config(config=__file__)

        # Missing all config, url and s3_schema_url -> ValueError
        with pytest.raises(ValueError) as excinfo:
            validate_config()
        assert (
            "You must provide either a local config path, a valid URL or an S3 URL"
            in str(excinfo.value)
        )

        # Non-existent file -> FileNotFoundError
        with pytest.raises(FileNotFoundError) as excinfo:
            validate_config(config="/tmp/nonexistent")
        assert 'Schema config "/tmp/nonexistent" not found' in str(
            excinfo.value
        )

    @patch("pgsync.urls.logger")
    def test_get_auth(self, mock_logger):
        with patch("pgsync.urls.Plugins", return_value=MagicMock()):
            _get_auth("something")
            mock_logger.assert_not_called()

        with patch("pgsync.urls.Plugins", side_effect=ModuleNotFoundError):
            assert _get_auth("something") is None
