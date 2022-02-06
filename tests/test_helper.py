"""Helper tests."""
import pytest
import sqlalchemy as sa
from mock import patch

from pgsync.helper import teardown


@pytest.mark.usefixtures("table_creator")
class TestHelper(object):
    """Helper tests."""

    @patch("pgsync.helper.logger")
    @patch("pgsync.helper.get_config")
    @patch("pgsync.helper.Sync")
    def test_teardown_with_drop_db(self, mock_sync, mock_config, mock_logger):
        mock_config.return_value = "tests/fixtures/schema.json"
        mock_sync.truncate_schemas.return_value = None
        with patch("pgsync.helper.drop_database") as mock_db:
            teardown(drop_db=True, config="fixtures/schema.json")
            mock_db.assert_called_once()
        mock_logger.warning.assert_not_called()

    @patch("pgsync.sync.ElasticHelper")
    @patch("pgsync.helper.logger")
    @patch("pgsync.helper.get_config")
    def test_teardown_without_drop_db(self, mock_config, mock_logger, mock_es):
        mock_config.return_value = "tests/fixtures/schema.json"
        with patch("pgsync.sync.Sync") as mock_sync:
            mock_sync.truncate_schemas.side_effect = sa.exc.OperationalError
            teardown(drop_db=False, config="fixtures/schema.json")
            mock_logger.warning.assert_called_once()
