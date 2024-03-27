"""Helper tests."""

import pytest
import sqlalchemy as sa
from mock import ANY, call, patch

from pgsync import helper


@pytest.mark.usefixtures("table_creator")
class TestHelper(object):
    """Helper tests."""

    @patch("pgsync.helper.logger")
    @patch("pgsync.helper.get_config")
    @patch("pgsync.helper.Sync")
    def test_teardown_with_drop_db(self, mock_sync, mock_config, mock_logger):
        mock_config.return_value = "tests/fixtures/schema.json"
        mock_sync.truncate_schemas.return_value = None
        with patch("pgsync.helper.database_exists", return_value=True):
            with patch("pgsync.helper.drop_database") as mock_db:
                helper.teardown(drop_db=True, config="fixtures/schema.json")
                assert mock_db.call_args_list == [
                    call(ANY),
                    call(ANY),
                ]

        mock_logger.warning.assert_not_called()

    @patch("pgsync.helper.logger")
    @patch("pgsync.helper.get_config")
    def test_teardown_without_drop_db(self, mock_config, mock_logger):
        mock_config.return_value = "tests/fixtures/schema.json"

        with patch("pgsync.node.Tree.build", return_value=None):
            with patch("pgsync.sync.Sync") as mock_sync:
                mock_sync.tree.build.return_value = None
                mock_sync.truncate_schemas.side_effect = (
                    sa.exc.OperationalError
                )
                helper.teardown(drop_db=False, config="fixtures/schema.json")
                assert mock_logger.warning.call_args_list == [
                    call(ANY),
                    call(ANY),
                ]
