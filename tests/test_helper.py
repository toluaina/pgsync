"""Helper tests."""

import pytest
import sqlalchemy as sa
from mock import ANY, call, patch

from pgsync import helper


@pytest.mark.usefixtures("table_creator")
class TestHelper(object):
    """Helper tests."""

    @patch("pgsync.helper.logger")
    @patch("pgsync.helper.validate_config")
    @patch("pgsync.helper.Sync")
    def test_teardown_with_drop_db(
        self, mock_sync, mock_validate_config, mock_logger
    ):

        mock_validate_config.return_value = None
        mock_sync.truncate_schemas.return_value = None

        with patch("pgsync.helper.database_exists", return_value=True):
            with patch("pgsync.helper.drop_database") as mock_drop_db:
                helper.teardown(
                    drop_db=True, config="tests/fixtures/schema.json"
                )

                # Ensure drop_database was called twice
                assert mock_drop_db.call_args_list == [
                    call(ANY),
                    call(ANY),
                ]

        # Ensure no warnings were logged
        mock_logger.warning.assert_not_called()

    @patch("pgsync.helper.logger")
    def test_teardown_without_drop_db(self, mock_logger):

        with patch("pgsync.node.Tree.build", return_value=None):
            with patch("pgsync.sync.Sync") as mock_sync:
                mock_sync.tree.build.return_value = None
                mock_sync.truncate_schemas.side_effect = (
                    sa.exc.OperationalError
                )
                helper.teardown(
                    drop_db=False, config="tests/fixtures/schema.json"
                )
                assert mock_logger.warning.call_args_list == [
                    call(ANY),
                    call(ANY),
                ]
