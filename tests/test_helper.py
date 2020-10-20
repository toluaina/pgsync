"""Helper tests."""
import pytest
from mock import patch

from pgsync.helper import teardown


@pytest.mark.usefixtures('table_creator')
class TestHelper(object):
    """Helper tests."""

    @patch('pgsync.helper.get_config')
    @patch('pgsync.helper.Sync')
    def test_teardown(self, mock_sync, mock_config):
        mock_config.return_value = 'tests/fixtures/schema.json'
        mock_sync.truncate_schemas.return_value = None
        teardown(drop_db=False, config='fixtures/schema.json')
