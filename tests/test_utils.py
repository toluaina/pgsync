"""Utils tests."""
import pytest
from mock import patch

from pgsync.exc import SchemaError
from pgsync.utils import get_config, show_settings


@pytest.mark.usefixtures("table_creator")
class TestUtils(object):
    """Utils tests."""

    def test_get_config(self):
        with pytest.raises(SchemaError) as excinfo:
            get_config()
        assert "Schema config not set" in str(excinfo.value)

        with pytest.raises(OSError) as excinfo:
            get_config("non_existent")
        assert 'Schema config "non_existent" not found' in str(excinfo.value)
        config = get_config("tests/fixtures/schema.json")
        assert config == "tests/fixtures/schema.json"

    @patch("pgsync.utils.logger")
    def test_show_settings(self, mock_logger):
        show_settings(schema="tests/fixtures/schema.json")
        mock_logger.info.assert_any_call("\033[4mSettings\033[0m:")
        mock_logger.info.assert_any_call(
            f'{"Schema":<10s}: {"tests/fixtures/schema.json"}'
        )
