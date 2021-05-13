"""Env vars tests."""
import pytest


@pytest.mark.usefixtures("table_creator")
class TestEnvVars(object):
    """Env vars tests."""

    def test_envvars(self):
        pass
