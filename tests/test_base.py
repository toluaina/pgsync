"""Base tests."""
import pytest


@pytest.mark.usefixtures('table_creator')
class TestBase(object):
    """Base tests."""

    pass
