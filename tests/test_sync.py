"""Node tests."""
from collections import namedtuple

import pytest
from mock import patch

ROW = namedtuple('Row', ['data', 'xid'])


@pytest.mark.usefixtures('table_creator')
class TestSync(object):
    """Sync tests."""

    def test_logical_slot_changes(self, sync):
        with patch('pgsync.sync.Sync.logical_slot_peek_changes') as mock_peek:
            mock_peek.return_value = [
                ROW('BEGIN: blah', 1234),
            ]
            with patch('pgsync.sync.Sync.sync_payloads') as mock_sync_payloads:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    'testdb_testdb',
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync_payloads.assert_not_called()

        with patch('pgsync.sync.Sync.logical_slot_peek_changes') as mock_peek:
            mock_peek.return_value = [
                ROW('COMMIT: blah', 1234),
            ]
            with patch('pgsync.sync.Sync.sync_payloads') as mock_sync_payloads:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    'testdb_testdb',
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync_payloads.assert_not_called()

        with patch('pgsync.sync.Sync.logical_slot_peek_changes') as mock_peek:
            mock_peek.return_value = [
                ROW(
                    "table public.book: INSERT: id[integer]:10 isbn[character "
                    "varying]:'888' title[character varying]:'My book title' "
                    "description[character varying]:null copyright[character "
                    "varying]:null tags[jsonb]:null publisher_id[integer]:null",
                    1234
                ),
            ]
            with patch('pgsync.sync.Sync.logical_slot_get_changes') as mock_get:
                with patch('pgsync.sync.Sync.sync_payloads') as mock_sync_payloads:
                    sync.logical_slot_changes()
                    mock_peek.assert_called_once_with(
                        'testdb_testdb',
                        txmin=None,
                        txmax=None,
                        upto_nchanges=None,
                    )
                    mock_get.assert_called_once()
                    mock_sync_payloads.assert_called_once()
