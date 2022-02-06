"""Sync tests."""
from collections import namedtuple

import pytest
from mock import patch

from pgsync.exc import RDSError, SchemaError
from pgsync.sync import Sync

ROW = namedtuple("Row", ["data", "xid"])


@pytest.mark.usefixtures("table_creator")
class TestSync(object):
    """Sync tests."""

    def test_logical_slot_changes(self, sync):
        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW("BEGIN: blah", 1234),
            ]
            with patch("pgsync.sync.Sync.sync") as mock_sync:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW("COMMIT: blah", 1234),
            ]
            with patch("pgsync.sync.Sync.sync") as mock_sync:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW(
                    "table public.book: INSERT: id[integer]:10 isbn[character "
                    "varying]:'888' title[character varying]:'My book title' "
                    "description[character varying]:null copyright[character "
                    "varying]:null tags[jsonb]:null publisher_id[integer]:null",
                    1234,
                ),
            ]
            with patch(
                "pgsync.sync.Sync.logical_slot_get_changes"
            ) as mock_get:
                with patch("pgsync.sync.Sync.sync") as mock_sync:
                    sync.logical_slot_changes()
                    mock_peek.assert_called_once_with(
                        "testdb_testdb",
                        txmin=None,
                        txmax=None,
                        upto_nchanges=None,
                    )
                    mock_get.assert_called_once()
                    mock_sync.assert_called_once()
        sync.es.close()

    @patch("pgsync.sync.ElasticHelper")
    def test_sync_validate(self, mock_es):
        with pytest.raises(SchemaError) as excinfo:
            sync = Sync(
                document={
                    "index": "testdb",
                    "nodes": ["foo"],
                },
                verbose=False,
                validate=True,
                repl_slots=False,
            )
        assert "Incompatible schema. Please run v2 schema migration" in str(
            excinfo.value
        )

        sync = Sync(
            document={
                "index": "testdb",
                "nodes": {"table": "book"},
                "plugins": ["Hero"],
            },
            verbose=False,
            validate=True,
            repl_slots=False,
        )

        def _side_effect(*args, **kwargs):
            if args[0] == 0:
                return 0
            elif args[0] == "max_replication_slots":
                raise RuntimeError(
                    "Ensure there is at least one replication slot defined "
                    "by setting max_replication_slots=1"
                )

            elif args[0] == "wal_level":
                raise RuntimeError(
                    "Enable logical decoding by setting wal_level=logical"
                )
            elif args[0] == "rds_logical_replication":
                raise RDSError("rds.logical_replication is not enabled")
            else:
                return args[0]

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("max_replication_slots"),
            ):
                sync = Sync(
                    document={
                        "index": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert (
            "Ensure there is at least one replication slot defined "
            "by setting max_replication_slots=1" in str(excinfo.value)
        )

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("wal_level"),
            ):
                sync = Sync(
                    document={
                        "index": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert "Enable logical decoding by setting wal_level=logical" in str(
            excinfo.value
        )

        with pytest.raises(RDSError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("rds_logical_replication"),
            ):
                sync = Sync(
                    document={
                        "index": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert "rds.logical_replication is not enabled" in str(excinfo.value)
