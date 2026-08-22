"""Unit tests for the notification-to-Redis producer paths."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pgsync.constants import PRIMARY_KEY_DELIMITER
from pgsync.sync import Sync


def make_sync():
    return SimpleNamespace(
        database="books",
        index="books-index",
        tree=SimpleNamespace(schemas={"public"}),
        redis=Mock(),
        lock=nullcontext(),
        count={"db": 0},
    )


def test_poll_db_once_flushes_buffer_when_notification_wait_times_out():
    sync = make_sync()
    conn = Mock()

    with patch("pgsync.sync.select.select", return_value=([], [], [])):
        assert Sync._poll_db_once(sync, conn, [{"id": 1}]) == []

    sync.redis.push.assert_called_once_with([{"id": 1}])
    conn.poll.assert_not_called()


@patch("pgsync.sync.logger")
def test_poll_db_once_keeps_only_valid_matching_notifications(mock_logger):
    sync = make_sync()
    conn = SimpleNamespace(
        poll=Mock(),
        notifies=[
            SimpleNamespace(
                channel="books",
                payload='{"indices": ["books-index"], "schema": "public"}',
            ),
            SimpleNamespace(channel="books", payload="not-json"),
            SimpleNamespace(
                channel="other",
                payload='{"indices": ["books-index"], "schema": "public"}',
            ),
        ],
    )

    with patch("pgsync.sync.select.select", return_value=([conn], [], [])):
        payloads = Sync._poll_db_once(sync, conn, [])

    assert payloads == [{"indices": ["books-index"], "schema": "public"}]
    assert sync.count["db"] == 1
    mock_logger.exception.assert_called_once()


@patch("pgsync.sync.logger")
def test_async_poll_db_pushes_valid_notification(mock_logger):
    sync = make_sync()
    sync.conn = SimpleNamespace(
        poll=Mock(),
        notifies=[
            SimpleNamespace(
                channel="books",
                payload='{"indices": ["books-index"], "schema": "public"}',
            )
        ],
    )

    Sync.async_poll_db(sync)

    sync.redis.push.assert_called_once_with(
        [{"indices": ["books-index"], "schema": "public"}]
    )
    assert sync.count["db"] == 1
    mock_logger.debug.assert_called_once()


def test_truncate_root_deletes_every_matching_document():
    search_client = Mock()
    search_client._search.return_value = ["1", "2"]
    search_client.prepare_action.side_effect = lambda doc: doc
    sync = SimpleNamespace(
        index="books-index",
        search_client=search_client,
    )
    node = SimpleNamespace(is_root=True, table="books")

    assert Sync._truncate_op(sync, node, {"books": []}) == {"books": []}

    search_client.bulk.assert_called_once_with(
        "books-index",
        [
            {"_id": "1", "_index": "books-index", "_op_type": "delete"},
            {"_id": "2", "_index": "books-index", "_op_type": "delete"},
        ],
    )


@patch("pgsync.sync.logger")
def test_truncate_child_returns_root_filters_and_skips_malformed_ids(
    mock_logger,
):
    search_client = Mock()
    search_client._search.return_value = [
        f"1{PRIMARY_KEY_DELIMITER}2",
        "malformed",
    ]
    sync = SimpleNamespace(
        index="books-index",
        search_client=search_client,
        tree=SimpleNamespace(
            root=SimpleNamespace(
                table="books",
                model=SimpleNamespace(primary_keys=["id", "tenant_id"]),
            )
        ),
    )
    node = SimpleNamespace(is_root=False, table="authors")

    filters = Sync._truncate_op(sync, node, {"books": []})

    assert filters == {"books": [{"id": "1", "tenant_id": "2"}]}
    mock_logger.warning.assert_called_once()
