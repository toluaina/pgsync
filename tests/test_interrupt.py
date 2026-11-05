"""Tests for Ctrl+C during a database query (pgsync.base.wait_interruptible)."""

import tempfile
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from psycopg2.extensions import (
    get_wait_callback,
    POLL_OK,
    POLL_READ,
    POLL_WRITE,
    QueryCanceledError,
)

from pgsync.base import interruptible_queries, wait_interruptible
from pgsync.sync import main, Sync


def make_conn(*states):
    conn = MagicMock()
    conn.poll.side_effect = list(states)
    conn.fileno.return_value = 7
    return conn


class TestWaitInterruptible:
    @patch("pgsync.base.select.select")
    def test_waits_until_ok(self, select):
        conn = make_conn(POLL_WRITE, POLL_READ, POLL_OK)
        wait_interruptible(conn)
        assert conn.poll.call_count == 3
        select.assert_any_call([], [7], [])
        select.assert_any_call([7], [], [])
        conn.cancel.assert_not_called()

    @patch("pgsync.base.select.select")
    def test_ctrl_c_cancels_the_query(self, select):
        select.side_effect = [KeyboardInterrupt, None]
        conn = make_conn(POLL_READ, POLL_READ, POLL_OK)
        with pytest.raises(KeyboardInterrupt):
            wait_interruptible(conn)
        conn.cancel.assert_called_once()

    @patch("pgsync.base.select.select")
    def test_cancel_error_becomes_keyboard_interrupt(self, select):
        select.side_effect = KeyboardInterrupt
        conn = make_conn(POLL_READ, QueryCanceledError("user request"))
        with pytest.raises(KeyboardInterrupt):
            wait_interruptible(conn)
        conn.cancel.assert_called_once()

    def test_statement_timeout_is_not_a_keyboard_interrupt(self):
        conn = make_conn(QueryCanceledError("statement timeout"))
        with pytest.raises(QueryCanceledError):
            wait_interruptible(conn)
        conn.cancel.assert_not_called()

    def test_bad_state(self):
        conn = make_conn(99)
        conn.OperationalError = RuntimeError
        with pytest.raises(RuntimeError, match="bad state from poll: 99"):
            wait_interruptible(conn)


class TestInterruptibleQueries:
    def test_set_and_unset(self):
        interruptible_queries()
        assert get_wait_callback() is wait_interruptible
        interruptible_queries(False)
        assert get_wait_callback() is None


class TestWalConsumer:
    def test_consume_sets_the_callback_once(self):
        sync = MagicMock()
        sync._interruptible_after_start = True
        message = MagicMock(payload="")
        with patch("pgsync.sync.interruptible_queries") as callback:
            Sync.consume(sync, message)
            Sync.consume(sync, message)
        callback.assert_called_once_with()
        assert sync._interruptible_after_start is False

    def test_consume_without_flag(self):
        sync = MagicMock()
        sync._interruptible_after_start = False
        with patch("pgsync.sync.interruptible_queries") as callback:
            Sync.consume(sync, MagicMock(payload=""))
        callback.assert_not_called()


@patch("pgsync.sync.show_settings")
@patch("pgsync.sync.validate_config")
@patch("pgsync.sync.config_loader")
@patch("pgsync.sync.Sync")
@patch("pgsync.sync.interruptible_queries")
class TestMain:
    def invoke(self, *args):
        with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
            return CliRunner().invoke(main, ["-c", tmp.name, *args])

    # MySQL changes --daemon to polling mode, which loops forever
    @patch("pgsync.sync.IS_MYSQL_COMPAT", False)
    def test_daemon_sets_the_callback(self, callback, MockSync, loader, *_):
        loader.return_value = [{"database": "db", "index": "idx"}]
        MockSync.return_value.tasks = []
        with patch("pgsync.sync._wait_for_exit") as wait:
            result = self.invoke("-d")
        wait.assert_called_once_with()
        assert result.exit_code == 0, result.output
        callback.assert_called_once_with()

    def test_wal_waits_for_the_stream(self, callback, MockSync, loader, *_):
        loader.return_value = [{"database": "db", "index": "idx"}]
        result = self.invoke("--wal")
        assert result.exit_code == 0, result.output
        callback.assert_not_called()
        assert MockSync.return_value._interruptible_after_start is True

    def test_wal_with_many_streams(self, callback, MockSync, loader, *_):
        loader.return_value = [
            {"database": "db", "index": "a"},
            {"database": "db", "index": "b"},
        ]
        syncs = [MagicMock(_interruptible_after_start=False) for _ in range(2)]
        MockSync.side_effect = syncs
        with patch("threading.Thread.start"):
            result = self.invoke("--wal")
        assert result.exit_code == 0, result.output
        callback.assert_not_called()
        assert all(not s._interruptible_after_start for s in syncs)


class TestWaitForExit:
    @patch("pgsync.sync.os._exit", side_effect=SystemExit)
    @patch("pgsync.sync.monitor_view.stop")
    @patch("pgsync.sync.time.sleep", side_effect=[None, KeyboardInterrupt])
    def test_ctrl_c_exits_without_traceback(self, sleep, stop, _exit, capsys):
        from pgsync.sync import _wait_for_exit

        with pytest.raises(SystemExit):
            _wait_for_exit()
        stop.assert_called_once_with()
        _exit.assert_called_once_with(1)
        assert capsys.readouterr().err == "\nAborted!\n"

    @patch("pgsync.sync._wait_for_exit")
    @patch("pgsync.sync.show_settings")
    @patch("pgsync.sync.validate_config")
    @patch("pgsync.sync.config_loader")
    @patch("pgsync.sync.Sync")
    def test_no_wait_without_daemon(self, MockSync, loader, _v, _s, wait):
        loader.return_value = [{"database": "db", "index": "idx"}]
        with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
            result = CliRunner().invoke(main, ["-c", tmp.name])
        assert result.exit_code == 0, result.output
        wait.assert_not_called()
