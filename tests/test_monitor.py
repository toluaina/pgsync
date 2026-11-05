"""Tests for the terminal monitor (pgsync --monitor)."""

import io
import sys
import tempfile
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest
from click.testing import CliRunner

from pgsync import monitor
from pgsync.base import Payload
from pgsync.constants import DELETE, INSERT, TRUNCATE, UPDATE
from pgsync.sync import main


def make_sync(index="book_index", consumer=True, pk=("id",)):
    sync = MagicMock()
    sync.database = "book"
    sync.index = index
    sync.consumer = consumer
    sync.count = {"xlog": 10, "db": 7, "redis": 0}
    sync.search_client.doc_count = 5
    sync.search_client.name = "OpenSearch"
    sync.slot_name = "book_book_index"
    sync.replication_slot_lag.return_value = None
    sync.search_client.count.return_value = 42
    sync.estimated_count.return_value = None
    sync._redis = None
    sync._checkpoint = 1234
    sync.tree.get_node.return_value.model.primary_keys = list(pk)
    return sync


@pytest.fixture(autouse=True)
def no_global_view():
    monitor.VIEW = None
    yield
    monitor.VIEW = None
    monitor.set_palette(monitor.PALETTES["default"])


@pytest.fixture
def classic():
    monitor.set_palette(monitor.palette("classic"))


class TestHelpers:
    def test_helpers_do_nothing_when_off(self):
        sync = make_sync()
        assert monitor.active() is False
        monitor.record(sync, [Payload(tg_op=INSERT, table="book")])
        assert monitor.progress(sync, 1, 2) is False
        monitor.register(sync, "WAL")
        monitor.stop()

    def test_helpers_feed_the_view(self):
        sync = make_sync()
        view = monitor.MonitorView()
        monitor.VIEW = view
        assert monitor.active() is True
        monitor.register(sync, "WAL")
        monitor.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 1}
                )
            ],
        )
        assert monitor.progress(sync, 1, 2) is True
        assert view.syncs == [(sync, "WAL")]
        assert view.by_table["public.book"][INSERT] == 1
        assert view._progress["book_index"][:2] == (1, 2)


class TestRecord:
    def test_counts_by_table_and_op(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 1}
                ),
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 2}
                ),
                Payload(
                    tg_op=DELETE, table="book", schema="public", old={"id": 3}
                ),
            ],
        )
        assert view.by_table["public.book"][INSERT] == 2
        assert view.by_table["public.book"][DELETE] == 1
        assert view._events_total == 3

    def test_newest_event_first(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 1}
                ),
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 2}
                ),
            ],
        )
        assert [e.pk for e in view.events] == ["id=2", "id=1"]

    def test_event_list_is_bounded(self):
        sync = make_sync()
        view = monitor.MonitorView(max_events=3)
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": i}
                )
                for i in range(10)
            ],
        )
        assert len(view.events) == 3
        assert view._events_total == 10

    def test_pause_keeps_counting(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.paused = True
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 1}
                )
            ],
        )
        assert len(view.events) == 0
        assert view.by_table["public.book"][INSERT] == 1

    def test_primary_key_uses_tree(self):
        sync = make_sync(pk=("book_id", "author_id"))
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT,
                    table="book_author",
                    schema="public",
                    new={"id": 9, "book_id": 1, "author_id": 2},
                )
            ],
        )
        assert view.events[0].pk == "book_id=1 author_id=2"

    def test_primary_key_fallback(self):
        sync = make_sync()
        sync.tree.get_node.side_effect = RuntimeError("not found")
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT,
                    table="x",
                    schema="public",
                    new={"uuid": "abc", "name": "n"},
                )
            ],
        )
        assert view.events[0].pk == "uuid=abc"

    def test_primary_key_lookup_is_cached(self):
        sync = make_sync()
        view = monitor.MonitorView()
        payloads = [
            Payload(tg_op=INSERT, table="book", schema="public", new={"id": i})
            for i in range(5)
        ]
        view.record(sync, payloads)
        assert sync.tree.get_node.call_count == 1

    def test_truncate_has_no_pk(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(
            sync, [Payload(tg_op=TRUNCATE, table="book", schema="public")]
        )
        assert view.events[0].pk == ""
        assert view.events[0].xmin is None

    def test_changed_columns(self):
        payload = Payload(
            tg_op=UPDATE,
            table="book",
            schema="public",
            old={"id": 1, "title": "a", "price": 1},
            new={"id": 1, "title": "b", "price": 1, "isbn": "x"},
        )
        assert monitor.MonitorView._changed_columns(payload) == ["title"]

    def test_changed_columns_without_old_row(self):
        payload = Payload(
            tg_op=UPDATE, table="book", schema="public", new={"id": 1}
        )
        assert monitor.MonitorView._changed_columns(payload) == []


class TestMessages:
    def test_errors_are_counted(self):
        view = monitor.MonitorView()
        view.message("WARNING", "slow")
        view.message("ERROR", "failed")
        view.message("CRITICAL", "down")
        assert view.errors == 2
        assert view.messages[0][1:] == ("CRITICAL", "down", 1)

    def test_stream_capture_splits_lines(self):
        view = monitor.MonitorView()
        stream = monitor._StreamCapture(view, "STDERR")
        stream.write("one\ntw")
        stream.write("o\n\rprogress 1\rprogress 2\n")
        assert [m[2] for m in view.messages] == ["progress 2", "two", "one"]

    def test_stdout_is_discarded(self):
        view = monitor.MonitorView()
        stream = monitor._StreamCapture(view, None)
        assert stream.write("|- public.book\n") == len("|- public.book\n")
        assert not view.messages

    def test_capture_sends_stderr_to_messages(self):
        view = monitor.MonitorView()
        view._capture_output()
        try:
            print("tree line")
            print("Traceback line", file=sys.stderr)
        finally:
            view._restore_output()
        assert [m[1:] for m in view.messages] == [
            ("STDERR", "Traceback line", 1)
        ]

    def test_capture_and_restore_output(self):
        import logging

        view = monitor.MonitorView()
        root = logging.getLogger()
        before = list(root.handlers)
        stdout, stderr = sys.stdout, sys.stderr
        out = view._capture_output()
        try:
            assert out is stdout
            assert isinstance(sys.stdout, monitor._StreamCapture)
            logging.getLogger("pgsync.test").error("boom")
            assert view.errors == 1
        finally:
            view._restore_output()
        assert sys.stdout is stdout
        assert sys.stderr is stderr
        assert root.handlers == before


class TestSample:
    def test_rates(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.syncs = [(sync, "WAL")]
        with patch("pgsync.monitor.time.time", return_value=100.0):
            view.sample()
        view._events_total = 30
        sync.search_client.doc_count = 25
        with patch("pgsync.monitor.time.time", return_value=101.0):
            view.sample()
        assert view.event_rate[-1] == 30
        assert view.doc_rate[-1] == 20

    def test_gap_fills_one_sample_per_second(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.syncs = [(sync, "WAL")]
        with patch("pgsync.monitor.time.time", return_value=100.0):
            view.sample()
        view._events_total = 30
        with patch("pgsync.monitor.time.time", return_value=103.0):
            view.sample()
        assert list(view.event_rate)[-3:] == [10, 10, 10]
        assert len(view.event_rate) == monitor.HISTORY


class TestRender:
    def render(self, view):
        from rich.console import Console

        console = Console(width=140, record=True, file=io.StringIO())
        console.print(view.render())
        return console.export_text()

    def test_render(self):
        pytest.importorskip("rich")
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(
            sync,
            [
                Payload(
                    tg_op=UPDATE,
                    table="book",
                    schema="public",
                    old={"id": 1, "title": "a"},
                    new={"id": 1, "title": "b"},
                    xmin=884213,
                )
            ],
        )
        view.progress(sync, 50, 200)
        view.message("ERROR", "failed to index")
        text = self.render(view)
        assert "book:book_index" in text
        assert "WAL" in text
        assert "public.book" in text
        assert "xmin=884213" in text
        assert "title" in text
        assert "catch-up" in text
        assert "25.0%" in text
        assert "failed to index" in text
        assert "1234" in text  # checkpoint

    def test_render_redis_queue(self):
        pytest.importorskip("rich")
        sync = make_sync()
        sync._redis = Mock(qsize=312)
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        view.probe()
        assert "312" in self.render(view)

    def test_render_redis_failure(self):
        pytest.importorskip("rich")
        sync = make_sync()
        sync._redis = Mock()
        type(sync._redis).qsize = PropertyMock(
            side_effect=ConnectionError("down")
        )
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        assert "book:book_index" in self.render(view)

    def test_render_producer_only_note(self):
        pytest.importorskip("rich")
        sync = make_sync(consumer=False)
        view = monitor.MonitorView()
        view.register(sync, "trigger producer")
        assert "producer only" in self.render(view)


@patch("pgsync.sync.show_settings")
@patch("pgsync.sync.validate_config")
@patch("pgsync.sync.config_loader")
@patch("pgsync.sync.Sync")
class TestCli:
    def invoke(self, *args):
        with tempfile.NamedTemporaryFile(suffix=".json") as tmp:
            return CliRunner().invoke(main, ["-c", tmp.name, *args])

    def test_needs_daemon_or_wal(self, MockSync, *_):
        result = self.invoke("--monitor")
        assert result.exit_code == 2
        assert "--monitor needs --daemon or --wal" in result.output
        MockSync.assert_not_called()

    def test_rejects_polling(self, MockSync, *_):
        result = self.invoke("--monitor", "--polling")
        assert result.exit_code == 2
        assert "polling mode" in result.output

    @patch("pgsync.sync.IS_MYSQL_COMPAT", True)
    def test_rejects_mysql_daemon(self, MockSync, *_):
        result = self.invoke("--monitor", "-d")
        assert result.exit_code == 2
        assert "MySQL and MariaDB" in result.output

    @patch("pgsync.monitor.available", return_value=False)
    def test_needs_rich(self, _available, MockSync, *_):
        result = self.invoke("--monitor", "--wal")
        assert result.exit_code == 2
        assert "pgsync[monitor]" in result.output

    def test_needs_terminal(self, MockSync, *_):
        # CliRunner output is not a terminal
        result = self.invoke("--monitor", "--wal")
        assert result.exit_code == 2
        assert "interactive terminal" in result.output

    @patch("pgsync.monitor.start")
    @patch("pgsync.monitor.register")
    def test_wal_registers_syncs(
        self, register, start, MockSync, mock_loader, *_
    ):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        with patch("pgsync.monitor.interactive", return_value=True):
            result = self.invoke("--monitor", "--wal")
        assert result.exit_code == 0, result.output
        start.assert_called_once()
        register.assert_called_once_with(MockSync.return_value, "WAL")

    @patch("pgsync.sync.IS_MYSQL_COMPAT", False)
    @patch("pgsync.monitor.start")
    @patch("pgsync.monitor.register")
    def test_daemon_producer_mode(
        self, register, start, MockSync, mock_loader, *_
    ):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        MockSync.return_value.tasks = []
        with (
            patch("pgsync.monitor.interactive", return_value=True),
            patch("pgsync.sync._wait_for_exit"),
        ):
            result = self.invoke("--monitor", "-d", "--producer")
        assert result.exit_code == 0, result.output
        register.assert_called_once_with(
            MockSync.return_value, "trigger producer"
        )

    @patch("pgsync.monitor.stop")
    @patch("pgsync.monitor.start")
    def test_stops_view_on_error(self, start, stop, MockSync, mock_loader, *_):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        MockSync.return_value.wal_consumer.side_effect = RuntimeError("boom")
        with patch("pgsync.monitor.interactive", return_value=True):
            result = self.invoke("--monitor", "--wal")
        assert isinstance(result.exception, RuntimeError)
        stop.assert_called_once()

    @patch("pgsync.sync.Timer")
    @patch("pgsync.monitor.active", return_value=True)
    @patch("pgsync.monitor.start")
    def test_no_timer_line_in_monitor_mode(
        self, start, active, Timer, MockSync, mock_loader, *_
    ):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        with patch("pgsync.monitor.interactive", return_value=True):
            result = self.invoke("--monitor", "--wal")
        assert result.exit_code == 0, result.output
        Timer.assert_not_called()

    @patch("pgsync.sync.Timer")
    def test_timer_line_without_monitor(
        self, Timer, MockSync, mock_loader, *_
    ):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        result = self.invoke("--wal")
        assert result.exit_code == 0, result.output
        Timer.assert_called_once()

    def test_without_monitor_does_not_start_view(
        self, MockSync, mock_loader, *_
    ):
        mock_loader.return_value = [{"database": "db", "index": "idx"}]
        with patch("pgsync.monitor.start") as start:
            result = self.invoke("--wal")
        assert result.exit_code == 0, result.output
        start.assert_not_called()


class TestEventList:
    def events(self):
        sync = make_sync(pk=("book_id", "author_id"))
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=UPDATE,
                    table="book_author",
                    schema="public",
                    old={"book_id": 3150, "author_id": 679, "rank": 1},
                    new={"book_id": 3150, "author_id": 679, "rank": 2},
                    xmin=884213,
                )
            ],
        )
        return monitor._EventList(list(view.events))

    def render(self, width):
        from rich.console import Console

        console = Console(width=width, record=True, file=io.StringIO())
        console.print(self.events())
        return console.export_text()

    def test_wide_terminal_shows_everything(self):
        text = self.render(120)
        assert "public.book_author" in text
        assert "book_id=3150 author_id=679" in text
        assert "xmin=884213" in text
        assert "rank" in text

    def test_narrow_terminal_keeps_time_and_xmin(self):
        text = self.render(60)
        assert all(len(line) <= 60 for line in text.splitlines())
        assert "xmin=884213" in text
        assert "UPDATE" in text
        assert "…" in text

    def test_widths_fit(self):
        event_list = self.events()
        for total in (60, 80, 120):
            table, pk, xmin, changed = event_list.widths(total)
            used = 12 + 8 + table + pk + xmin + changed + 5 * 2
            assert used <= total

    def test_no_events(self):
        assert monitor._EventList([]).widths(80)[3] >= 0


def book_payloads(count, tg_op=INSERT, table="book"):
    return [
        Payload(tg_op=tg_op, table=table, schema="public", new={"id": i})
        for i in range(count)
    ]


class TestRecordCost:
    def test_builds_events_only_for_the_kept_payloads(self):
        sync = make_sync()
        view = monitor.MonitorView(max_events=3)
        with patch.object(
            monitor.MonitorView, "_format_pk", wraps=view._format_pk
        ) as format_pk:
            view.record(sync, book_payloads(1000))
        assert format_pk.call_count == 3
        assert [e.pk for e in view.events] == ["id=999", "id=998", "id=997"]
        assert view.by_table["public.book"][INSERT] == 1000
        assert view._events_total == 1000

    def test_paused_builds_no_events(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.paused = True
        with patch.object(monitor.MonitorView, "_format_pk") as format_pk:
            view.record(sync, book_payloads(10))
        format_pk.assert_not_called()
        assert view.by_table["public.book"][INSERT] == 10

    def test_last_event_time(self):
        sync = make_sync()
        view = monitor.MonitorView()
        assert view.last_event_at is None
        view.record(sync, [])
        assert view.last_event_at is None
        view.record(sync, book_payloads(1))
        assert view.last_event_at is not None
        assert view._last_event["book_index"] == view.last_event_at


class TestSkipped:
    def test_skipped_event_is_marked(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(1, table="book_tag"), skipped=True)
        assert view.events[0].skipped is True
        assert view.by_table["public.book_tag"][INSERT] == 1

    def test_skipped_event_render(self):
        from rich.console import Console

        event = monitor.Event(
            INSERT, "public.book_tag", "id=1", 7, [], skipped=True
        )
        console = Console(width=120, record=True, file=io.StringIO())
        console.print(monitor._EventList([event]))
        assert "skipped" in console.export_text()

    def test_sync_payloads_marks_unknown_table(self):
        from pgsync.sync import Sync

        sync = MagicMock()
        sync.tree.tables = {"book"}
        sync.tree.schemas = {"public"}
        with patch("pgsync.sync.monitor_view.record") as record:
            docs = list(
                Sync._payloads(sync, book_payloads(2, table="book_tag"))
            )
        assert docs == []
        record.assert_called_once()
        assert record.call_args.kwargs == {"skipped": True}


class TestRedisCache:
    def test_render_does_not_read_the_queue(self):
        sync = make_sync()
        sync._redis = MagicMock()
        qsize = PropertyMock(return_value=5)
        type(sync._redis).qsize = qsize
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        for _ in range(4):
            view.render()
            view.sample()
        assert qsize.call_count == 0

    def test_probe_refreshes_the_queue(self):
        sync = make_sync()
        sync._redis = MagicMock()
        type(sync._redis).qsize = PropertyMock(side_effect=[5, 9])
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        assert view._counts(sync)["redis"] is None
        view.probe()
        assert view._counts(sync)["redis"] == 5
        view.probe()
        assert view._counts(sync)["redis"] == 9

    def test_wal_mode_has_no_queue(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.sample()
        view.register(sync, "WAL")
        assert view._counts(sync)["redis"] is None


class TestHealth:
    def states(self, view, sync):
        return view._states([(sync, "trigger", view._counts(sync))])

    def test_healthy(self, classic):
        sync = make_sync()
        view = monitor.MonitorView()
        assert view._health(self.states(view, sync)) == (
            "healthy",
            "bold green",
        )

    def test_backlog(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view._queues["book_index"] = monitor.QUEUE_WARN
        assert view._health(self.states(view, sync))[0] == "backlog"

    def test_recent_error(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.message("ERROR", "failed")
        assert view._health(self.states(view, sync))[0] == "errors"
        view.last_error_at -= monitor.RECENT_ERROR + 1
        assert view._health(self.states(view, sync))[0] == "healthy"

    def test_stale_checkpoint(self):
        sync = make_sync()
        view = monitor.MonitorView()
        changed_at = 1000.0
        view._checkpoints["book:book_index"] = (sync._checkpoint, changed_at)
        view._last_event["book_index"] = changed_at + 30
        with patch("pgsync.monitor.time.time", return_value=changed_at + 120):
            states = self.states(view, sync)
        assert states[0]["stale"] is True
        assert view._health(states)[0] == "checkpoint stale"

    def test_old_checkpoint_without_events_is_not_stale(self):
        sync = make_sync()
        view = monitor.MonitorView()
        changed_at = 1000.0
        view._checkpoints["book:book_index"] = (sync._checkpoint, changed_at)
        view._last_event["book_index"] = changed_at - 5
        with patch("pgsync.monitor.time.time", return_value=changed_at + 120):
            assert self.states(view, sync)[0]["stale"] is False

    def test_queue_style(self, classic):
        assert monitor.MonitorView._queue_style(None) == "cyan"
        assert (
            monitor.MonitorView._queue_style(monitor.QUEUE_WARN - 1) == "cyan"
        )
        assert (
            monitor.MonitorView._queue_style(monitor.QUEUE_WARN)
            == "bold yellow"
        )
        assert (
            monitor.MonitorView._queue_style(monitor.QUEUE_ALERT) == "bold red"
        )

    def test_header_shows_health(self):
        from rich.console import Console

        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        console = Console(width=120, record=True, file=io.StringIO())
        console.print(view.render())
        assert "● healthy" in console.export_text()


class TestFillHeight:
    def render(self, height):
        from rich.console import Console

        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(sync, book_payloads(150))
        console = Console(
            width=100, height=height, record=True, file=io.StringIO()
        )
        console.print(view.render(console))
        lines = console.export_text().splitlines()
        return lines, sum("xmin" in line or "id=" in line for line in lines)

    def test_taller_terminal_shows_more_events(self):
        _, short = self.render(30)
        _, tall = self.render(60)
        assert tall == short + 30

    def test_panel_fits_the_terminal(self):
        for height in (24, 40, 80):
            lines, _ = self.render(height)
            assert len(lines) == height

    def test_tiny_terminal_shows_one_event(self):
        _, rows = self.render(5)
        assert rows == 1

    def test_without_console_shows_default(self):
        from rich.console import Console

        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(50))
        console = Console(width=100, record=True, file=io.StringIO())
        console.print(view.render())
        text = console.export_text()
        assert sum("id=" in line for line in text.splitlines()) == (
            monitor.VISIBLE_EVENTS
        )


class TestLastEvent:
    def test_throughput_shows_last_event(self):
        from rich.console import Console

        sync = make_sync()
        view = monitor.MonitorView()
        console = Console(width=120, record=True, file=io.StringIO())
        console.print(view._throughput())
        assert "none" in console.export_text()
        view.record(sync, book_payloads(1))
        view.last_event_at -= 12
        console = Console(width=120, record=True, file=io.StringIO())
        console.print(view._throughput())
        assert "12s ago" in console.export_text()


def text_of(renderable, width=120, height=None):
    from rich.console import Console

    console = Console(
        width=width, height=height, record=True, file=io.StringIO()
    )
    console.print(renderable)
    return console.export_text()


class TestTableLimit:
    def view_with_tables(self, count):
        view = monitor.MonitorView()
        for i in range(count):
            view.by_table[f"public.t{i:02d}"][INSERT] = count - i
        return view

    def test_all_tables_fit(self):
        view = self.view_with_tables(3)
        text = text_of(view._tables(limit=10))
        assert "public.t02" in text
        assert "more tables" not in text

    def test_extra_tables_are_summed_up(self):
        view = self.view_with_tables(40)
        text = text_of(view._tables(limit=10))
        assert "public.t08" in text  # 9 busiest tables
        assert "public.t09" not in text
        assert "… 31 more tables" in text

    def test_render_limit_follows_the_terminal_height(self):
        from rich.console import Console

        view = self.view_with_tables(40)
        console = Console(
            width=120, height=40, record=True, file=io.StringIO()
        )
        console.print(view.render(console))
        text = console.export_text()
        # height 40: 40 // 4 = 10 rows, 9 tables and one summary row
        assert "… 31 more tables" in text


class TestRepeatedMessages:
    def test_same_message_is_counted(self):
        view = monitor.MonitorView()
        for _ in range(3):
            view.message("WARNING", "retry")
        assert len(view.messages) == 1
        assert view.messages[0][1:] == ("WARNING", "retry", 3)

    def test_repeated_message_moves_to_the_top(self):
        view = monitor.MonitorView()
        view.message("WARNING", "a")
        view.message("ERROR", "b")
        view.message("WARNING", "a")
        assert [m[2:] for m in view.messages] == [("a", 2), ("b", 1)]

    def test_same_text_other_level_is_not_joined(self):
        view = monitor.MonitorView()
        view.message("WARNING", "x")
        view.message("ERROR", "x")
        assert len(view.messages) == 2

    def test_repeated_errors_are_all_counted(self):
        view = monitor.MonitorView()
        for _ in range(5):
            view.message("ERROR", "boom")
        assert view.errors == 5

    def test_count_shows(self):
        view = monitor.MonitorView()
        for _ in range(14):
            view.message("WARNING", "retry")
        assert "retry  ×14" in text_of(view._messages())


class TestFilter:
    def test_cycle_busiest_first_then_all(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(5, table="book"))
        view.record(sync, book_payloads(2, table="author"))
        seen = []
        for _ in range(3):
            view.next_filter()
            seen.append(view.filter)
        assert seen == ["public.book", "public.author", None]

    def test_no_tables(self):
        view = monitor.MonitorView()
        view.next_filter()
        assert view.filter is None

    def test_quiet_table_keeps_its_events(self):
        sync = make_sync()
        view = monitor.MonitorView(max_events=3)
        view.record(sync, book_payloads(1, table="author"))
        view.record(sync, book_payloads(10, table="book"))
        assert all(e.table == "public.book" for e in view.events)
        view.filter = "public.author"
        text = text_of(view.render())
        assert "table: public.author" in text
        assert "public.author  id=0" in text
        assert "public.book    id=" not in text

    def test_key_hint(self):
        view = monitor.MonitorView()
        assert "f filter" in text_of(view.render())


class TestSlotLag:
    def test_probe_reads_lag_once_per_interval(self):
        sync = make_sync()
        sync.replication_slot_lag.return_value = 2048
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        with patch("pgsync.monitor.time.time", return_value=1000.0):
            view.probe()
            view.probe()
        sync.replication_slot_lag.assert_called_once_with("book_book_index")
        assert view._counts(sync)["lag"] == 2048
        with patch(
            "pgsync.monitor.time.time",
            return_value=1000.0 + monitor.LAG_INTERVAL,
        ):
            view.probe()
        assert sync.replication_slot_lag.call_count == 2

    def test_lag_failure_shows_dash(self):
        sync = make_sync()
        sync.replication_slot_lag.side_effect = RuntimeError("standby")
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.probe()
        assert view._counts(sync)["lag"] is None

    def test_bytes(self):
        assert monitor.MonitorView._bytes(0) == "0 B"
        assert monitor.MonitorView._bytes(1536) == "1.5 KB"
        assert monitor.MonitorView._bytes(3 * 1024**2) == "3.0 MB"
        assert monitor.MonitorView._bytes(5 * 1024**4) == "5120.0 GB"

    def test_lag_style(self, classic):
        style = monitor.MonitorView._lag_style
        assert style(None) == ""
        assert style(monitor.LAG_WARN - 1) == ""
        assert style(monitor.LAG_WARN) == "bold yellow"
        assert style(monitor.LAG_ALERT) == "bold red"

    def test_health(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view._lags["book_index"] = monitor.LAG_WARN
        states = view._states([(sync, "WAL", view._counts(sync))])
        assert view._health(states)[0] == "slot lag"

    def test_render(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view._lags["book_index"] = 3 * 1024**2
        text = text_of(view.render())
        assert "slot lag" in text
        assert "3.0 MB" in text

    def test_base_query(self):
        from pgsync.base import Base

        base = MagicMock()
        base.fetchone.return_value = (4096,)
        assert Base.replication_slot_lag(base, "slot") == 4096
        base.fetchone.return_value = None
        assert Base.replication_slot_lag(base, "slot") is None
        base.fetchone.return_value = (None,)
        assert Base.replication_slot_lag(base, "slot") is None


class TestEngineColumn:
    def test_engine_name(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        assert "opensearch" not in text_of(view.render())
        view.probe()
        assert "opensearch" in text_of(view.render())

    def test_mixed_engines(self):
        first, second = make_sync(index="a"), make_sync(index="b")
        second.search_client.name = "Elasticsearch"
        view = monitor.MonitorView()
        view.register(first, "WAL")
        view.register(second, "WAL")
        view.probe()
        text = text_of(view.render())
        assert "opensearch" not in text
        assert " indexed" in text


class TestSpeed:
    def test_top_sections_are_measured_once_per_layout(self):
        from rich.console import Console

        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(sync, book_payloads(5))
        from rich.console import Group

        console = Console(width=100, height=40, file=io.StringIO())
        measured = []
        render_lines = console.render_lines

        def count(renderable, *args, **kwargs):
            # rich also calls render_lines for each cell: count the top only
            if isinstance(renderable, Group):
                measured.append(renderable)
            return render_lines(renderable, *args, **kwargs)

        with patch.object(console, "render_lines", side_effect=count):
            for _ in range(5):
                view.render(console)
            assert len(measured) == 1
            view.message("WARNING", "new row in MESSAGES")
            view.render(console)
            assert len(measured) == 2

    def test_signature(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        first = view.signature()
        assert view.signature() == first
        view.record(sync, book_payloads(1))
        assert view.signature() != first
        second = view.signature()
        view.paused = True
        assert view.signature() != second


class TestIndexName:
    def test_same_database_and_index(self):
        sync = make_sync(index="book")
        view = monitor.MonitorView()
        states = view._states([(sync, "WAL", view._counts(sync))])
        assert states[0]["name"] == "book"

    def test_different_database_and_index(self):
        sync = make_sync(index="book_index")
        view = monitor.MonitorView()
        states = view._states([(sync, "WAL", view._counts(sync))])
        assert states[0]["name"] == "book:book_index"


class TestTrend:
    def test_arrows(self):
        trend = monitor.MonitorView._trend
        assert trend([]).plain == " "
        assert trend([5]).plain == " "
        assert trend([5, None, 5]).plain == " "
        assert trend([1, 2, 9]).plain == "↑"
        assert trend([9, 2, 1]).plain == "↓"
        assert trend([None, None]).plain == " "

    def test_queue_arrow_in_pipeline(self):
        sync = make_sync()
        sync._redis = MagicMock()
        type(sync._redis).qsize = PropertyMock(side_effect=[10, 50])
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        view.probe()
        view.probe()
        assert "50 ↑" in text_of(view.render())

    def test_queue_trend_window(self):
        sync = make_sync()
        sync._redis = MagicMock()
        sizes = [1] + [5] * (monitor.TREND_SECONDS + 1)
        type(sync._redis).qsize = PropertyMock(side_effect=sizes)
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        for _ in sizes:
            view.probe()
        # the oldest value left the window, so the queue is steady
        assert view._trend(view._queue_history["book_index"]).plain == " "

    def test_lag_arrow(self):
        sync = make_sync()
        sync.replication_slot_lag.side_effect = [4096, 1024]
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.probe()
        view._next_lag = 0
        view.probe()
        assert "1.0 KB ↓" in text_of(view.render())


class TestPeak:
    def test_peak(self):
        view = monitor.MonitorView()
        view.event_rate.extend([3, 1204, 7])
        view.doc_rate.extend([980, 2])
        text = text_of(view.render())
        assert "peak 1,204" in text
        assert "peak 980" in text

    def test_narrow_terminal_shows_fewer_seconds(self):
        from rich.console import Console

        console = Console(width=70, height=30, file=io.StringIO())
        seconds, bar = monitor.MonitorView._widths(console)
        assert 10 <= seconds < monitor.HISTORY
        assert bar == 8
        assert monitor.MonitorView._widths(None) == (
            monitor.HISTORY,
            monitor.BAR_WIDTH,
        )
        view = monitor.MonitorView()
        console = Console(width=70, height=30, record=True, file=io.StringIO())
        console.print(view.render(console))
        assert f"last {seconds}s" in console.export_text()


class TestHiddenColumns:
    def test_wal_mode_hides_db_and_redis(self):
        sync = make_sync()
        sync._checkpoint = None
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        header = text_of(view.render()).splitlines()[2]
        assert "xlog" in header
        assert "sent" in header
        for name in (" db ", "redis", "slot lag", "checkpoint", "opensearch"):
            assert name not in header

    def test_trigger_mode_shows_db_and_redis(self):
        sync = make_sync()
        sync._redis = Mock(qsize=3)
        sync.replication_slot_lag.return_value = 10
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        view.probe()
        header = text_of(view.render()).splitlines()[2]
        for name in (" db ", "redis", "slot lag", "checkpoint", "opensearch"):
            assert name in header

    def test_index_count_failure(self):
        sync = make_sync()
        sync.search_client.count.side_effect = RuntimeError("no index")
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.probe()
        assert view._counts(sync)["indexed"] is None

    def test_index_count_interval(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        with patch("pgsync.monitor.time.time", return_value=1000.0):
            view.probe()
            view.probe()
        sync.search_client.count.assert_called_once_with("book_index")
        with patch(
            "pgsync.monitor.time.time",
            return_value=1000.0 + monitor.COUNT_INTERVAL,
        ):
            view.probe()
        assert sync.search_client.count.call_count == 2


class TestKeys:
    def test_help(self):
        view = monitor.MonitorView()
        view.press("?")
        assert view.view == "help"
        text = text_of(view.render())
        assert "HELP" in text
        assert "checkpoint stale" in text
        assert "LIVE EVENTS" not in text
        view.press("?")
        assert view.view == "events"
        view.press("?")
        view.press("esc")
        assert view.view == "events"

    def test_pause_and_filter(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(1))
        view.press("p")
        assert view.paused is True
        view.press("f")
        assert view.filter == "public.book"
        view.press("x")  # an unknown key does nothing
        assert view.view == "events"

    def test_details_without_event(self):
        view = monitor.MonitorView()
        view.press("d")
        assert view.view == "details"
        assert "no event yet" in text_of(view.render())

    def test_details_of_update(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=UPDATE,
                    table="book",
                    schema="public",
                    old={"id": 1, "title": "Old", "isbn": "x"},
                    new={"id": 1, "title": "New", "isbn": "x"},
                    xmin=77,
                )
            ],
        )
        view.press("d")
        text = text_of(view.render())
        assert "EVENT DETAILS" in text
        assert "xmin=77" in text
        assert "'Old'" in text
        assert "'New'" in text
        assert "column" in text
        view.press("d")
        assert view.view == "events"

    def test_details_are_frozen(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(1))
        view.press("d")
        view.record(
            sync,
            [
                Payload(
                    tg_op=INSERT, table="book", schema="public", new={"id": 99}
                )
            ],
        )
        assert "id=99" not in text_of(view.render())

    def test_details_follow_the_filter(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(sync, book_payloads(1, table="author"))
        view.record(sync, book_payloads(1, table="book"))
        view.filter = "public.author"
        view.press("d")
        assert view._details[0].table == "public.author"

    def test_details_of_delete(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.record(
            sync,
            [
                Payload(
                    tg_op=DELETE,
                    table="book",
                    schema="public",
                    old={"id": 4, "title": "Gone"},
                )
            ],
        )
        view.press("d")
        text = text_of(view.render())
        assert "'Gone'" in text
        assert " old" in text

    def test_details_cut_long_rows(self):
        sync = make_sync()
        view = monitor.MonitorView()
        row = {f"c{i}": i for i in range(50)}
        view.record(
            sync,
            [Payload(tg_op=INSERT, table="book", schema="public", new=row)],
        )
        view.press("d")
        assert "more columns" in text_of(view.render())

    def test_footer(self):
        text = text_of(monitor.MonitorView().render())
        assert "d details" in text
        assert "? help" in text

    def test_signature_changes_with_view(self):
        view = monitor.MonitorView()
        before = view.signature()
        view.press("?")
        assert view.signature() != before


class TestReadKey:
    def read(self, data):
        stream = Mock()
        stream.fileno.return_value = 7
        with (
            patch(
                "pgsync.monitor.select.select", return_value=([stream], [], [])
            ),
            patch("pgsync.monitor.os.read", return_value=data),
        ):
            return monitor.MonitorView._read_key(stream, 0.1)

    def test_keys(self):
        assert self.read(b"q") == "q"
        assert self.read(b"D") == "d"
        assert self.read(b"?") == "?"
        assert self.read(b"\x1b") == "esc"
        assert self.read(b"\x1b[A") is None  # an arrow key
        assert self.read(b"") is None

    def test_no_key(self):
        with patch("pgsync.monitor.select.select", return_value=([], [], [])):
            assert monitor.MonitorView._read_key(Mock(), 0.1) is None


class TestScan:
    def scan(self, view, sync, count):
        docs = view.scan(sync, iter(range(count)))
        return docs

    def test_track_scan_off(self):
        docs = [1, 2]
        assert monitor.track_scan(make_sync(), docs) is docs

    def test_progress_with_estimate(self):
        sync = make_sync()
        sync.estimated_count.return_value = 200
        view = monitor.MonitorView()
        monitor.VIEW = view
        docs = monitor.track_scan(sync, iter(range(1000)))
        for _ in range(50):
            next(docs)
        view._scans["book_index"]["started"] -= 10
        sync.estimated_count.assert_called_once_with(
            sync.tree.root.table, sync.tree.root.schema
        )
        text = text_of(view.render())
        assert "PROGRESS" in text
        assert "initial sync" in text
        assert "25.0%" in text
        assert "50 / ~200" in text
        assert "  eta " in text

    def test_estimate_too_low(self):
        sync = make_sync()
        sync.estimated_count.return_value = 10
        view = monitor.MonitorView()
        docs = view.scan(sync, iter(range(100)))
        for _ in range(50):
            next(docs)
        text = text_of(view.render())
        assert "99.9%" in text
        assert "  eta " not in text

    def test_progress_without_estimate(self):
        sync = make_sync()
        sync.estimated_count.side_effect = RuntimeError("mysql")
        view = monitor.MonitorView()
        docs = view.scan(sync, iter(range(10)))
        next(docs)
        text = text_of(view.render())
        assert "initial sync" in text
        assert "1 docs" in text

    def test_finished(self):
        sync = make_sync()
        view = monitor.MonitorView()
        assert list(view.scan(sync, iter(range(7)))) == list(range(7))
        text = text_of(view.render())
        assert "done" in text
        assert "7 docs in" in text
        view._scans["book_index"]["finished"] -= monitor.SCAN_TTL + 1
        assert "PROGRESS" not in text_of(view.render())

    def test_failure_marks_finished(self):
        def broken():
            yield 1
            raise RuntimeError("bulk")

        sync = make_sync()
        view = monitor.MonitorView()
        with pytest.raises(RuntimeError):
            list(view.scan(sync, broken()))
        assert view._scans["book_index"]["finished"] is not None


class TestProbeThread:
    def test_start_and_stop(self):
        view = monitor.MonitorView()
        with (
            patch.object(view, "_run"),
            patch.object(view, "_capture_output", return_value=io.StringIO()),
            patch.object(view, "probe") as probe,
        ):
            view.start()
            assert view._probe_thread.is_alive()
            view.stop()
            view._probe_thread.join(timeout=2)
        assert not view._probe_thread.is_alive()
        probe.assert_called()


class TestWording:
    def test_plural(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(sync, book_payloads(1))
        text = text_of(view.render())
        assert "1 index" in text
        assert "1 table" in text
        assert "1 tables" not in text
        view.record(sync, book_payloads(1, table="author"))
        assert "2 tables" in text_of(view.render())

    def test_trigger_note_in_details(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "trigger")
        view.record(sync, book_payloads(1))
        view.press("d")
        assert "primary key and foreign" in text_of(view.render())

    def test_no_trigger_note_in_wal_mode(self):
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(sync, book_payloads(1))
        view.press("d")
        assert "primary key and foreign" not in text_of(view.render())


class TestPalette:
    def test_default(self):
        assert monitor.palette() == monitor.PALETTES["default"]
        assert set(monitor.palette()) == set(monitor.ROLES)

    def test_every_palette_has_every_role(self):
        for name in monitor.PALETTES:
            assert set(monitor.palette(name)) == set(monitor.ROLES), name

    def test_theme_name_ignores_case(self):
        assert monitor.palette(" Light ") == monitor.PALETTES["light"]
        assert monitor.palette("") == monitor.PALETTES["default"]

    def test_unknown_theme(self):
        with pytest.raises(ValueError, match="Use one of: default, light"):
            monitor.palette("neon")

    def test_colors_change_single_roles(self):
        styles = monitor.palette("mono", "border=magenta, insert=bold #00ff00")
        assert styles["border"] == "magenta"
        assert styles["insert"] == "bold #00ff00"
        assert styles["delete"] == ""

    def test_bad_role(self):
        with pytest.raises(ValueError, match="role is one of"):
            monitor.palette("default", "frame=red")
        with pytest.raises(ValueError, match="role=style"):
            monitor.palette("default", "red")

    def test_bad_style(self):
        with pytest.raises(ValueError, match='invalid style "notacolor"'):
            monitor.palette("default", "border=notacolor")

    def test_palette_is_used(self):
        monitor.set_palette(monitor.palette("default", "border=magenta"))
        assert monitor._s("border") == "magenta"
        assert monitor._s("ok", bold=True) == "bold #9ece6a"
        assert monitor._op(INSERT) == "#9ece6a"
        panel = monitor.MonitorView().render()
        assert panel.border_style == "magenta"

    def test_mono_has_no_color(self):
        monitor.set_palette(monitor.palette("mono"))
        assert monitor._s("alert", bold=True) == "bold"
        assert monitor._op(DELETE) == ""
        sync = make_sync()
        view = monitor.MonitorView()
        view.register(sync, "WAL")
        view.record(sync, book_payloads(3))
        assert "public.book" in text_of(view.render())

    @patch("pgsync.sync.IS_MYSQL_COMPAT", False)
    def test_cli_rejects_a_bad_theme(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile("w", suffix=".json") as config:
            config.write("[]")
            config.flush()
            with (
                patch("pgsync.monitor.interactive", return_value=True),
                patch("pgsync.monitor.start") as start,
                patch("pgsync.sync.settings.MONITOR_THEME", "neon"),
            ):
                result = runner.invoke(
                    main, ["--config", config.name, "-d", "-m"]
                )
        assert result.exit_code == 2
        assert 'unknown monitor theme "neon"' in result.output
        start.assert_not_called()
