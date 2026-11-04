"""Terminal monitor of sync events (pgsync --monitor).

Sync feeds the monitor through the module-level helpers (``record``,
``progress``, ``track_scan``). They do nothing unless ``start`` was called.

Requires the optional ``rich`` package: ``pip install pgsync[monitor]``.
"""

import atexit
import io
import logging
import os
import select
import signal
import sys
import threading
import time
import typing as t
from collections import Counter, defaultdict, deque
from datetime import datetime

from . import __version__
from .constants import DELETE, INSERT, TRUNCATE, UPDATE

try:
    # optional dependency: pip install pgsync[monitor]
    from rich import box
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.style import Style
    from rich.table import Table
    from rich.text import Text
except ImportError:  # pragma: no cover
    box = Console = Group = Live = Panel = Style = Table = Text = None

try:
    # POSIX only: key presses need a terminal in cbreak mode
    import termios
    import tty
except ImportError:  # pragma: no cover
    termios = tty = None

if t.TYPE_CHECKING:  # pragma: no cover
    from .base import Payload
    from .sync import Sync

OPS: t.Tuple[str, ...] = (INSERT, UPDATE, DELETE, TRUNCATE)

# a rich style for each color role (MONITOR_THEME, MONITOR_COLORS)
ROLES: t.Tuple[str, ...] = (
    "border",  # the panel border
    "accent",  # the redis queue, the events sparkline, the progress bars
    "docs",  # the documents sparkline and the sent column
    "ok",  # healthy, a falling value, a new value
    "warn",  # a warning, a growing value, a changed column
    "alert",  # an error, an old value
    "track",  # the empty part of a progress bar
    "insert",
    "update",
    "delete",
    "truncate",
)
PALETTES: t.Dict[str, t.Dict[str, str]] = {
    # for a dark terminal background
    "default": {
        "border": "#7aa2f7",
        "accent": "#7dcfff",
        "docs": "#9ece6a",
        "ok": "#9ece6a",
        "warn": "#e0af68",
        "alert": "#f7768e",
        "track": "#3b4261",
        "insert": "#9ece6a",
        "update": "#e0af68",
        "delete": "#f7768e",
        "truncate": "#bb9af7",
    },
    # for a light terminal background
    "light": {
        "border": "#1e66f5",
        "accent": "#04a5e5",
        "docs": "#40a02b",
        "ok": "#40a02b",
        "warn": "#df8e1d",
        "alert": "#d20f39",
        "track": "#ccd0da",
        "insert": "#40a02b",
        "update": "#df8e1d",
        "delete": "#d20f39",
        "truncate": "#8839ef",
    },
    # the 16 terminal colors
    "classic": {
        "border": "blue",
        "accent": "cyan",
        "docs": "green",
        "ok": "green",
        "warn": "yellow",
        "alert": "red",
        "track": "bright_black",
        "insert": "green",
        "update": "yellow",
        "delete": "red",
        "truncate": "magenta",
    },
    # no color: only bold and dim text
    "mono": {
        "border": "",
        "accent": "",
        "docs": "",
        "ok": "",
        "warn": "",
        "alert": "",
        "track": "dim",
        "insert": "",
        "update": "",
        "delete": "",
        "truncate": "",
    },
}
# the active palette (see set_palette)
STYLE: t.Dict[str, str] = dict(PALETTES["default"])
SPARK: str = "▁▂▃▄▅▆▇█"
HISTORY: int = 30  # seconds of throughput history
PROGRESS_TTL: float = 5.0  # hide catch-up progress after this many seconds
MAX_EVENTS: int = 200  # events kept for a tall terminal
VISIBLE_EVENTS: int = 12  # events shown if the terminal height is unknown
QUEUE_WARN: int = 1_000  # redis queue size shown in yellow
QUEUE_ALERT: int = 10_000  # redis queue size shown in red
STALE_CHECKPOINT: float = 60.0  # checkpoint age (seconds) shown in red
RECENT_ERROR: float = 60.0  # an error in this many seconds sets the health
TABLE_ROWS: int = 10  # tables shown if the terminal height is unknown
LAG_INTERVAL: float = 5.0  # seconds between two slot lag queries
LAG_WARN: int = 100 * 1024**2  # slot lag (bytes) shown in yellow
LAG_ALERT: int = 1024**3  # slot lag (bytes) shown in red
IDLE_REFRESH: float = 1.0  # seconds between two frames if nothing changed
PROBE_INTERVAL: float = 1.0  # seconds between two redis queue reads
COUNT_INTERVAL: float = 10.0  # seconds between two index document counts
TREND_SECONDS: int = 5  # the queue arrow compares with this many seconds ago
SCAN_TTL: float = 10.0  # show a finished initial sync this many seconds
BAR_WIDTH: int = 24  # characters in a progress bar

VIEW: t.Optional["MonitorView"] = None


def palette(theme: str = "default", colors: str = "") -> t.Dict[str, str]:
    """Return a palette: a named theme, changed by ``colors``.

    ``colors`` is a comma-separated list of ``role=style`` items, for
    example ``"border=magenta,insert=bold #00ff00"``. Raise ValueError if
    the theme, a role, or a style is not valid.
    """
    name: str = (theme or "default").strip().lower()
    if name not in PALETTES:
        raise ValueError(
            f'unknown monitor theme "{theme}". '
            f"Use one of: {', '.join(PALETTES)}."
        )
    result: t.Dict[str, str] = dict(PALETTES[name])
    for item in (colors or "").split(","):
        if not item.strip():
            continue
        role, sep, style = item.partition("=")
        role, style = role.strip().lower(), style.strip()
        if not sep or role not in ROLES:
            raise ValueError(
                f'invalid monitor color "{item.strip()}". Use role=style, '
                f"where role is one of: {', '.join(ROLES)}."
            )
        if Style is not None and style:
            try:
                Style.parse(style)
            except Exception:
                raise ValueError(
                    f'invalid style "{style}" for the monitor color "{role}".'
                )
        result[role] = style
    return result


def set_palette(styles: t.Dict[str, str]) -> None:
    STYLE.clear()
    STYLE.update(styles)


def _s(role: str, bold: bool = False) -> str:
    """Return the style of a color role, with bold if ``bold`` is True."""
    style: str = STYLE.get(role, "")
    return f"bold {style}".strip() if bold else style


def _op(tg_op: str) -> str:
    """Return the style of an operation, for example INSERT."""
    return STYLE.get(tg_op.lower(), "")


class Event(object):
    """A change event as shown in the monitor."""

    __slots__ = ("ts", "tg_op", "table", "pk", "xmin", "columns", "skipped")

    def __init__(
        self,
        tg_op: str,
        table: str,
        pk: str,
        xmin: t.Optional[int],
        columns: t.List[str],
        ts: t.Optional[float] = None,
        skipped: bool = False,
    ):
        self.tg_op: str = tg_op
        self.table: str = table
        self.pk: str = pk
        self.xmin: t.Optional[int] = xmin
        self.columns: t.List[str] = columns
        self.ts: float = ts or time.time()
        # the table is not in the schema tree, so Sync ignores the event
        self.skipped: bool = skipped


class _LogHandler(logging.Handler):
    """Send log records at WARNING and above to the monitor."""

    def __init__(self, view: "MonitorView"):
        super().__init__(level=logging.WARNING)
        self.view = view

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.view.message(
                record.levelname, f"{record.name}: {record.getMessage()}"
            )
        except Exception:  # pragma: no cover
            self.handleError(record)


class _StreamCapture(io.TextIOBase):
    """Replace stdout/stderr so stray writes do not break the screen.

    If ``level`` is None, discard the text: stdout has only status output,
    which the monitor replaces.
    """

    def __init__(self, view: "MonitorView", level: t.Optional[str]):
        self.view = view
        self.level: t.Optional[str] = level
        self._partial: str = ""

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        if self.level is None:
            return len(text)
        # carriage returns are in-place progress updates: keep the last one
        lines: t.List[str] = (self._partial + text).split("\n")
        self._partial = lines.pop()
        for line in lines:
            line = line.rsplit("\r", 1)[-1].strip()
            if line:
                self.view.message(self.level, line)
        return len(text)

    def flush(self) -> None:
        pass


class _EventList(object):
    """The event rows, laid out for the width of the terminal."""

    GAP: int = 2  # space between two columns
    TS_WIDTH: int = 12
    OP_WIDTH: int = 8

    def __init__(self, events: t.List[Event]):
        self.events: t.List[Event] = events

    @staticmethod
    def _width(values: t.Iterable[str], limit: int) -> int:
        return min(limit, max((len(value) for value in values), default=0))

    def widths(self, total: int) -> t.Tuple[int, int, int, int]:
        """Return the table, pk, xmin, and changed-columns widths.

        The time, operation, and xmin columns keep their full width. The
        table column gets up to 30 characters. The pk column gets the
        remaining space before the changed columns.
        """
        events: t.List[Event] = self.events
        xmin: int = self._width(
            (f"xmin={event.xmin}" for event in events), limit=24
        )
        table: int = self._width((event.table for event in events), limit=30)
        pk: int = self._width((event.pk for event in events), limit=40)
        rest: int = total - self.TS_WIDTH - self.OP_WIDTH - xmin - 5 * self.GAP
        # a narrow terminal: share the space between the table and the pk
        if table + pk > rest:
            table = min(table, max(8, rest // 2))
            pk = max(6, rest - table)
        changed: int = max(0, rest - table - pk)
        return table, pk, xmin, changed

    def __rich_console__(self, console, options):
        table_w, pk_w, xmin_w, changed_w = self.widths(options.max_width)
        gap: str = " " * self.GAP

        def cell(text: str, width: int, style: str = "") -> Text:
            value: Text = Text(text, style=style, no_wrap=True)
            value.truncate(width, overflow="ellipsis", pad=True)
            return value

        for event in self.events:
            ts: str = datetime.fromtimestamp(event.ts).strftime("%H:%M:%S.%f")[
                :-3
            ]
            xmin: str = "" if event.xmin is None else f"xmin={event.xmin}"
            if event.skipped:
                parts: t.List[Text] = [
                    cell(ts, self.TS_WIDTH),
                    cell(event.tg_op, self.OP_WIDTH),
                    cell(event.table, table_w),
                    cell(event.pk, pk_w),
                    cell(xmin, xmin_w),
                ]
                if changed_w > 0:
                    parts.append(cell("skipped", changed_w))
                line: Text = Text(gap, no_wrap=True, style="dim").join(parts)
                line.stylize("dim")
            else:
                parts = [
                    cell(ts, self.TS_WIDTH, "dim"),
                    cell(
                        event.tg_op,
                        self.OP_WIDTH,
                        _op(event.tg_op),
                    ),
                    cell(event.table, table_w, "bold"),
                    cell(event.pk, pk_w),
                    cell(xmin, xmin_w, "dim"),
                ]
                changed: str = ", ".join(event.columns)
                if changed and changed_w > 0:
                    parts.append(cell(changed, changed_w, "italic"))
                line = Text(gap, no_wrap=True).join(parts)
            line.rstrip()
            yield line


class MonitorView(object):
    """Collect sync events and render them to the terminal."""

    def __init__(
        self,
        max_events: int = MAX_EVENTS,
        max_messages: int = 4,
        refresh_per_second: float = 4,
    ):
        self.started: float = time.time()
        self.refresh_per_second: float = refresh_per_second
        self.syncs: t.List[t.Tuple["Sync", str]] = []
        self.events: t.Deque[Event] = deque(maxlen=max_events)
        self.last_event_at: t.Optional[float] = None
        self.last_error_at: t.Optional[float] = None
        # (time, level, text, count): a repeated message has a count above 1
        self.messages: t.Deque[t.Tuple[float, str, str, int]] = deque(
            maxlen=max_messages
        )
        self.by_table: t.DefaultDict[str, Counter] = defaultdict(Counter)
        self.errors: int = 0
        self.paused: bool = False
        # show only the events of this table (the f key)
        self.filter: t.Optional[str] = None
        self._table_events: t.DefaultDict[str, t.Deque[Event]] = defaultdict(
            lambda: deque(maxlen=max_events)
        )
        # "events", "details" (the d key), or "help" (the ? key)
        self.view: str = "events"
        # the event that the details view shows: (event, old row, new row)
        self._details: t.Optional[t.Tuple[Event, dict, dict]] = None
        # the newest row by table, and by None for all tables
        self._rows: t.Dict[t.Optional[str], t.Tuple[Event, dict, dict]] = {}
        self._lags: t.Dict[str, t.Optional[int]] = {}  # by index
        self._next_lag: float = 0.0
        self._doc_counts: t.Dict[str, t.Optional[int]] = {}  # by index
        self._next_count: float = 0.0
        # recent values for the trend arrows, by index
        self._queue_history: t.DefaultDict[str, t.Deque[t.Optional[int]]] = (
            defaultdict(lambda: deque(maxlen=TREND_SECONDS + 1))
        )
        self._lag_history: t.DefaultDict[str, t.Deque[t.Optional[int]]] = (
            defaultdict(lambda: deque(maxlen=2))
        )
        # initial sync (full table scan) progress, by index
        self._scans: t.Dict[str, dict] = {}
        self._probe_thread: t.Optional[threading.Thread] = None
        # rows used by the top sections, by layout (see _visible_events)
        self._top_rows: t.Dict[tuple, int] = {}
        self.event_rate: t.Deque[int] = deque([0] * HISTORY, maxlen=HISTORY)
        self.doc_rate: t.Deque[int] = deque([0] * HISTORY, maxlen=HISTORY)
        self._events_total: int = 0
        self._sample: t.Optional[t.Tuple[int, int, int]] = None
        self._checkpoints: t.Dict[str, t.Tuple[t.Any, float]] = {}
        self._last_event: t.Dict[str, float] = {}  # by index
        self._queues: t.Dict[str, t.Optional[int]] = {}  # by index
        self._progress: t.Dict[str, t.Tuple[int, int, float]] = {}
        self._pk_names: t.Dict[t.Tuple[str, str, str], t.List[str]] = {}
        self._lock: threading.Lock = threading.Lock()
        self._stop_event: threading.Event = threading.Event()
        self._thread: t.Optional[threading.Thread] = None
        self._saved: t.Optional[dict] = None

    # input from Sync

    def register(self, sync: "Sync", mode: str) -> None:
        with self._lock:
            self.syncs.append((sync, mode))

    def record(
        self,
        sync: "Sync",
        payloads: t.List["Payload"],
        skipped: bool = False,
    ) -> None:
        """Record one batch of change events from Sync._payloads.

        The monitor counts every payload, but builds an Event only for the
        payloads that the event list can keep.
        """
        now: float = time.time()
        counts: Counter = Counter(
            (f"{payload.schema}.{payload.table}", payload.tg_op)
            for payload in payloads
        )
        events: t.List[Event] = []
        last: t.Optional[t.Tuple[Event, dict, dict]] = None
        if not self.paused:
            events = [
                Event(
                    payload.tg_op,
                    f"{payload.schema}.{payload.table}",
                    self._format_pk(sync, payload),
                    payload.xmin,
                    self._changed_columns(payload),
                    ts=now,
                    skipped=skipped,
                )
                for payload in payloads[-self.events.maxlen :]
            ]
            if events:
                last = (events[-1], payloads[-1].old, payloads[-1].new)
        with self._lock:
            for (table, tg_op), count in counts.items():
                self.by_table[table][tg_op] += count
            self._events_total += len(payloads)
            if payloads:
                self.last_event_at = now
                self._last_event[sync.index] = now
            self.events.extendleft(events)
            for event in events:
                self._table_events[event.table].appendleft(event)
            if last is not None:
                self._rows[None] = last
                self._rows[last[0].table] = last

    def progress(self, sync: "Sync", current: int, total: int) -> None:
        with self._lock:
            self._progress[sync.index] = (current, total, time.time())

    def message(self, level: str, text: str) -> None:
        """Add a message. A repeated message moves to the top with a count."""
        now: float = time.time()
        with self._lock:
            if level in ("ERROR", "CRITICAL"):
                self.errors += 1
                self.last_error_at = now
            count: int = 1
            for item in list(self.messages):
                if item[1] == level and item[2] == text:
                    count += item[3]
                    self.messages.remove(item)
                    break
            self.messages.appendleft((now, level, text, count))

    def next_filter(self) -> None:
        """Show the events of the next table, then all events (the f key)."""
        with self._lock:
            tables: t.List[str] = self._busiest_tables()
            if self.filter not in tables:
                self.filter = tables[0] if tables else None
            else:
                index: int = tables.index(self.filter) + 1
                self.filter = tables[index] if index < len(tables) else None

    def _busiest_tables(self) -> t.List[str]:
        return [
            name
            for name, _ in sorted(
                self.by_table.items(), key=lambda item: -sum(item[1].values())
            )
        ]

    def _format_pk(self, sync: "Sync", payload: "Payload") -> str:
        data: dict = payload.data
        if not data:
            return ""
        key: t.Tuple[str, str, str] = (
            sync.index,
            payload.schema,
            payload.table,
        )
        names: t.Optional[t.List[str]] = self._pk_names.get(key)
        if names is None:
            try:
                node = sync.tree.get_node(payload.table, payload.schema)
                names = [str(name) for name in node.model.primary_keys]
            except Exception:
                names = []
            self._pk_names[key] = names
        values: t.List[str] = [
            f"{name}={data[name]}" for name in names if name in data
        ]
        if not values:
            # unknown primary key: show the first column only
            name, value = next(iter(data.items()))
            values = [f"{name}={value}"]
        return " ".join(values)

    @staticmethod
    def _changed_columns(payload: "Payload") -> t.List[str]:
        if payload.tg_op != UPDATE or not payload.old or not payload.new:
            return []
        return [
            column
            for column, value in payload.new.items()
            if column in payload.old and payload.old[column] != value
        ]

    # sampling

    @staticmethod
    def _queue_size(sync: "Sync") -> t.Optional[int]:
        # do not create a Redis connection in WAL mode
        if sync._redis is None:
            return None
        try:
            return sync._redis.qsize
        except Exception:
            return None

    @staticmethod
    def _slot_lag(sync: "Sync") -> t.Optional[int]:
        try:
            return sync.replication_slot_lag(sync.slot_name)
        except Exception:
            # for example a standby, where PG_CURRENT_WAL_LSN() fails
            return None

    @staticmethod
    def _doc_count(sync: "Sync") -> t.Optional[int]:
        try:
            return sync.search_client.count(sync.index)
        except Exception:
            # for example the index does not exist yet
            return None

    def _counts(self, sync: "Sync") -> dict:
        return {
            "xlog": sync.count.get("xlog", 0),
            "db": sync.count.get("db", 0),
            "redis": self._queues.get(sync.index),
            "lag": self._lags.get(sync.index),
            "sent": sync.search_client.doc_count,
            "indexed": self._doc_counts.get(sync.index),
            "engine": sync.search_client.name,
            "checkpoint": sync._checkpoint,
        }

    def probe(self) -> None:
        """Read the values that need a network call (in the probe thread)."""
        syncs: t.List["Sync"] = [sync for sync, _ in list(self.syncs)]
        queues: t.Dict[str, t.Optional[int]] = {
            sync.index: self._queue_size(sync) for sync in syncs
        }
        lags: t.Dict[str, t.Optional[int]] = {}
        if time.time() >= self._next_lag:
            lags = {sync.index: self._slot_lag(sync) for sync in syncs}
            self._next_lag = time.time() + LAG_INTERVAL
        doc_counts: t.Dict[str, t.Optional[int]] = {}
        if time.time() >= self._next_count:
            doc_counts = {sync.index: self._doc_count(sync) for sync in syncs}
            self._next_count = time.time() + COUNT_INTERVAL
        with self._lock:
            self._queues.update(queues)
            self._lags.update(lags)
            self._doc_counts.update(doc_counts)
            for index, size in queues.items():
                self._queue_history[index].append(size)
            for index, lag in lags.items():
                self._lag_history[index].append(lag)

    def sample(self) -> None:
        """Take one throughput sample. The render loop calls this every second."""
        docs: int = sum(
            sync.search_client.doc_count for sync, _ in list(self.syncs)
        )
        now: int = int(time.time())
        with self._lock:
            if self._sample is not None:
                last_ts, last_events, last_docs = self._sample
                # one sample per elapsed second, so a slow render does not
                # compress the history
                seconds: int = max(1, now - last_ts)
                event_delta: int = self._events_total - last_events
                doc_delta: int = max(0, docs - last_docs)
                for _ in range(seconds):
                    self.event_rate.append(event_delta // seconds)
                    self.doc_rate.append(doc_delta // seconds)
            self._sample = (now, self._events_total, docs)

    def scan(self, sync: "Sync", docs: t.Iterable[dict]) -> t.Iterator[dict]:
        """Count the documents of an initial sync (a full table scan)."""
        total: t.Optional[int] = None
        try:
            root = sync.tree.root
            total = sync.estimated_count(root.table, root.schema)
        except Exception:
            total = None
        state: dict = {
            "done": 0,
            "total": total,
            "started": time.time(),
            "finished": None,
        }
        with self._lock:
            self._scans[sync.index] = state
        try:
            for doc in docs:
                state["done"] += 1
                yield doc
        finally:
            state["finished"] = time.time()

    # rendering

    @staticmethod
    def _spark(values: t.Iterable[int]) -> str:
        values = list(values)
        top: int = max(values) or 1
        return "".join(
            SPARK[min(len(SPARK) - 1, int(v / top * (len(SPARK) - 1)))]
            for v in values
        )

    @staticmethod
    def _age(seconds: float) -> str:
        seconds = int(seconds)
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m{seconds % 60:02d}s"
        return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"

    @staticmethod
    def _bytes(size: int) -> str:
        value: float = float(size)
        for unit in ("B", "KB", "MB", "GB"):
            if value < 1024 or unit == "GB":
                return (
                    f"{value:.0f} {unit}"
                    if unit == "B"
                    else f"{value:.1f} {unit}"
                )
            value /= 1024
        return f"{value:.1f} TB"  # pragma: no cover

    @staticmethod
    def _trend(values: t.Iterable[t.Optional[int]]) -> Text:
        """Return an arrow: up if the value grows, down if it shrinks."""
        known: t.List[int] = [value for value in values if value is not None]
        if len(known) < 2 or known[-1] == known[0]:
            return Text(" ")
        if known[-1] > known[0]:
            return Text("↑", style=_s("warn", bold=True))
        return Text("↓", style=_s("ok", bold=True))

    @staticmethod
    def _bar(fraction: float, width: int = BAR_WIDTH) -> Text:
        filled: int = int(width * max(0.0, min(1.0, fraction)))
        bar: Text = Text("━" * filled, style=_s("accent"))
        bar.append("━" * (width - filled), style=_s("track"))
        return bar

    def _states(
        self, rows: t.List[t.Tuple["Sync", str, dict]]
    ) -> t.List[dict]:
        """Add the checkpoint age and the warnings to each pipeline row."""
        now: float = time.time()
        states: t.List[dict] = []
        for sync, mode, counts in rows:
            # show the name one time if the database and the index match
            name: str = (
                sync.index
                if sync.database == sync.index
                else f"{sync.database}:{sync.index}"
            )
            checkpoint: t.Any = counts["checkpoint"]
            previous = self._checkpoints.get(name)
            if previous is None or previous[0] != checkpoint:
                self._checkpoints[name] = (checkpoint, now)
            changed_at: float = self._checkpoints[name][1]
            age: float = now - changed_at
            # events arrive, but the checkpoint does not move
            stale: bool = (
                checkpoint is not None
                and age > STALE_CHECKPOINT
                and self._last_event.get(sync.index, 0) > changed_at
            )
            states.append(
                dict(
                    counts,
                    index=sync.index,
                    name=name,
                    mode=mode,
                    age=age,
                    stale=stale,
                )
            )
        return states

    @staticmethod
    def _queue_style(size: t.Optional[int]) -> str:
        if size is not None and size >= QUEUE_ALERT:
            return _s("alert", bold=True)
        if size is not None and size >= QUEUE_WARN:
            return _s("warn", bold=True)
        return _s("accent")

    @staticmethod
    def _lag_style(lag: t.Optional[int]) -> str:
        if lag is not None and lag >= LAG_ALERT:
            return _s("alert", bold=True)
        if lag is not None and lag >= LAG_WARN:
            return _s("warn", bold=True)
        return ""

    def _health(self, states: t.List[dict]) -> t.Tuple[str, str]:
        """Return the health label and its style for the header."""
        now: float = time.time()
        if self.last_error_at and now - self.last_error_at < RECENT_ERROR:
            return "errors", _s("alert", bold=True)
        if any(state["stale"] for state in states):
            return "checkpoint stale", _s("warn", bold=True)
        if any((state["lag"] or 0) >= LAG_WARN for state in states):
            return "slot lag", _s("warn", bold=True)
        if any((state["redis"] or 0) >= QUEUE_WARN for state in states):
            return "backlog", _s("warn", bold=True)
        return "healthy", _s("ok", bold=True)

    @staticmethod
    def _heading(title: str, note: str = "") -> Text:
        text: Text = Text(title, style="bold")
        if note:
            text.append(f"  {note}", style="dim")
        return text

    @staticmethod
    def _table() -> "Table":
        return Table(
            box=box.SIMPLE_HEAD,
            show_edge=False,
            pad_edge=False,
            expand=False,
            header_style="dim",
            collapse_padding=True,
        )

    @staticmethod
    def _plural(count: int, word: str, words: str) -> str:
        return f"{count:,} {word if count == 1 else words}"

    @staticmethod
    def _widths(console: t.Optional["Console"]) -> t.Tuple[int, int]:
        """Return the sparkline and progress bar widths for the terminal."""
        if console is None:
            return HISTORY, BAR_WIDTH
        # the panel border and padding use 4 columns
        width: int = console.width - 4
        return (
            max(10, min(HISTORY, width - 52)),
            max(8, min(BAR_WIDTH, width - 72)),
        )

    def _pipeline(self, states: t.List[dict]):
        """Show one row for each index. Hide a column with no values."""

        def shown(key: str) -> bool:
            return any(state[key] is not None for state in states)

        def trend(history: t.Dict[str, t.Deque], state: dict) -> Text:
            return self._trend(history.get(state["index"], ()))

        trigger: bool = any(
            state["mode"].startswith("trigger") for state in states
        )
        table = self._table()
        table.add_column("index", style="bold", no_wrap=True)
        table.add_column("mode", style="dim", no_wrap=True)
        table.add_column("xlog", justify="right", no_wrap=True)
        if trigger:
            table.add_column("db", justify="right", no_wrap=True)
        if shown("redis"):
            table.add_column("redis", justify="right", no_wrap=True)
        if shown("lag"):
            table.add_column("slot lag", justify="right", no_wrap=True)
        table.add_column(
            "sent", justify="right", no_wrap=True, style=_s("docs")
        )
        if shown("indexed"):
            # the documents in the index, for example "opensearch"
            engines: t.Set[str] = {state["engine"] for state in states}
            table.add_column(
                engines.pop().lower() if len(engines) == 1 else "indexed",
                justify="right",
                no_wrap=True,
            )
        if shown("checkpoint"):
            table.add_column("checkpoint", justify="right", no_wrap=True)
            table.add_column("age", justify="right", no_wrap=True)
        for state in states:
            checkpoint: t.Any = state["checkpoint"]
            redis: t.Optional[int] = state["redis"]
            lag: t.Optional[int] = state["lag"]
            indexed: t.Optional[int] = state["indexed"]
            cells: list = [state["name"], state["mode"], f"{state['xlog']:,}"]
            if trigger:
                cells.append(f"{state['db']:,}")
            if shown("redis"):
                cells.append(
                    Text.assemble(
                        Text(
                            "-" if redis is None else f"{redis:,}",
                            style=self._queue_style(redis),
                        ),
                        " ",
                        trend(self._queue_history, state),
                    )
                )
            if shown("lag"):
                cells.append(
                    Text.assemble(
                        Text(
                            "-" if lag is None else self._bytes(lag),
                            style=self._lag_style(lag),
                        ),
                        " ",
                        trend(self._lag_history, state),
                    )
                )
            cells.append(f"{state['sent']:,}")
            if shown("indexed"):
                cells.append("-" if indexed is None else f"{indexed:,}")
            if shown("checkpoint"):
                cells += [
                    "-" if checkpoint is None else str(checkpoint),
                    Text(
                        "-" if checkpoint is None else self._age(state["age"]),
                        style=(
                            _s("alert", bold=True) if state["stale"] else "dim"
                        ),
                    ),
                ]
            table.add_row(*cells)
        return table

    def _progress_rows(self, bar: int = BAR_WIDTH) -> t.List[list]:
        """Return the initial sync and catch-up rows for PROGRESS."""
        now: float = time.time()
        rows: t.List[list] = []
        for index, state in list(self._scans.items()):
            done: int = state["done"]
            total: t.Optional[int] = state["total"]
            finished: t.Optional[float] = state["finished"]
            elapsed: float = max(1e-6, (finished or now) - state["started"])
            if finished is not None:
                if now - finished > SCAN_TTL:
                    continue
                rows.append(
                    [
                        "initial sync",
                        index,
                        self._bar(1.0, bar),
                        Text("done", style=_s("ok", bold=True)),
                        Text(
                            f"{done:,} docs in {self._age(elapsed)}",
                            style="dim",
                        ),
                    ]
                )
                continue
            rate: float = done / elapsed
            detail: Text = Text(f"{done:,}")
            if total:
                # the total is an estimate: never show 100% before the end
                fraction: float = min(done / total, 0.999)
                detail.append(f" / ~{total:,}", style="dim")
                detail.append(f"  {rate:,.0f}/s")
                if rate > 0 and done < total:
                    eta: float = (total - done) / rate
                    detail.append(f"  eta {self._age(eta)}", style="dim")
                rows.append(
                    [
                        "initial sync",
                        index,
                        self._bar(fraction, bar),
                        f"{fraction:.1%}",
                        detail,
                    ]
                )
            else:
                detail.append(f" docs  {rate:,.0f}/s")
                rows.append(["initial sync", index, "", "", detail])
        for index, (current, total, ts) in list(self._progress.items()):
            if now - ts > PROGRESS_TTL or not total:
                continue
            rows.append(
                [
                    "catch-up",
                    index,
                    self._bar(current / total, bar),
                    f"{min(current, total) / total:.1%}",
                    Text(f"{current:,} / {total:,}"),
                ]
            )
        return rows

    def _progress_grid(self, rows: t.List[list]):
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim", no_wrap=True)
        grid.add_column(style="bold", no_wrap=True)
        grid.add_column(no_wrap=True)
        grid.add_column(justify="right", no_wrap=True)
        grid.add_column(no_wrap=True, overflow="ellipsis")
        for row in rows:
            grid.add_row(*row)
        return grid

    def _throughput(self, seconds: int = HISTORY):
        now: float = time.time()
        events: t.List[int] = list(self.event_rate)[-seconds:]
        docs: t.List[int] = list(self.doc_rate)[-seconds:]
        last_event: str = "none"
        if self.last_event_at is not None:
            last_event = f"{self._age(now - self.last_event_at)} ago"
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim", no_wrap=True)
        grid.add_column(no_wrap=True)
        grid.add_column(justify="right", style="bold", no_wrap=True)
        grid.add_column(justify="right", style="dim", no_wrap=True)
        grid.add_column(style="dim", no_wrap=True)
        grid.add_column(style="bold", no_wrap=True)
        grid.add_row(
            "events",
            Text(self._spark(events), style=_s("accent")),
            f"{events[-1]:,}/s",
            f"peak {max(events):,}",
            "last event",
            last_event,
        )
        grid.add_row(
            "docs",
            Text(self._spark(docs), style=_s("docs")),
            f"{docs[-1]:,}/s",
            f"peak {max(docs):,}",
            "errors",
            Text(
                str(self.errors),
                style=_s("alert", bold=True) if self.errors else "",
            ),
        )
        return grid

    def _tables(self, limit: int):
        """Show the busiest tables. Draw no more than ``limit`` rows."""
        table = self._table()
        table.add_column("table", style="bold")
        for op in OPS:
            table.add_column(op, justify="right", style=_op(op))
        names: t.List[str] = self._busiest_tables()
        # one row for the busiest tables, and one row for the others
        shown: t.List[str] = (
            names if len(names) <= limit else names[: max(1, limit - 1)]
        )
        for name in shown:
            ops: Counter = self.by_table[name]
            table.add_row(name, *(f"{ops[op]:,}" for op in OPS))
        if len(shown) < len(names):
            table.add_row(
                Text(f"… {len(names) - len(shown)} more tables", style="dim")
            )
        return table

    def _messages(self):
        alert: str = _s("alert", bold=True)
        styles: dict = {"ERROR": alert, "CRITICAL": alert}
        table = Table.grid(padding=(0, 2))
        table.add_column(style="dim", no_wrap=True)
        table.add_column(no_wrap=True)
        table.add_column(no_wrap=True, overflow="ellipsis")
        for ts, level, text, count in self.messages:
            line: Text = Text(text)
            if count > 1:
                line.append(f"  ×{count:,}", style="bold")
            table.add_row(
                datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
                Text(level, style=styles.get(level, _s("warn"))),
                line,
            )
        return table

    def _help(self) -> t.List[t.Any]:
        """Return the help view (the ? key)."""

        def grid():
            table = Table.grid(padding=(0, 2))
            table.add_column(style="bold", no_wrap=True)
            table.add_column(no_wrap=True, overflow="ellipsis")
            return table

        keys = grid()
        for key, text in (
            ("q", "stop pgsync (the same as Ctrl+C)"),
            ("p", "pause or start the event list"),
            ("f", "show the events of the next table, then all tables"),
            ("d", "show the columns of the newest event"),
            ("?", "show or close this help"),
            ("esc", "go back to the event list"),
        ):
            keys.add_row(key, text)
        health = grid()
        for label, style, text in (
            ("healthy", _s("ok", bold=True), "no problem found"),
            (
                "backlog",
                _s("warn", bold=True),
                f"redis queue {QUEUE_WARN:,} or more",
            ),
            (
                "slot lag",
                _s("warn", bold=True),
                f"replication slot {self._bytes(LAG_WARN)} or more behind",
            ),
            (
                "checkpoint stale",
                _s("warn", bold=True),
                f"events arrive, no checkpoint for {STALE_CHECKPOINT:.0f}s",
            ),
            (
                "errors",
                _s("alert", bold=True),
                f"an error in the last {RECENT_ERROR:.0f}s",
            ),
        ):
            health.add_row(Text(f"● {label}", style=style), text)
        columns = grid()
        for name, text in (
            ("xlog", "changes read from the WAL or the trigger queue"),
            ("db", "trigger events read from the database"),
            ("redis", "events that wait in the redis queue  ↑ grows  ↓ falls"),
            ("slot lag", "WAL that the replication slot did not confirm"),
            ("sent", "documents sent to the search engine"),
            ("opensearch", "documents in the index (read each 10s)"),
            (
                "checkpoint",
                "the last saved transaction ID or LSN, and its age",
            ),
        ):
            columns.add_row(name, text)
        ops: Text = Text()
        for op in OPS:
            ops.append(f"{op}  ", style=_op(op))
        ops.append("skipped", style="dim")
        ops.append(" (the table is not in the schema)", style="dim")
        return [
            self._heading("HELP", "? or esc to close"),
            keys,
            Text(""),
            self._heading("HEALTH"),
            health,
            Text(""),
            self._heading("COLUMNS"),
            columns,
            Text(""),
            self._heading("EVENTS"),
            ops,
        ]

    def _details_view(
        self, limit: int, trigger: bool = False
    ) -> t.List[t.Any]:
        """Return the details view (the d key): the columns of one event."""
        heading: Text = self._heading("EVENT DETAILS", "d or esc to close")
        if self._details is None:
            return [
                heading,
                Text("no event yet", style="dim italic"),
            ]
        event, old, new = self._details
        summary: Text = Text()
        summary.append(
            datetime.fromtimestamp(event.ts).strftime("%H:%M:%S.%f")[:-3],
            style="dim",
        )
        summary.append("  ")
        summary.append(event.tg_op, style=_op(event.tg_op))
        summary.append("  ")
        summary.append(event.table, style="bold")
        summary.append(f"  {event.pk}")
        if event.xmin is not None:
            summary.append(f"  xmin={event.xmin}", style="dim")
        if event.skipped:
            summary.append("  skipped", style="dim")
        table = self._table()
        table.add_column("column", style="bold", no_wrap=True)
        names: t.List[str] = list(new) + [
            name for name in old if name not in new
        ]
        if event.tg_op == UPDATE:
            table.add_column("old", no_wrap=True, overflow="ellipsis", ratio=1)
        table.add_column(
            "old" if event.tg_op == DELETE else "new",
            no_wrap=True,
            overflow="ellipsis",
            ratio=1,
        )
        # the table header uses 2 rows, the summary and the note 2 rows
        rows: int = max(1, limit - 4)
        shown: t.List[str] = names[:rows]
        if len(names) > rows:
            shown = names[: rows - 1]
        for name in shown:
            if event.tg_op == UPDATE:
                changed: bool = name in old and old.get(name) != new.get(name)
                table.add_row(
                    Text(
                        name,
                        style=_s("warn", bold=True) if changed else "bold",
                    ),
                    Text(
                        repr(old.get(name)) if name in old else "",
                        style=_s("alert") if changed else "dim",
                    ),
                    Text(
                        repr(new.get(name)) if name in new else "",
                        style=_s("ok") if changed else "dim",
                    ),
                )
            else:
                row: dict = old if event.tg_op == DELETE else new
                table.add_row(name, repr(row.get(name)))
        if len(shown) < len(names):
            table.add_row(
                Text(f"… {len(names) - len(shown)} more columns", style="dim")
            )
        if not names:
            table.add_row(Text("no column data", style="dim italic"))
        parts: t.List[t.Any] = [heading, summary, table]
        if trigger:
            parts.append(
                Text(
                    "trigger mode sends only the primary key and foreign "
                    "key columns",
                    style="dim italic",
                )
            )
        return parts

    def _visible_events(
        self,
        top: t.List[t.Any],
        layout: tuple,
        console: t.Optional["Console"],
    ) -> int:
        """Return the number of rows that fit below the top sections.

        Measuring needs a full render, so keep the result for each layout.
        """
        if console is None:
            return VISIBLE_EVENTS
        key: tuple = (console.width,) + layout
        used: t.Optional[int] = self._top_rows.get(key)
        if used is None:
            # the panel border uses 2 rows and 4 columns
            options = console.options.update(width=max(1, console.width - 4))
            used = len(console.render_lines(Group(*top), options, pad=False))
            self._top_rows[key] = used
        return max(1, console.height - 2 - used)

    def render(self, console: t.Optional["Console"] = None):
        """Build the panel. With ``console``, fill the terminal height."""
        rows: t.List[t.Tuple["Sync", str, dict]] = [
            (sync, mode, self._counts(sync)) for sync, mode in list(self.syncs)
        ]
        table_limit: int = (
            TABLE_ROWS if console is None else max(3, console.height // 4)
        )
        blank: Text = Text("")
        seconds, bar = self._widths(console)
        with self._lock:
            states: t.List[dict] = self._states(rows)
            health, health_style = self._health(states)
            count: int = len(states)
            top: list = [
                self._heading(
                    "PIPELINE",
                    self._plural(count, "index", "indices"),
                ),
                self._pipeline(states),
                blank,
            ]
            progress: t.List[list] = self._progress_rows(bar)
            if progress:
                top += [
                    self._heading("PROGRESS"),
                    self._progress_grid(progress),
                    blank,
                ]
            table_rows: int = min(len(self.by_table), table_limit)
            top += [
                self._heading("THROUGHPUT", f"last {seconds}s"),
                self._throughput(seconds),
                blank,
            ]
            if self.view == "events":
                top += [
                    self._heading(
                        "BY TABLE",
                        (
                            self._plural(len(self.by_table), "table", "tables")
                            if self.by_table
                            else ""
                        ),
                    ),
                    self._tables(table_limit),
                    blank,
                ]
            # messages go above the events: a short terminal cuts the bottom
            if self.messages:
                top += [self._heading("MESSAGES"), self._messages(), blank]
            view: str = self.view
            trigger: bool = bool(self.syncs) and all(
                mode.startswith("trigger") for _, mode in self.syncs
            )
            notes: t.List[str] = []
            if self.filter is not None:
                notes.append(f"table: {self.filter}")
            if self.paused:
                notes.append("paused")
            if view == "events":
                top.append(self._heading("LIVE EVENTS", " · ".join(notes)))
            events: t.List[Event] = list(
                self.events
                if self.filter is None
                else self._table_events[self.filter]
            )
            producer_only: bool = not self.events and all(
                not sync.consumer for sync, _ in self.syncs
            )
            layout: tuple = (
                view,
                tuple(state["redis"] is None for state in states),
                tuple(state["lag"] is None for state in states),
                len(progress),
                table_rows,
                len(self.messages),
            )

        parts: list = list(top)
        visible: int = self._visible_events(top, layout, console)
        if view == "help":
            parts += self._help()
        elif view == "details":
            parts += self._details_view(visible, trigger)
        elif producer_only:
            parts.append(
                Text(
                    "producer only: the consumer process shows the events",
                    style="dim italic",
                )
            )
        elif not events:
            parts.append(Text("waiting for events", style="dim italic"))
        else:
            parts.append(_EventList(events[:visible]))

        return Panel(
            Group(*parts),
            title=Text.assemble(
                (f"pgsync {__version__}", "bold"),
                " ── ",
                (f"● {health}", health_style),
                f" ── up {self._age(time.time() - self.started)} ── ",
                datetime.now().strftime("%H:%M:%S"),
            ),
            title_align="left",
            subtitle=(
                "[bold]q[/] quit · [bold]p[/] pause · [bold]f[/] filter · "
                "[bold]d[/] details · [bold]?[/] help"
            ),
            subtitle_align="left",
            border_style=_s("border"),
            padding=(0, 1),
        )

    # lifecycle

    def _capture_output(self) -> t.TextIO:
        """Send log records and stray stdout/stderr writes to the monitor.

        Returns the real stdout, which the monitor draws on.
        """
        root: logging.Logger = logging.getLogger()
        before: t.List[logging.Handler] = list(root.handlers)
        console_handlers: t.List[logging.Handler] = [
            handler
            for handler in root.handlers
            if isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
        ]
        handler: _LogHandler = _LogHandler(self)
        for console_handler in console_handlers:
            root.removeHandler(console_handler)
        root.addHandler(handler)
        self._saved = {
            "handlers": before,
            "handler": handler,
            "stdout": sys.stdout,
            "stderr": sys.stderr,
        }
        sys.stdout = _StreamCapture(self, None)
        sys.stderr = _StreamCapture(self, "STDERR")
        return self._saved["stdout"]

    def _restore_output(self) -> None:
        if self._saved is None:
            return
        root: logging.Logger = logging.getLogger()
        before: t.List[logging.Handler] = self._saved["handlers"]
        # keep any handler added while the monitor was on
        added: t.List[logging.Handler] = [
            handler
            for handler in root.handlers
            if handler not in before and handler is not self._saved["handler"]
        ]
        root.handlers = before + added
        sys.stdout = self._saved["stdout"]
        sys.stderr = self._saved["stderr"]
        self._saved = None

    def signature(self) -> tuple:
        """Return the values that the panel shows, except the clocks."""
        with self._lock:
            return (
                self._events_total,
                self.errors,
                self.messages[0][::3] if self.messages else None,
                self.paused,
                self.filter,
                self.view,
                id(self._details),
                tuple(
                    (
                        sync.count.get("xlog", 0),
                        sync.count.get("db", 0),
                        sync.search_client.doc_count,
                        sync._checkpoint,
                    )
                    for sync, _ in self.syncs
                ),
                tuple(self._queues.items()),
                tuple(self._lags.items()),
                tuple(self._doc_counts.items()),
                tuple(
                    tuple(history) for history in self._queue_history.values()
                ),
                tuple(self._progress.values()),
                tuple(
                    (scan["done"], scan["finished"])
                    for scan in self._scans.values()
                ),
            )

    def press(self, key: str) -> None:
        """Act on one key press (except q, which stops pgsync)."""
        with self._lock:
            if key == "p":
                self.paused = not self.paused
            elif key == "?":
                self.view = "events" if self.view == "help" else "help"
            elif key == "d":
                if self.view == "details":
                    self.view = "events"
                else:
                    # a frozen copy: new events do not change the view
                    self._details = self._rows.get(self.filter)
                    self.view = "details"
            elif key == "esc":
                self.view = "events"
        if key == "f":
            self.next_filter()

    @staticmethod
    def _read_key(stream: t.TextIO, timeout: float) -> t.Optional[str]:
        """Return one key, "esc", or None.

        Read the bytes from the file descriptor, not from the text stream:
        the stream keeps a buffer that select does not see.
        """
        ready, _, _ = select.select([stream], [], [], timeout)
        if not ready:
            return None
        data: str = os.read(stream.fileno(), 32).decode(errors="ignore")
        if data == "\x1b":
            return "esc"
        if not data or data.startswith("\x1b"):
            # an arrow key or another escape sequence
            return None
        return data[0].lower()

    def _probe_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.probe()
            except Exception:  # pragma: no cover
                pass
            self._stop_event.wait(PROBE_INTERVAL)

    def _run(self, out: t.TextIO) -> None:
        console: Console = Console(file=out)
        stdin: t.TextIO = sys.__stdin__
        fd: t.Optional[int] = None
        old: t.Optional[list] = None
        if termios is not None and stdin is not None and stdin.isatty():
            fd = stdin.fileno()
            old = termios.tcgetattr(fd)
            # cbreak keeps Ctrl+C working
            tty.setcbreak(fd)
        interval: float = 1.0 / self.refresh_per_second
        next_sample: float = 0.0
        last: t.Optional[tuple] = None
        drawn_at: float = 0.0
        key: t.Optional[str] = None
        try:
            with Live(
                self.render(console),
                console=console,
                screen=True,
                auto_refresh=False,
                redirect_stdout=False,
                redirect_stderr=False,
            ) as live:
                while not self._stop_event.is_set():
                    now: float = time.time()
                    if now >= next_sample:
                        self.sample()
                        next_sample = now + 1.0
                    current: tuple = (console.size, self.signature())
                    if (
                        key is not None
                        or current != last
                        or now - drawn_at >= IDLE_REFRESH
                    ):
                        live.update(self.render(console), refresh=True)
                        last, drawn_at = current, now
                    key = (
                        self._read_key(stdin, interval)
                        if fd is not None
                        else None
                    )
                    if fd is None:
                        self._stop_event.wait(interval)
                    elif key == "q":
                        # same as Ctrl+C
                        os.kill(os.getpid(), signal.SIGINT)
                    elif key is not None:
                        self.press(key)
        finally:
            if fd is not None:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def start(self) -> None:
        out: t.TextIO = self._capture_output()
        self._thread = threading.Thread(
            target=self._run, args=(out,), name="pgsync-monitor", daemon=True
        )
        self._thread.start()
        self._probe_thread = threading.Thread(
            target=self._probe_loop, name="pgsync-monitor-probe", daemon=True
        )
        self._probe_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._restore_output()


def available() -> bool:
    return Live is not None


def interactive() -> bool:
    return sys.stdout.isatty()


def start() -> MonitorView:
    """Start the monitor. Call this once, from the main thread."""
    global VIEW
    if VIEW is None:
        VIEW = MonitorView()
        VIEW.start()
        atexit.register(stop)
    return VIEW


def stop() -> None:
    global VIEW
    if VIEW is not None:
        VIEW.stop()
        VIEW = None


def register(sync: "Sync", mode: str) -> None:
    if VIEW is not None:
        VIEW.register(sync, mode)


def record(
    sync: "Sync", payloads: t.List["Payload"], skipped: bool = False
) -> None:
    if VIEW is not None:
        VIEW.record(sync, payloads, skipped=skipped)


def progress(sync: "Sync", current: int, total: int) -> bool:
    """Show catch-up progress. Return True if the monitor took it."""
    if VIEW is None:
        return False
    VIEW.progress(sync, current, total)
    return True


def track_scan(sync: "Sync", docs: t.Iterable[dict]) -> t.Iterable[dict]:
    """Show the progress of an initial sync (a full table scan)."""
    if VIEW is None:
        return docs
    return VIEW.scan(sync, docs)


def active() -> bool:
    return VIEW is not None
