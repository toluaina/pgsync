#!/usr/bin/env python
"""PGSync data integrity stress harness.

Simulates a high traffic database on the book example topology (deep
nested one_to_one chains, one_to_many objects, through tables and
scalar aggregations) while pgsync syncs continuously in polling mode.
At configurable intervals the harness stops the world, drains the sync
pipeline and verifies Elasticsearch against the database using
independent SQL (not pgsync's own query builder):

- every book in the database has a document in the index (no data loss)
- every document in the index has a book in the database (no missed
  deletes)
- document fields match the database, including nested entities:
  publisher, authors with their city, country and continent, scalar
  language and subject aggregations and the rating

Everything it creates is temporary: a dedicated database, index,
replication slot and schema file, all torn down at exit unless --keep
is passed or a verification cycle fails.

Usage:
    python qa/stress_harness.py --cycles 3 --duration 10 --writers 6
"""

import itertools
import json
import logging
import random
import sys
import threading
import time
import typing as t
import uuid
from pathlib import Path

import click
import sqlalchemy as sa

REPO: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from pgsync.base import create_database  # noqa
from pgsync.singleton import Singleton  # noqa
from pgsync.sync import Sync  # noqa
from pgsync.urls import get_database_url  # noqa
from pgsync.utils import config_loader  # noqa

logger = logging.getLogger("qa.stress_harness")

QA_PREFIX: str = "qa_book_stress"

DDL: t.List[str] = [
    """
    CREATE TABLE continent (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE country (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL,
        continent_id INTEGER NOT NULL REFERENCES continent (id),
        UNIQUE (name, continent_id)
    )
    """,
    """
    CREATE TABLE city (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL,
        country_id INTEGER NOT NULL REFERENCES country (id),
        UNIQUE (name, country_id)
    )
    """,
    """
    CREATE TABLE publisher (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL UNIQUE,
        is_active BOOLEAN NOT NULL DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE author (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL UNIQUE,
        city_id INTEGER NOT NULL REFERENCES city (id)
    )
    """,
    """
    CREATE TABLE language (
        id SERIAL PRIMARY KEY,
        code VARCHAR(256) NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE subject (
        id SERIAL PRIMARY KEY,
        name VARCHAR(256) NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE book (
        id SERIAL PRIMARY KEY,
        isbn VARCHAR(256) NOT NULL UNIQUE,
        title VARCHAR(256) NOT NULL,
        description VARCHAR(256),
        tags JSONB,
        publisher_id INTEGER NOT NULL REFERENCES publisher (id)
    )
    """,
    """
    CREATE TABLE rating (
        id SERIAL PRIMARY KEY,
        book_isbn VARCHAR(256) NOT NULL UNIQUE
            REFERENCES book (isbn) ON DELETE CASCADE,
        value DOUBLE PRECISION
    )
    """,
    """
    CREATE TABLE book_author (
        id SERIAL PRIMARY KEY,
        book_isbn VARCHAR(256) NOT NULL
            REFERENCES book (isbn) ON DELETE CASCADE,
        author_id INTEGER NOT NULL REFERENCES author (id),
        UNIQUE (book_isbn, author_id)
    )
    """,
    """
    CREATE TABLE book_language (
        id SERIAL PRIMARY KEY,
        book_isbn VARCHAR(256) NOT NULL
            REFERENCES book (isbn) ON DELETE CASCADE,
        language_id INTEGER NOT NULL REFERENCES language (id),
        UNIQUE (book_isbn, language_id)
    )
    """,
    """
    CREATE TABLE book_subject (
        id SERIAL PRIMARY KEY,
        book_isbn VARCHAR(256) NOT NULL
            REFERENCES book (isbn) ON DELETE CASCADE,
        subject_id INTEGER NOT NULL REFERENCES subject (id),
        UNIQUE (book_isbn, subject_id)
    )
    """,
]


def build_schema(database: str, index: str) -> t.List[dict]:
    """The book topology without transforms, for crisp verification."""
    return [
        {
            "database": database,
            "index": index,
            "nodes": {
                "table": "book",
                "columns": ["id", "isbn", "title", "description", "tags"],
                "children": [
                    {
                        "table": "publisher",
                        "columns": ["id", "name", "is_active"],
                        "label": "publisher",
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    },
                    {
                        "table": "author",
                        "columns": ["id", "name"],
                        "label": "authors",
                        "relationship": {
                            "type": "one_to_many",
                            "variant": "object",
                            "through_tables": ["book_author"],
                        },
                        "children": [
                            {
                                "table": "city",
                                "columns": ["id", "name"],
                                "label": "city",
                                "relationship": {
                                    "variant": "object",
                                    "type": "one_to_one",
                                },
                                "children": [
                                    {
                                        "table": "country",
                                        "columns": ["id", "name"],
                                        "label": "country",
                                        "relationship": {
                                            "variant": "object",
                                            "type": "one_to_one",
                                        },
                                        "children": [
                                            {
                                                "table": "continent",
                                                "columns": ["name"],
                                                "label": "continent",
                                                "relationship": {
                                                    "variant": "object",
                                                    "type": "one_to_one",
                                                },
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "table": "language",
                        "label": "languages",
                        "columns": ["code"],
                        "relationship": {
                            "type": "one_to_many",
                            "variant": "scalar",
                            "through_tables": ["book_language"],
                        },
                    },
                    {
                        "table": "subject",
                        "label": "subjects",
                        "columns": ["name"],
                        "relationship": {
                            "type": "one_to_many",
                            "variant": "scalar",
                            "through_tables": ["book_subject"],
                        },
                    },
                    {
                        "table": "rating",
                        "label": "rating",
                        "columns": ["value"],
                        "relationship": {
                            "type": "one_to_one",
                            "variant": "object",
                        },
                    },
                ],
            },
        }
    ]


def rand_word(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def human_bytes(size: t.Union[int, float]) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(size) < 1024 or unit == "GB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}GB"


class Traffic:
    """Concurrent writers applying a randomized operation mix."""

    def __init__(
        self,
        engine: sa.engine.Engine,
        rng: random.Random,
        think_time: float = 0.02,
    ):
        self.engine = engine
        self.rng = rng
        self.think_time = think_time
        self.stop_event = threading.Event()
        self.counts: t.Dict[str, int] = {}
        self.errors: t.Dict[str, int] = {}
        self.timings: t.List[float] = []
        self.lock = threading.Lock()

    def bump(
        self, op: str, error: bool = False, elapsed: t.Optional[float] = None
    ) -> None:
        with self.lock:
            bucket = self.errors if error else self.counts
            bucket[op] = bucket.get(op, 0) + 1
            if elapsed is not None:
                self.timings.append(elapsed)

    def latency_ms(self) -> t.Dict[str, float]:
        with self.lock:
            values = sorted(self.timings)
        if not values:
            return {"p50": 0.0, "p95": 0.0, "max": 0.0}

        def pct(p: float) -> float:
            k = min(len(values) - 1, round(p * (len(values) - 1)))
            return round(values[k] * 1000, 1)

        return {"p50": pct(0.50), "p95": pct(0.95), "max": pct(1.0)}

    def scalar(self, conn: sa.engine.Connection, statement: str) -> t.Any:
        return conn.execute(sa.text(statement)).scalar()

    def random_id(
        self, conn: sa.engine.Connection, table: str
    ) -> t.Optional[int]:
        return self.scalar(
            conn, f"SELECT id FROM {table} ORDER BY random() LIMIT 1"
        )

    def random_isbn(self, conn: sa.engine.Connection) -> t.Optional[str]:
        return self.scalar(
            conn, "SELECT isbn FROM book ORDER BY random() LIMIT 1"
        )

    # the traffic operations applied by writer threads

    def insert_book(self, conn: sa.engine.Connection, count: int = 1) -> None:
        for _ in range(count):
            isbn = rand_word("isbn")
            publisher_id = self.random_id(conn, "publisher")
            conn.execute(
                sa.text(
                    "INSERT INTO book "
                    "(isbn, title, description, tags, publisher_id) "
                    "VALUES (:isbn, :title, :description, :tags, :pid)"
                ),
                {
                    "isbn": isbn,
                    "title": rand_word("title"),
                    "description": rand_word("desc"),
                    "tags": json.dumps(
                        [
                            rand_word("tag")
                            for _ in range(self.rng.randrange(3))
                        ]
                    ),
                    "pid": publisher_id,
                },
            )
            for _ in range(self.rng.randrange(3)):
                author_id = self.random_id(conn, "author")
                conn.execute(
                    sa.text(
                        "INSERT INTO book_author (book_isbn, author_id) "
                        "VALUES (:isbn, :aid) ON CONFLICT DO NOTHING"
                    ),
                    {"isbn": isbn, "aid": author_id},
                )
            for _ in range(self.rng.randrange(3)):
                language_id = self.random_id(conn, "language")
                conn.execute(
                    sa.text(
                        "INSERT INTO book_language (book_isbn, language_id) "
                        "VALUES (:isbn, :lid) ON CONFLICT DO NOTHING"
                    ),
                    {"isbn": isbn, "lid": language_id},
                )
            for _ in range(self.rng.randrange(3)):
                subject_id = self.random_id(conn, "subject")
                conn.execute(
                    sa.text(
                        "INSERT INTO book_subject (book_isbn, subject_id) "
                        "VALUES (:isbn, :sid) ON CONFLICT DO NOTHING"
                    ),
                    {"isbn": isbn, "sid": subject_id},
                )
            if self.rng.random() < 0.7:
                conn.execute(
                    sa.text(
                        "INSERT INTO rating (book_isbn, value) "
                        "VALUES (:isbn, :value) ON CONFLICT DO NOTHING"
                    ),
                    {"isbn": isbn, "value": self.rng.randrange(1, 11) / 2},
                )

    def op_insert_book(self, conn: sa.engine.Connection) -> None:
        self.insert_book(conn)

    def op_bulk_insert(self, conn: sa.engine.Connection) -> None:
        # a bulk load: many rows sharing one transaction (and one xid)
        self.insert_book(conn, count=self.rng.randrange(20, 40))

    def op_update_book(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        if isbn:
            conn.execute(
                sa.text(
                    "UPDATE book SET title = :title, description = :desc "
                    "WHERE isbn = :isbn"
                ),
                {
                    "title": rand_word("title"),
                    "desc": rand_word("desc"),
                    "isbn": isbn,
                },
            )

    def op_delete_book(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        if isbn:
            # through tables and rating cascade on book delete
            conn.execute(
                sa.text("DELETE FROM book WHERE isbn = :isbn"),
                {"isbn": isbn},
            )

    def op_rename_author(self, conn: sa.engine.Connection) -> None:
        author_id = self.random_id(conn, "author")
        if author_id:
            conn.execute(
                sa.text("UPDATE author SET name = :name WHERE id = :id"),
                {"name": rand_word("author"), "id": author_id},
            )

    def op_move_author(self, conn: sa.engine.Connection) -> None:
        # deep nested change: city, country and continent all shift
        author_id = self.random_id(conn, "author")
        city_id = self.random_id(conn, "city")
        if author_id and city_id:
            conn.execute(
                sa.text("UPDATE author SET city_id = :cid WHERE id = :id"),
                {"cid": city_id, "id": author_id},
            )

    def op_update_publisher(self, conn: sa.engine.Connection) -> None:
        publisher_id = self.random_id(conn, "publisher")
        if publisher_id:
            conn.execute(
                sa.text(
                    "UPDATE publisher SET is_active = NOT is_active "
                    "WHERE id = :id"
                ),
                {"id": publisher_id},
            )

    def op_update_rating(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        if isbn:
            conn.execute(
                sa.text(
                    "INSERT INTO rating (book_isbn, value) "
                    "VALUES (:isbn, :value) "
                    "ON CONFLICT (book_isbn) DO UPDATE SET value = :value"
                ),
                {"isbn": isbn, "value": self.rng.randrange(1, 11) / 2},
            )

    def op_link_author(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        author_id = self.random_id(conn, "author")
        if isbn and author_id:
            conn.execute(
                sa.text(
                    "INSERT INTO book_author (book_isbn, author_id) "
                    "VALUES (:isbn, :aid) ON CONFLICT DO NOTHING"
                ),
                {"isbn": isbn, "aid": author_id},
            )

    def op_unlink_author(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        if isbn:
            conn.execute(
                sa.text(
                    "DELETE FROM book_author WHERE id = ("
                    "SELECT id FROM book_author WHERE book_isbn = :isbn "
                    "LIMIT 1)"
                ),
                {"isbn": isbn},
            )

    def op_churn_subjects(self, conn: sa.engine.Connection) -> None:
        isbn = self.random_isbn(conn)
        subject_id = self.random_id(conn, "subject")
        if isbn and subject_id:
            if self.rng.random() < 0.5:
                conn.execute(
                    sa.text(
                        "INSERT INTO book_subject (book_isbn, subject_id) "
                        "VALUES (:isbn, :sid) ON CONFLICT DO NOTHING"
                    ),
                    {"isbn": isbn, "sid": subject_id},
                )
            else:
                conn.execute(
                    sa.text(
                        "DELETE FROM book_subject "
                        "WHERE book_isbn = :isbn AND subject_id = :sid"
                    ),
                    {"isbn": isbn, "sid": subject_id},
                )

    OPS: t.List[t.Tuple[str, float]] = [
        ("insert_book", 0.30),
        ("update_book", 0.20),
        ("delete_book", 0.08),
        ("rename_author", 0.06),
        ("move_author", 0.05),
        ("update_publisher", 0.05),
        ("update_rating", 0.08),
        ("link_author", 0.07),
        ("unlink_author", 0.05),
        ("churn_subjects", 0.05),
        ("bulk_insert", 0.01),
    ]

    def writer(self) -> None:
        names = [name for name, _ in self.OPS]
        weights = [weight for _, weight in self.OPS]
        while not self.stop_event.is_set():
            op = self.rng.choices(names, weights=weights, k=1)[0]
            op_started = time.monotonic()
            try:
                with self.engine.begin() as conn:
                    getattr(self, f"op_{op}")(conn)
                self.bump(op, elapsed=time.monotonic() - op_started)
            except sa.exc.SQLAlchemyError:
                # unique conflicts and delete races are expected traffic
                self.bump(op, error=True)
            # keep the WAL volume proportional to the cycle duration
            self.stop_event.wait(self.think_time * self.rng.random() * 2)

    def long_transaction_writer(self, hold_seconds: float) -> None:
        """Holds a write transaction open across pull cycles."""
        while not self.stop_event.is_set():
            try:
                with self.engine.begin() as conn:
                    self.insert_book(conn)
                    # the xid is assigned now; commit happens much later
                    self.stop_event.wait(hold_seconds)
                self.bump("long_transaction")
            except sa.exc.SQLAlchemyError:
                self.bump("long_transaction", error=True)
            self.stop_event.wait(1.0)


class Verifier:
    """Rebuilds expected documents with plain SQL and diffs the index.

    Takes the sync holder rather than a Sync so restart simulations can
    swap the instance underneath it.
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
        holder: t.Dict[str, Sync],
        index: str,
    ):
        self.engine = engine
        self.holder = holder
        self.index = index

    @property
    def sync(self) -> Sync:
        return self.holder["sync"]

    def db_state(self) -> t.Dict[str, dict]:
        docs: t.Dict[str, dict] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT b.id, b.isbn, b.title, b.description, b.tags, "
                    "p.id, p.name, p.is_active "
                    "FROM book b JOIN publisher p ON p.id = b.publisher_id"
                )
            ).fetchall()
            for (
                book_id,
                isbn,
                title,
                description,
                tags,
                pub_id,
                pub_name,
                pub_active,
            ) in rows:
                docs[str(book_id)] = {
                    "id": book_id,
                    "isbn": isbn,
                    "title": title,
                    "description": description,
                    "tags": tags,
                    "publisher": {
                        "id": pub_id,
                        "name": pub_name,
                        "is_active": pub_active,
                    },
                    "authors": [],
                    "languages": [],
                    "subjects": [],
                    "rating": None,
                }
            isbn_to_id = {doc["isbn"]: key for key, doc in docs.items()}

            rows = conn.execute(
                sa.text(
                    "SELECT ba.book_isbn, a.id, a.name, "
                    "ci.id, ci.name, co.id, co.name, cn.name "
                    "FROM book_author ba "
                    "JOIN author a ON a.id = ba.author_id "
                    "JOIN city ci ON ci.id = a.city_id "
                    "JOIN country co ON co.id = ci.country_id "
                    "JOIN continent cn ON cn.id = co.continent_id"
                )
            ).fetchall()
            for (
                isbn,
                author_id,
                author_name,
                city_id,
                city_name,
                country_id,
                country_name,
                continent_name,
            ) in rows:
                key = isbn_to_id.get(isbn)
                if key:
                    docs[key]["authors"].append(
                        {
                            "id": author_id,
                            "name": author_name,
                            "city": {
                                "id": city_id,
                                "name": city_name,
                                "country": {
                                    "id": country_id,
                                    "name": country_name,
                                    "continent": {"name": continent_name},
                                },
                            },
                        }
                    )

            for field, statement in (
                (
                    "languages",
                    "SELECT bl.book_isbn, l.code FROM book_language bl "
                    "JOIN language l ON l.id = bl.language_id",
                ),
                (
                    "subjects",
                    "SELECT bs.book_isbn, s.name FROM book_subject bs "
                    "JOIN subject s ON s.id = bs.subject_id",
                ),
            ):
                for isbn, value in conn.execute(sa.text(statement)):
                    key = isbn_to_id.get(isbn)
                    if key:
                        docs[key][field].append(value)

            rows = conn.execute(
                sa.text("SELECT book_isbn, value FROM rating")
            ).fetchall()
            for isbn, value in rows:
                key = isbn_to_id.get(isbn)
                if key:
                    docs[key]["rating"] = {"value": value}
        return docs

    def es_state(self) -> t.Dict[str, dict]:
        docs: t.Dict[str, dict] = {}
        client = getattr(self.sync.search_client, "_SearchClient__client")
        body: dict = {"query": {"match_all": {}}}
        response = client.search(
            index=self.index, body=body, scroll="1m", size=1000
        )
        while True:
            hits = response["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                docs[hit["_id"]] = hit["_source"]
            response = client.scroll(
                scroll_id=response["_scroll_id"], scroll="1m"
            )
        return docs

    def index_size_bytes(self) -> int:
        client = getattr(self.sync.search_client, "_SearchClient__client")
        stats = client.indices.stats(index=self.index)
        return int(stats["_all"]["total"]["store"]["size_in_bytes"])

    @staticmethod
    def normalize(doc: dict) -> dict:
        out = dict(doc)
        out.pop("_meta", None)
        authors = out.get("authors") or []
        out["authors"] = sorted(authors, key=lambda a: a.get("id") or 0)
        for field in ("languages", "subjects"):
            values = out.get(field) or []
            out[field] = sorted(v for v in values if v is not None)
        if not out.get("rating"):
            out["rating"] = None
        if not out.get("tags"):
            out["tags"] = []
        return out

    def verify(self) -> dict:
        expected = self.db_state()
        actual = self.es_state()

        missing = sorted(set(expected) - set(actual), key=int)
        stale = sorted(set(actual) - set(expected), key=int)
        mismatched: t.List[dict] = []
        for key in set(expected) & set(actual):
            want = self.normalize(expected[key])
            got = self.normalize(actual[key])
            if want != got:
                mismatched.append({"id": key, "want": want, "got": got})

        return {
            "books_in_db": len(expected),
            "docs_in_es": len(actual),
            "missing": missing,
            "stale": stale,
            "mismatched": mismatched,
            "ok": not missing and not stale and not mismatched,
        }


class WarningCounter(logging.Handler):
    """Counts pinned-checkpoint warnings emitted by pgsync during a run."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.pinned: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        if "Checkpoint pinned" in record.getMessage():
            self.pinned += 1


def seed(engine: sa.engine.Engine, rng: random.Random) -> None:
    with engine.begin() as conn:
        for statement in DDL:
            conn.execute(sa.text(statement))
        continents = ["Europe", "Asia", "Americas"]
        for name in continents:
            conn.execute(
                sa.text("INSERT INTO continent (name) VALUES (:name)"),
                {"name": name},
            )
        for index in range(6):
            conn.execute(
                sa.text(
                    "INSERT INTO country (name, continent_id) "
                    "VALUES (:name, :cid)"
                ),
                {"name": rand_word("country"), "cid": (index % 3) + 1},
            )
        for index in range(12):
            conn.execute(
                sa.text(
                    "INSERT INTO city (name, country_id) "
                    "VALUES (:name, :cid)"
                ),
                {"name": rand_word("city"), "cid": (index % 6) + 1},
            )
        for _ in range(8):
            conn.execute(
                sa.text(
                    "INSERT INTO publisher (name, is_active) "
                    "VALUES (:name, TRUE)"
                ),
                {"name": rand_word("publisher")},
            )
        for index in range(30):
            conn.execute(
                sa.text(
                    "INSERT INTO author (name, city_id) "
                    "VALUES (:name, :cid)"
                ),
                {"name": rand_word("author"), "cid": (index % 12) + 1},
            )
        for _ in range(6):
            conn.execute(
                sa.text("INSERT INTO language (code) VALUES (:code)"),
                {"code": rand_word("lang")},
            )
        for _ in range(10):
            conn.execute(
                sa.text("INSERT INTO subject (name) VALUES (:name)"),
                {"name": rand_word("subject")},
            )
    # initial books so update and delete ops have targets immediately
    traffic = Traffic(engine, rng)
    with engine.begin() as conn:
        traffic.insert_book(conn, count=100)


@click.command()
@click.option("--cycles", default=3, show_default=True, help="Verify cycles")
@click.option(
    "--duration",
    default=10.0,
    show_default=True,
    help="Seconds of traffic per cycle",
)
@click.option(
    "--writers", default=6, show_default=True, help="Concurrent writers"
)
@click.option(
    "--pull-interval",
    default=0.25,
    show_default=True,
    help="Seconds between pgsync polling pulls",
)
@click.option("--seed", "seed_value", default=42, show_default=True)
@click.option(
    "--think-time",
    default=0.02,
    show_default=True,
    help="Mean pause between writer operations in seconds",
)
@click.option(
    "--keep",
    is_flag=True,
    help="Keep the database, index and slot after the run",
)
@click.option(
    "--soak",
    is_flag=True,
    help="Run cycles until interrupted; Ctrl+C verifies then exits",
)
@click.option(
    "--restart-sync",
    is_flag=True,
    help="Simulate a pgsync crash and restart mid-cycle, under traffic",
)
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write a JSON report of every cycle to this path",
)
def main(
    cycles: int,
    duration: float,
    writers: int,
    pull_interval: float,
    seed_value: int,
    think_time: float,
    keep: bool,
    soak: bool,
    restart_sync: bool,
    report_path: t.Optional[str],
) -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    rng = random.Random(seed_value)
    name: str = f"{QA_PREFIX}_{uuid.uuid4().hex[:6]}"
    schema_path: Path = Path(__file__).parent / f".{name}.json"

    click.echo(f"database/index: {name}")
    create_database(name)
    engine: sa.engine.Engine = sa.create_engine(
        get_database_url(name), pool_size=writers + 4, max_overflow=8
    )
    seed(engine, rng)

    schema_path.write_text(json.dumps(build_schema(name, name), indent=2))
    doc: dict = next(config_loader(config=str(schema_path)))

    def make_sync() -> Sync:
        instance: Sync = Sync(doc, validate=True, repl_slots=False)
        # the per-batch progress bar would swamp the harness output
        instance.log_xlog_progress = lambda *args, **kwargs: None
        return instance

    holder: t.Dict[str, Sync] = {"sync": make_sync()}
    holder["sync"].setup()

    warning_counter = WarningCounter()
    logging.getLogger("pgsync.sync").addHandler(warning_counter)

    # continuous polling sync in the background; pull_lock serializes
    # the background poller against the drain pulls in the main thread
    sync_stop = threading.Event()
    sync_errors: t.List[str] = []
    pull_lock = threading.Lock()

    def pull_loop() -> None:
        while not sync_stop.is_set():
            try:
                with pull_lock:
                    holder["sync"].pull(polling=True)
            except Exception as exc:  # noqa: BLE001
                sync_errors.append(repr(exc))
            sync_stop.wait(pull_interval)

    pull_thread = threading.Thread(target=pull_loop, daemon=True)
    pull_thread.start()

    def restart_sync_under_traffic(cycle: int) -> threading.Thread:
        """Kill the running Sync and start a fresh one, like a restart.

        The new instance reloads the checkpoint from disk and resumes
        from the replication slot, while writers keep committing during
        the outage window.
        """
        nonlocal pull_thread
        click.echo(f"cycle {cycle}: simulating pgsync restart...")
        sync_stop.set()
        pull_thread.join(timeout=300)
        Singleton._instances = {}
        holder["sync"] = make_sync()
        time.sleep(1.0)  # outage window: traffic continues uncaptured
        sync_stop.clear()
        pull_thread = threading.Thread(target=pull_loop, daemon=True)
        pull_thread.start()
        return pull_thread

    verifier = Verifier(engine, holder, name)
    results: t.List[dict] = []
    failed: bool = False
    interrupted: bool = False

    cycle_iter: t.Iterable[int] = (
        itertools.count(1) if soak else range(1, cycles + 1)
    )
    try:
        for cycle in cycle_iter:
            traffic = Traffic(engine, rng, think_time=think_time)
            threads = [
                threading.Thread(target=traffic.writer, daemon=True)
                for _ in range(writers)
            ]
            threads.append(
                threading.Thread(
                    target=traffic.long_transaction_writer,
                    args=(min(6.0, duration * 0.8),),
                    daemon=True,
                )
            )
            started = time.monotonic()
            for thread in threads:
                thread.start()
            try:
                if restart_sync:
                    time.sleep(duration / 2)
                    restart_sync_under_traffic(cycle)
                    time.sleep(duration / 2)
                else:
                    time.sleep(duration)
            except KeyboardInterrupt:
                interrupted = True
                click.echo(
                    "interrupted: stopping traffic for a final verification"
                )
            traffic.stop_event.set()
            for thread in threads:
                thread.join(timeout=30)
            elapsed = time.monotonic() - started

            # replication backlog accumulated while traffic was running
            with engine.connect() as conn:
                backlog_bytes = (
                    conn.execute(
                        sa.text(
                            "SELECT PG_WAL_LSN_DIFF(PG_CURRENT_WAL_LSN(), "
                            "confirmed_flush_lsn) "
                            "FROM pg_replication_slots "
                            "WHERE slot_name = :slot"
                        ),
                        {"slot": f"{name}_{name}"},
                    ).scalar()
                    or 0
                )

            # drain: serialize against the poller, then pull twice so
            # anything committed during the first pull is also captured
            click.echo(f"cycle {cycle}: draining...")
            synced_before = dict(holder["sync"].count)
            drain_started = time.monotonic()
            with pull_lock:
                holder["sync"].pull(polling=True)
                holder["sync"].pull(polling=True)
            holder["sync"].search_client.refresh([name])
            drain_seconds = time.monotonic() - drain_started
            docs_synced = sum(holder["sync"].count.values()) - sum(
                synced_before.values()
            )

            verify_started = time.monotonic()
            report = verifier.verify()
            verify_seconds = time.monotonic() - verify_started

            ops = sum(traffic.counts.values())
            report.update(
                {
                    "cycle": cycle,
                    "ops": ops,
                    "ops_per_second": round(ops / elapsed, 1),
                    "op_latency_ms": traffic.latency_ms(),
                    "backlog_bytes": int(backlog_bytes),
                    "drain_seconds": round(drain_seconds, 1),
                    "docs_synced": docs_synced,
                    "sync_docs_per_second": (
                        round(docs_synced / drain_seconds, 1)
                        if drain_seconds
                        else 0.0
                    ),
                    "verify_seconds": round(verify_seconds, 1),
                    "op_counts": dict(sorted(traffic.counts.items())),
                    "op_conflicts": dict(sorted(traffic.errors.items())),
                }
            )
            results.append(report)
            status = (
                click.style("OK", fg="green")
                if report["ok"]
                else click.style("FAILED", fg="red")
            )
            click.echo(
                f"cycle {cycle}: {status} ops={ops} "
                f"({report['ops_per_second']}/s) "
                f"db={report['books_in_db']} es={report['docs_in_es']} "
                f"missing={len(report['missing'])} "
                f"stale={len(report['stale'])} "
                f"mismatched={len(report['mismatched'])}"
            )
            if not report["ok"]:
                failed = True
                for key in ("missing", "stale"):
                    if report[key]:
                        click.echo(f"  {key}: {report[key][:10]}")
                for item in report["mismatched"][:3]:
                    click.echo(f"  mismatch id={item['id']}")
                    click.echo(f"    want: {json.dumps(item['want'])[:400]}")
                    click.echo(f"    got:  {json.dumps(item['got'])[:400]}")
                break
            if interrupted:
                break
    finally:
        sync_stop.set()
        pull_thread.join(timeout=60)

        click.echo(
            f"pinned-checkpoint warnings observed: {warning_counter.pinned} "
            f"(long transactions were exercised)"
        )
        if sync_errors:
            failed = True
            click.echo(f"sync errors: {sync_errors[:5]}")

        try:
            index_size_bytes = verifier.index_size_bytes()
        except Exception:  # noqa: BLE001
            index_size_bytes = 0

        summary: dict = {
            "cycles_run": len(results),
            "cycles_passed": sum(1 for r in results if r["ok"]),
            "total_ops": sum(r["ops"] for r in results),
            "final_books": results[-1]["books_in_db"] if results else 0,
            "mean_ops_per_second": (
                round(
                    sum(r["ops_per_second"] for r in results) / len(results),
                    1,
                )
                if results
                else 0
            ),
            "total_docs_synced": sum(r.get("docs_synced", 0) for r in results),
            "index_size_bytes": index_size_bytes,
            "pinned_checkpoint_warnings": warning_counter.pinned,
            "restart_sync": restart_sync,
            "sync_errors": sync_errors,
            "passed": not failed,
        }

        if results:
            header = (
                f"{'cycle':>5} {'ops':>6} {'ops/s':>7} {'p95 ms':>7} "
                f"{'backlog':>9} {'drain s':>8} {'sync/s':>7} "
                f"{'books':>7} {'result':>7}"
            )
            click.echo(header)
            click.echo("-" * len(header))
            for r in results:
                # pad before styling: ANSI codes would skew the column
                verdict = "OK" if r["ok"] else "FAILED"
                result_cell = click.style(
                    f"{verdict:>7}", fg="green" if r["ok"] else "red"
                )
                click.echo(
                    f"{r['cycle']:>5} {r['ops']:>6} "
                    f"{r['ops_per_second']:>7} "
                    f"{r.get('op_latency_ms', {}).get('p95', 0):>7} "
                    f"{human_bytes(r.get('backlog_bytes', 0)):>9} "
                    f"{r.get('drain_seconds', 0):>8} "
                    f"{r.get('sync_docs_per_second', 0):>7} "
                    f"{r['books_in_db']:>7} {result_cell}"
                )
            click.echo("-" * len(header))
        click.echo(
            f"summary: {summary['cycles_passed']}/{summary['cycles_run']} "
            f"cycles passed, {summary['total_ops']} ops at "
            f"{summary['mean_ops_per_second']}/s mean, "
            f"{summary['total_docs_synced']} docs synced, "
            f"{summary['final_books']} books at rest, "
            f"index {human_bytes(index_size_bytes)}"
        )

        artifact: dict = {
            "config": {
                "database": name,
                "duration": duration,
                "writers": writers,
                "think_time": think_time,
                "pull_interval": pull_interval,
                "seed": seed_value,
                "restart_sync": restart_sync,
            },
            "summary": summary,
            "cycles": results,
        }
        if report_path:
            Path(report_path).write_text(json.dumps(artifact, indent=2))
            click.echo(f"report written to {report_path}")
        elif failed:
            failure_path = Path(__file__).parent / f".failure_{name}.json"
            failure_path.write_text(json.dumps(artifact, indent=2))
            click.echo(f"failure artifacts written to {failure_path}")

        if keep or failed:
            click.echo(
                f"keeping database {name}, index {name} and schema "
                f"{schema_path} for inspection"
            )
        else:
            holder["sync"].teardown(drop_view=True)
            holder["sync"].search_client.teardown(name)
            engine.dispose()
            # pgsync's own pools may still hold sessions: force the drop
            admin = sa.create_engine(
                get_database_url("postgres"), isolation_level="AUTOCOMMIT"
            )
            with admin.connect() as conn:
                conn.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
                )
            admin.dispose()
            schema_path.unlink(missing_ok=True)
            click.echo("teardown complete")

    if failed:
        raise SystemExit(1)
    click.echo(
        f"all {len(results)} cycles passed: no data loss, "
        f"no stale documents, no field mismatches"
    )


if __name__ == "__main__":
    main()
