"""Tests for the root document resolution of child changes."""

import json
import select
import time
import typing as t
from contextlib import contextmanager

import mock
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from pgsync.base import Payload, subtransactions
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.singleton import Singleton
from pgsync.sync import Sync

from .testing_utils import search

INDEXES = [
    "author_city_id_idx",
    "book_author_author_id_idx",
    "book_buyer_id_idx",
    "book_publisher_id_idx",
]

TABLES = [
    "book_author",
    "book_group",
    '"group"',
    "rating",
    "book",
    "publisher",
    "author",
    "city",
    "country",
    "continent",
]

REPLICA_IDENTITY_TABLES = ["rating", "book_author"]

# objects that tests make, dropped before and after each test so that a
# stopped run leaves nothing behind
DROP_STATEMENTS = [
    "DROP TABLE IF EXISTS review",
    "DROP TABLE IF EXISTS stack_label",
    "DROP TABLE IF EXISTS stack",
    "DROP TYPE IF EXISTS stack_code",
    "DROP SCHEMA IF EXISTS other CASCADE",
    "DROP TABLE IF EXISTS public.reader",
]


def object_child(
    table: str,
    columns: t.List[str],
    kind: str = "one_to_one",
    **relationship: t.Any,
) -> dict:
    """Return a child node with an object relationship."""
    return {
        "table": table,
        "columns": columns,
        "relationship": {"variant": "object", "type": kind, **relationship},
    }


BOOK_RATING = {
    "table": "book",
    "columns": ["isbn", "title"],
    "children": [object_child("rating", ["id", "value"])],
}

PUBLISHER_BOOK_RATING = {
    "table": "publisher",
    "columns": ["id", "name"],
    "children": [
        {
            **object_child("book", ["isbn", "title"], "one_to_many"),
            "children": [object_child("rating", ["id", "value"])],
        }
    ],
}

BOOK_AUTHOR_CITY = {
    "table": "book",
    "columns": ["isbn", "title"],
    "children": [
        {
            **object_child(
                "author",
                ["id", "name"],
                "one_to_many",
                through_tables=["book_author"],
            ),
            "children": [object_child("city", ["id", "name"])],
        }
    ],
}

PUBLISHER_BOOK_AUTHOR = {
    "table": "publisher",
    "columns": ["id", "name"],
    "children": [
        {
            **object_child("book", ["isbn", "title"], "one_to_many"),
            "children": [
                object_child(
                    "author",
                    ["id", "name"],
                    "one_to_many",
                    through_tables=["book_author"],
                )
            ],
        }
    ],
}


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestRootResolution(object):
    """Root document resolution for child changes."""

    @pytest.fixture(scope="function")
    def cleanup(self, sync):
        session = sync.session
        with subtransactions(session):
            for statement in DROP_STATEMENTS:
                session.execute(sa.text(statement))
        yield session
        with subtransactions(session):
            for statement in DROP_STATEMENTS:
                session.execute(sa.text(statement))
            for index in INDEXES:
                session.execute(sa.text(f"DROP INDEX IF EXISTS {index}"))
            for table in REPLICA_IDENTITY_TABLES:
                session.execute(
                    sa.text(f"ALTER TABLE {table} REPLICA IDENTITY DEFAULT")
                )
            session.execute(
                sa.text(
                    f"TRUNCATE TABLE {', '.join(TABLES)} "
                    "RESTART IDENTITY CASCADE"
                )
            )
        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )
        sync.search_client.teardown(index="testdb")
        sync.redis.delete()

    # helpers

    def _add(self, session: Session, *rows: t.Any) -> None:
        """Add each row in its own transaction, in order."""
        for row in rows:
            with subtransactions(session):
                session.add(row)

    def _execute(self, session: Session, statement: sa.sql.Executable) -> None:
        with subtransactions(session):
            session.execute(statement)

    def _index(
        self, session: Session, name: str, table: str, column: str
    ) -> None:
        self._execute(
            session, sa.text(f"CREATE INDEX {name} ON {table} ({column})")
        )

    def _replica_identity_full(self, session: Session, table: str) -> None:
        self._execute(
            session, sa.text(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
        )

    def _sync(
        self, session: Session, nodes: dict, refresh: bool = True
    ) -> Sync:
        """Index the current rows, and return a Sync for the nodes.

        If refresh is False, the index has no refresh. Then the documents
        are in the index, but a search does not find them.
        """
        Singleton._instances = {}
        doc = {"index": "testdb", "database": "testdb", "nodes": nodes}
        if not refresh:
            doc["setting"] = {"refresh_interval": "-1"}
        sync = Sync(doc)
        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )
        sync.search_client.bulk(sync.index, sync.sync())
        if refresh:
            sync.search_client.refresh("testdb")
        return sync

    def _close(self, sync: Sync) -> None:
        sync.search_client.close()
        sync.session.close()
        sync.engine.dispose(close=True)
        Singleton._instances = {}

    def _docs(self, sync: Sync, key: str) -> t.Dict[t.Any, dict]:
        """Apply the pending changes, and return the docs by root key."""
        sync.logical_slot_changes()
        sync.search_client.refresh("testdb")
        docs = {doc[key]: doc for doc in search(sync.search_client, "testdb")}
        self._close(sync)
        return docs

    def _root_filters(
        self, sync: Sync, payloads: t.List[Payload]
    ) -> t.List[dict]:
        """Return the root filters that the payloads give to the sync."""
        root_filters: t.List[dict] = []

        def _sync(
            filters: t.Optional[dict] = None, **kwargs: t.Any
        ) -> t.Iterator[dict]:
            root_filters.extend(filters[sync.tree.root.table])
            return iter([])

        with mock.patch.object(sync, "sync", side_effect=_sync):
            list(sync._payloads(payloads))
        self._close(sync)
        return root_filters

    @contextmanager
    def _new_sync(self, nodes: dict) -> t.Iterator[Sync]:
        """Yield a Sync for the nodes, and close it after."""
        Singleton._instances = {}
        sync = Sync(
            {"index": "testdb", "database": "testdb", "nodes": nodes},
            validate=False,
            repl_slots=False,
        )
        try:
            yield sync
        finally:
            self._close(sync)

    def _path_query(
        self, nodes: dict, table: str
    ) -> t.Optional[sa.sql.Select]:
        """Return the root path query of the table in the nodes."""
        with self._new_sync(nodes) as sync:
            return sync._root_path_query(sync.tree.get_node(table, "public"))

    def _two_books_with_ratings(
        self, session: Session, book_cls: type, rating_cls: type
    ) -> None:
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
            rating_cls(id=1, book_isbn="abc", value=1.1),
            rating_cls(id=2, book_isbn="def", value=2.2),
        )

    def _author(
        self,
        session: Session,
        continent_cls: type,
        country_cls: type,
        city_cls: type,
        author_cls: type,
    ) -> None:
        """Add author 1, Hugo, in Paris."""
        self._add(
            session,
            continent_cls(id=1, name="Europe"),
            country_cls(id=1, name="France", continent_id=1),
            city_cls(id=1, name="Paris", country_id=1),
            author_cls(id=1, name="Hugo", city_id=1),
        )

    @contextmanager
    def _triggers(self, sync: Sync) -> t.Iterator[t.Any]:
        """Install the triggers, and yield a connection that listens."""
        slot = f"{sync.database}_testdb"
        sync.setup()
        conn = sync.engine.raw_connection()
        try:
            listener = conn.driver_connection
            listener.set_session(autocommit=True)
            listener.cursor().execute(f"LISTEN {sync.database}")
            yield listener
        finally:
            conn.close()
            # teardown drops the replication slot that the tests use
            sync.teardown()
            if not sync.replication_slots(slot):
                sync.create_replication_slot(slot)

    def _notifications(
        self, listener: t.Any, count: int = 1, timeout: float = 5.0
    ) -> t.List[dict]:
        """Wait for count notifications, and return their payloads."""
        deadline = time.monotonic() + timeout
        listener.poll()
        while len(listener.notifies) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            select.select([listener], [], [], remaining)
            listener.poll()
        payloads = [json.loads(n.payload) for n in listener.notifies]
        listener.notifies.clear()
        return payloads

    # a child that moves to a different root

    def test_move_child_names_differ(self, cleanup, book_cls, rating_cls):
        """A direct child that moves to a different root is in that root.

        The child primary key (rating.id) and the referenced root column
        (book.isbn) have different names.
        """
        session = cleanup
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, BOOK_RATING)
        table = rating_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(book_isbn="def"),
        )

        docs = self._docs(sync, "isbn")
        assert docs["abc"]["rating"] is None
        assert docs["def"]["rating"] == {"id": 1, "value": 1.1}

    def test_move_child_id_names(self, cleanup, continent_cls, country_cls):
        """A direct child that moves to a different root is in that root.

        The child primary key (country.id) and the referenced root column
        (continent.id) have the same name.
        """
        session = cleanup
        self._add(
            session,
            continent_cls(id=1, name="Europe"),
            continent_cls(id=2, name="Asia"),
            country_cls(id=1, name="France", continent_id=1),
        )
        sync = self._sync(
            session,
            {
                "table": "continent",
                "columns": ["id", "name"],
                "children": [
                    object_child("country", ["id", "name"], "one_to_many")
                ],
            },
        )
        table = country_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(continent_id=2),
        )

        docs = self._docs(sync, "id")
        assert docs[1]["country"] is None
        assert docs[2]["country"] == [{"id": 1, "name": "France"}]

    @pytest.mark.parametrize("refresh", [True, False])
    def test_move_grandchild_names_differ(
        self, cleanup, publisher_cls, book_cls, rating_cls, refresh
    ):
        """A grandchild that moves to a different root is in that root.

        If the search does not find the old root, the old root comes from
        the old foreign key. With WAL, the old row has it only with
        REPLICA IDENTITY FULL.
        """
        session = cleanup
        if not refresh:
            self._replica_identity_full(session, "rating")
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            publisher_cls(id=2, name="Lion Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
            book_cls(isbn="def", title="The Lion Club", publisher_id=2),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, PUBLISHER_BOOK_RATING, refresh=refresh)
        table = rating_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(book_isbn="def"),
        )

        docs = self._docs(sync, "id")
        assert docs[1]["book"][0]["rating"] is None
        assert docs[2]["book"][0]["rating"] == {"id": 1, "value": 1.1}

    # a change before the root is searchable

    def test_update_grandchild_before_root_is_searchable(
        self, cleanup, publisher_cls, book_cls, rating_cls
    ):
        """pgsync syncs a grandchild update before a search finds the root."""
        session = cleanup
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, PUBLISHER_BOOK_RATING, refresh=False)
        table = rating_cls.__table__
        self._execute(
            session, table.update().where(table.c.id == 1).values(value=9.9)
        )

        docs = self._docs(sync, "id")
        assert docs[1]["book"][0]["rating"] == {"id": 1, "value": 9.9}

    def test_insert_grandchild_before_root_is_searchable(
        self, cleanup, publisher_cls, book_cls, rating_cls
    ):
        """pgsync syncs a grandchild insert before a search finds the root.

        The payload has no root key, so the root comes from a database
        query of the new row.
        """
        session = cleanup
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
        )
        sync = self._sync(session, PUBLISHER_BOOK_RATING, refresh=False)
        self._add(session, rating_cls(id=1, book_isbn="abc", value=1.1))

        docs = self._docs(sync, "id")
        assert docs[1]["book"][0]["rating"] == {"id": 1, "value": 1.1}

    @pytest.mark.parametrize("refresh", [True, False])
    def test_insert_child_with_schema_foreign_key(
        self, cleanup, book_cls, refresh
    ):
        """pgsync syncs a child insert with a foreign key only in the schema.

        The review table has no database foreign key constraint, and the
        column names (review.book_ref, book.isbn) are different.
        """
        session = cleanup
        self._execute(
            session,
            sa.text(
                "CREATE TABLE review ("
                "id INTEGER PRIMARY KEY, "
                "book_ref VARCHAR(256), "
                "body VARCHAR(256))"
            ),
        )
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
        )
        sync = self._sync(
            session,
            {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    object_child(
                        "review",
                        ["id", "body"],
                        "one_to_many",
                        foreign_key={
                            "child": ["book_ref"],
                            "parent": ["isbn"],
                        },
                    )
                ],
            },
            refresh=refresh,
        )
        self._execute(
            session,
            sa.text(
                "INSERT INTO review (id, book_ref, body) "
                "VALUES (1, 'def', 'Great')"
            ),
        )

        docs = self._docs(sync, "isbn")
        assert docs["abc"]["review"] is None
        assert docs["def"]["review"] == [{"id": 1, "body": "Great"}]

    def test_delete_child_before_root_is_searchable(
        self, cleanup, book_cls, rating_cls
    ):
        """pgsync syncs a child delete before a search finds the root.

        With REPLICA IDENTITY FULL, the WAL delete holds the foreign key.
        """
        session = cleanup
        self._replica_identity_full(session, "rating")
        self._two_books_with_ratings(session, book_cls, rating_cls)
        sync = self._sync(session, BOOK_RATING, refresh=False)
        table = rating_cls.__table__
        self._execute(session, table.delete().where(table.c.id == 1))

        docs = self._docs(sync, "isbn")
        assert docs["abc"]["rating"] is None
        assert docs["def"]["rating"] == {"id": 2, "value": 2.2}

    def test_delete_grandchild_before_root_is_searchable(
        self, cleanup, publisher_cls, book_cls, rating_cls
    ):
        """pgsync syncs a grandchild delete before a search finds the root.

        The deleted row does not exist, so the root comes from its parent.
        """
        session = cleanup
        self._replica_identity_full(session, "rating")
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, PUBLISHER_BOOK_RATING, refresh=False)
        table = rating_cls.__table__
        self._execute(session, table.delete().where(table.c.id == 1))

        docs = self._docs(sync, "id")
        assert docs[1]["book"][0]["rating"] is None

    def test_update_through_grandchild_before_root_is_searchable(
        self,
        cleanup,
        continent_cls,
        country_cls,
        city_cls,
        author_cls,
        book_cls,
        book_author_cls,
    ):
        """pgsync syncs an update below a through table before a search
        finds the root."""
        session = cleanup
        self._author(session, continent_cls, country_cls, city_cls, author_cls)
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
            book_author_cls(id=1, book_isbn="abc", author_id=1),
        )
        self._index(session, "author_city_id_idx", "author", "city_id")
        self._index(
            session, "book_author_author_id_idx", "book_author", "author_id"
        )
        sync = self._sync(session, BOOK_AUTHOR_CITY, refresh=False)
        table = city_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(name="Lyon"),
        )

        docs = self._docs(sync, "isbn")
        assert docs["abc"]["author"][0]["city"] == {"id": 1, "name": "Lyon"}
        assert docs["def"]["author"] is None

    @pytest.mark.parametrize("change", ["insert", "move", "delete"])
    def test_through_row_below_first_level_before_root_is_searchable(
        self,
        cleanup,
        continent_cls,
        country_cls,
        city_cls,
        author_cls,
        publisher_cls,
        book_cls,
        book_author_cls,
        change,
    ):
        """pgsync syncs a through table row below the first level before a
        search finds the root.

        publisher -> book -> [book_author] -> author. The through table has
        no foreign key to the root, so the root comes from the linked book.
        """
        session = cleanup
        self._replica_identity_full(session, "book_author")
        self._author(session, continent_cls, country_cls, city_cls, author_cls)
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            publisher_cls(id=2, name="Lion Books"),
            book_cls(isbn="abc", title="A", publisher_id=1),
            book_cls(isbn="def", title="B", publisher_id=2),
        )
        if change != "insert":
            self._add(
                session, book_author_cls(id=1, book_isbn="abc", author_id=1)
            )
        sync = self._sync(session, PUBLISHER_BOOK_AUTHOR, refresh=False)
        table = book_author_cls.__table__
        self._execute(
            session,
            {
                "insert": table.insert().values(
                    id=1, book_isbn="abc", author_id=1
                ),
                "move": table.update()
                .where(table.c.id == 1)
                .values(book_isbn="def"),
                "delete": table.delete().where(table.c.id == 1),
            }[change],
        )

        docs = self._docs(sync, "id")
        hugo = [{"id": 1, "name": "Hugo"}]
        expected = {
            "insert": (hugo, None),
            "move": (None, hugo),
            "delete": (None, None),
        }[change]
        assert docs[1]["book"][0]["author"] == expected[0]
        assert docs[2]["book"][0]["author"] == expected[1]

    # the roots that a change re-syncs

    def test_update_child_fk_on_parent(self, cleanup, publisher_cls, book_cls):
        """A child update re-syncs only the roots that reference it.

        The foreign key is on the parent (book.publisher_id).
        """
        session = cleanup
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            publisher_cls(id=2, name="Lion Books"),
            book_cls(isbn="abc", title="A", publisher_id=1),
            book_cls(isbn="def", title="B", publisher_id=1),
            book_cls(isbn="ghi", title="C", publisher_id=2),
        )
        sync = self._sync(
            session,
            {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [object_child("publisher", ["id", "name"])],
            },
        )
        roots = []
        update_op = sync._update_op

        def _update_op(node, filters, payloads):
            filters = update_op(node, filters, payloads)
            roots.extend(where["isbn"] for where in filters["book"])
            return filters

        table = publisher_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(name="Tigers"),
        )

        with mock.patch.object(sync, "_update_op", side_effect=_update_op):
            docs = self._docs(sync, "isbn")

        assert set(roots) == {"abc", "def"}
        assert docs["abc"]["publisher"]["name"] == "Tigers"
        assert docs["def"]["publisher"]["name"] == "Tigers"
        assert docs["ghi"]["publisher"]["name"] == "Lion Books"

    def test_child_updates_on_one_root_give_one_root_filter(
        self, cleanup, book_cls, rating_cls
    ):
        """Many updates of one child row give one root filter."""
        session = cleanup
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, BOOK_RATING)
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="rating",
                schema="public",
                old={"id": 1, "book_isbn": "abc"},
                new={"id": 1, "book_isbn": "abc", "value": float(i)},
            )
            for i in range(10000)
        ]

        assert self._root_filters(sync, payloads) == [{"isbn": "abc"}]

    @pytest.mark.parametrize("move", [False, True])
    def test_child_update_queries(
        self, cleanup, publisher_cls, book_cls, rating_cls, move
    ):
        """A child update queries the old root only if the row moves."""
        session = cleanup
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            publisher_cls(id=2, name="Lion Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
            book_cls(isbn="def", title="The Lion Club", publisher_id=2),
            rating_cls(id=1, book_isbn="def" if move else "abc", value=1.1),
        )
        # with no refresh, only the database queries find the roots
        sync = self._sync(session, PUBLISHER_BOOK_RATING, refresh=False)
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="rating",
                schema="public",
                old={"id": 1, "book_isbn": "abc"},
                new={"id": 1, "book_isbn": "def" if move else "abc"},
            )
        ]
        with mock.patch.object(
            sync, "fetchall", wraps=sync.fetchall
        ) as fetchall:
            root_filters = self._root_filters(sync, payloads)

        assert fetchall.call_count == (2 if move else 1)
        assert sorted(where["id"] for where in root_filters) == (
            [1, 2] if move else [1]
        )

    def test_delete_through_row_resyncs_only_its_root(
        self,
        cleanup,
        continent_cls,
        country_cls,
        city_cls,
        author_cls,
        book_cls,
        book_author_cls,
    ):
        """A through table delete re-syncs only the root of the row.

        Two books have author 1. A delete of the abc link does not re-sync
        def.
        """
        session = cleanup
        self._author(session, continent_cls, country_cls, city_cls, author_cls)
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
            book_author_cls(id=1, book_isbn="abc", author_id=1),
            book_author_cls(id=2, book_isbn="def", author_id=1),
        )
        sync = self._sync(session, BOOK_AUTHOR_CITY)
        payloads = [
            Payload(
                tg_op="DELETE",
                table="book_author",
                schema="public",
                old={"id": 1, "book_isbn": "abc", "author_id": 1},
            )
        ]

        assert self._root_filters(sync, payloads) == [{"isbn": "abc"}]

    @pytest.mark.parametrize("resolver", ["payload", "search"])
    def test_delete_child_without_the_anchor_query(
        self, cleanup, book_cls, rating_cls, resolver
    ):
        """A child delete finds its root from the payload or from the
        search, also without the anchor query.

        The deleted rating 99 is not in the index, so the search of the
        rating primary key finds no root. The search of the foreign key
        finds it.
        """
        session = cleanup
        self._two_books_with_ratings(session, book_cls, rating_cls)
        sync = self._sync(session, BOOK_RATING, refresh=resolver == "search")
        payloads = [
            Payload(
                tg_op="DELETE",
                table="rating",
                schema="public",
                old={"id": 99, "book_isbn": "abc"},
            )
        ]

        def anchor(node, payloads, filters, moved=False):
            return filters

        def payload(node, payloads, foreign_keys, filters):
            return filters

        with mock.patch.object(
            sync, "_root_anchor_resolver", side_effect=anchor
        ):
            if resolver == "search":
                with mock.patch.object(
                    sync, "_root_payload_resolver", side_effect=payload
                ):
                    root_filters = self._root_filters(sync, payloads)
            else:
                root_filters = self._root_filters(sync, payloads)

        assert [str(where["isbn"]) for where in root_filters] == ["abc"]

    def test_update_through_child(
        self, cleanup, book_cls, group_cls, book_group_cls
    ):
        """pgsync syncs an update of a child below a through table.

        There is no direct foreign key between book and group, so the
        foreign keys hold the columns of both hops.
        """
        session = cleanup
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            book_cls(isbn="def", title="The Lion Club"),
            group_cls(id=1, group_name="Fiction"),
            book_group_cls(id=1, book_isbn="abc", group_id=1),
        )
        sync = self._sync(
            session,
            {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    object_child(
                        "group",
                        ["id", "group_name"],
                        "one_to_many",
                        through_tables=["book_group"],
                    )
                ],
            },
        )
        table = group_cls.__table__
        self._execute(
            session,
            table.update().where(table.c.id == 1).values(group_name="Myth"),
        )

        docs = self._docs(sync, "isbn")
        assert docs["abc"]["group"] == [{"id": 1, "group_name": "Myth"}]
        assert docs["def"]["group"] is None

    def test_unresolved_child_changes_are_counted(
        self, cleanup, book_cls, rating_cls
    ):
        """A child change with no root goes to the log and the counter."""
        session = cleanup
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, BOOK_RATING)

        def payloads(tg_op, row):
            return [
                Payload(
                    tg_op=tg_op,
                    table="rating",
                    schema="public",
                    old=row,
                    new=row,
                )
            ]

        with mock.patch(
            "pgsync.sync.monitor_view.unresolved_changes"
        ) as unresolved:
            # a known child row finds its root
            list(sync._payloads(payloads("UPDATE", {"id": 1})))
            unresolved.assert_not_called()
            # an unknown child row with no foreign key finds no root
            list(sync._payloads(payloads("UPDATE", {"id": 99})))
            list(sync._payloads(payloads("DELETE", {"id": 99})))

        self._close(sync)
        assert unresolved.call_args_list == [mock.call(1), mock.call(1)]

    # the root path query

    @pytest.mark.parametrize("foreign_key", [None, ["buyer_id"]])
    def test_root_path_query_needs_paired_foreign_keys(
        self, cleanup, foreign_key
    ):
        """The path query is None if a hop has ambiguous foreign keys.

        book has two foreign keys to user (buyer_id and seller_id).
        """
        relationship = {}
        if foreign_key:
            relationship["foreign_key"] = {
                "child": ["id"],
                "parent": foreign_key,
            }
            self._index(cleanup, "book_buyer_id_idx", "book", "buyer_id")
        statement = self._path_query(
            {
                "table": "book",
                "columns": ["isbn"],
                "children": [
                    object_child("user", ["id", "name"], **relationship)
                ],
            },
            "user",
        )
        if foreign_key:
            assert "buyer_id" in str(statement)
            assert "seller_id" not in str(statement)
        else:
            assert statement is None

    @pytest.mark.parametrize("index", [None, "full", "partial", "brin"])
    def test_root_path_query_needs_an_index(self, cleanup, index):
        """The path query is None if a joined column has no index.

        publisher is below book, and the foreign key is on the parent
        (book.publisher_id). Without an index, the join scans book. A
        partial index or a BRIN index does not count.
        """
        indexed = index == "full"
        if index:
            self._execute(
                cleanup,
                sa.text(
                    "CREATE INDEX book_publisher_id_idx ON book "
                    + {
                        "full": "(publisher_id)",
                        "partial": "(publisher_id) WHERE title IS NOT NULL",
                        "brin": "USING brin (publisher_id)",
                    }[index]
                ),
            )
        statement = self._path_query(
            {
                "table": "book",
                "columns": ["isbn"],
                "children": [object_child("publisher", ["id", "name"])],
            },
            "publisher",
        )
        if indexed:
            assert "publisher_id" in str(statement)
        else:
            assert statement is None

    @pytest.mark.parametrize("indexed", [False, True])
    def test_anchor_query_needs_an_index(
        self, cleanup, publisher_cls, book_cls, indexed
    ):
        """The anchor query is not used if the anchor column has no index.

        The anchor of publisher is book, and the anchor column is
        book.publisher_id.
        """
        session = cleanup
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
        )
        if indexed:
            self._index(
                session, "book_publisher_id_idx", "book", "publisher_id"
            )
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="publisher",
                schema="public",
                old={"id": 1},
                new={"id": 1},
            )
        ]
        with self._new_sync(
            {
                "table": "book",
                "columns": ["isbn"],
                "children": [object_child("publisher", ["id", "name"])],
            }
        ) as sync:
            node = sync.tree.get_node("publisher", "public")
            filters = sync._root_anchor_resolver(node, payloads, [])
        assert filters == ([{"isbn": "abc"}] if indexed else [])

    def test_root_path_query_needs_unique_tables(self, cleanup):
        """The path query is None if a table occurs two times in the path.

        Base.models keeps one alias for each table, so the join cannot
        tell the two book tables apart.
        """
        nodes = {
            "table": "book",
            "columns": ["isbn"],
            "children": [
                {
                    **object_child("publisher", ["id", "name"]),
                    "children": [
                        {
                            **object_child("book", ["isbn"], "one_to_many"),
                            "label": "books",
                        }
                    ],
                }
            ],
        }
        self._index(cleanup, "book_publisher_id_idx", "book", "publisher_id")
        with self._new_sync(nodes) as sync:
            books = [
                node
                for node in sync.tree.traverse_breadth_first()
                if node.label == "books"
            ]
            assert len(books) == 1
            assert sync._root_path_query(books[0]) is None

    def test_resolvers_skip_through_shapes(self, cleanup):
        """The database query skips a through node, and the anchor query
        skips a node with a through table.

        The foreign keys of the mock pair the nodes with the root, so only
        these checks stop the queries.
        """
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="author",
                schema="public",
                old={"id": 1},
                new={"id": 1},
            )
        ]
        with self._new_sync(BOOK_AUTHOR_CITY) as sync:
            through = sync.tree.get_node("book_author", "public")
            author = sync.tree.get_node("author", "public")
            with mock.patch.object(
                sync.query_builder,
                "get_foreign_keys",
                return_value={
                    "public.author": ["id"],
                    "public.book": ["isbn"],
                },
            ):
                with mock.patch.object(sync, "_root_path_query") as query:
                    assert (
                        sync._root_database_resolver(through, payloads, [])
                        is False
                    )
                    assert (
                        sync._root_anchor_resolver(author, payloads, []) == []
                    )
            query.assert_not_called()

    @pytest.mark.parametrize(
        "nodes", [BOOK_AUTHOR_CITY, PUBLISHER_BOOK_AUTHOR]
    )
    def test_payload_resolver_through_table(self, cleanup, nodes):
        """The payload of a through table holds the root key only if the
        child that it links is a child of the root."""
        payloads = [
            Payload(
                tg_op="INSERT",
                table="book_author",
                schema="public",
                new={"id": 1, "book_isbn": "abc", "author_id": 1},
            )
        ]
        with self._new_sync(nodes) as sync:
            node = sync.tree.get_node("book_author", "public")
            filters = sync._root_payload_resolver(
                node, payloads, sync._parent_foreign_keys(node), []
            )
        if nodes is BOOK_AUTHOR_CITY:
            assert filters == [{"isbn": "abc"}]
        else:
            assert filters == []

    def test_payload_resolver_through_table_below_the_root(self, cleanup):
        """A through table below the first level gives no root key, also
        if it has a foreign key to the root.

        The mock gives book_author a foreign key to the root publisher.
        The through table links a book, not the root.
        """
        payloads = [
            Payload(
                tg_op="INSERT",
                table="book_author",
                schema="public",
                new={"id": 1, "book_isbn": "abc", "author_id": 1},
            )
        ]
        with self._new_sync(PUBLISHER_BOOK_AUTHOR) as sync:
            node = sync.tree.get_node("book_author", "public")
            with mock.patch.object(
                sync.query_builder,
                "get_foreign_keys",
                return_value={
                    "public.book_author": ["author_id"],
                    "public.publisher": ["id"],
                },
            ):
                filters = sync._root_payload_resolver(node, payloads, {}, [])
        assert filters == []

    def test_foreign_key_to_a_different_schema(self, cleanup):
        """A foreign key to a table in a different schema does not stop
        the payload resolver."""
        session = cleanup
        statements = [
            "CREATE SCHEMA other",
            "CREATE TABLE public.reader (id INTEGER PRIMARY KEY)",
            "CREATE TABLE other.book ("
            "isbn VARCHAR(256) PRIMARY KEY, title VARCHAR(256))",
            "CREATE TABLE other.rating ("
            "id INTEGER PRIMARY KEY, "
            "book_isbn VARCHAR(256) REFERENCES other.book (isbn), "
            "reader_id INTEGER REFERENCES public.reader (id))",
        ]
        for statement in statements:
            self._execute(session, sa.text(statement))
        nodes = {
            "table": "book",
            "schema": "other",
            "columns": ["isbn", "title"],
            "children": [
                {
                    **object_child(
                        "rating",
                        ["id"],
                        foreign_key={
                            "child": ["book_isbn"],
                            "parent": ["isbn"],
                        },
                    ),
                    "schema": "other",
                }
            ],
        }
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="rating",
                schema="other",
                old={"id": 1},
                new={"id": 1, "book_isbn": "abc", "reader_id": 1},
            )
        ]
        with self._new_sync(nodes) as sync:
            node = sync.tree.get_node("rating", "other")
            filters = sync._root_payload_resolver(
                node, payloads, sync._parent_foreign_keys(node), []
            )
        assert filters == [{"isbn": "abc"}]

    def _stack(self, session: Session, link: str) -> None:
        """Make the stack table, with an enum primary key and a link."""
        statements = [
            "CREATE TYPE stack_code AS ENUM ('abc', 'def')",
            "CREATE TABLE stack ("
            "code stack_code PRIMARY KEY, "
            f"{link}, "
            "note VARCHAR(256))",
        ]
        for statement in statements:
            self._execute(session, sa.text(statement))

    def test_update_child_with_enum_key_before_root_is_searchable(
        self, cleanup, publisher_cls, book_cls
    ):
        """pgsync syncs an update of a child with an enum primary key before
        a search finds the root.

        A WAL value of an enum column keeps its quotes, e.g. "'abc'", so
        the database rejects the query of the changed rows. The anchor then
        finds the root from the foreign key.
        """
        session = cleanup
        self._stack(session, "book_isbn VARCHAR(256) REFERENCES book (isbn)")
        self._add(
            session,
            publisher_cls(id=1, name="Tiger Books"),
            book_cls(isbn="abc", title="The Tiger Club", publisher_id=1),
        )
        self._execute(
            session,
            sa.text("INSERT INTO stack VALUES ('abc', 'abc', 'old')"),
        )
        sync = self._sync(
            session,
            {
                "table": "publisher",
                "columns": ["id", "name"],
                "children": [
                    {
                        **object_child(
                            "book", ["isbn", "title"], "one_to_many"
                        ),
                        "children": [
                            object_child("stack", ["note"], "one_to_many")
                        ],
                    }
                ],
            },
            refresh=False,
        )
        self._execute(
            session,
            sa.text("UPDATE stack SET note = 'new' WHERE code = 'abc'"),
        )

        with mock.patch("pgsync.sync.logger.warning") as warning:
            docs = self._docs(sync, "id")
        assert docs[1]["book"][0]["stack"] == [{"note": "new"}]
        # the log has the table and the error type, but no row values
        assert warning.call_count == 1
        message = warning.call_args[0][0]
        assert "public.stack" in message
        assert "InvalidTextRepresentation" in message
        assert "abc" not in message

    def test_failed_root_query_warns_one_time(self, cleanup):
        """A root query that the database rejects gives one warning for each
        table, and does not stop the sync."""
        session = cleanup
        self._stack(session, "publisher_id INTEGER REFERENCES publisher (id)")
        self._execute(
            session,
            sa.text(
                "CREATE TABLE stack_label ("
                "id INTEGER PRIMARY KEY, "
                "stack_code stack_code REFERENCES stack (code))"
            ),
        )
        nodes = {
            "table": "publisher",
            "columns": ["id", "name"],
            "children": [
                {
                    **object_child("stack", ["code"], "one_to_many"),
                    "children": [object_child("stack_label", ["id"])],
                }
            ],
        }
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="stack_label",
                schema="public",
                old={"id": 1, "stack_code": "'abc'"},
                new={"id": 1, "stack_code": "'def'"},
            )
        ]
        with self._new_sync(nodes) as sync:
            node = sync.tree.get_node("stack_label", "public")
            with mock.patch("pgsync.sync.logger.warning") as warning:
                for _ in range(2):
                    filters = sync._root_anchor_resolver(node, payloads, [])
                    assert filters == []
        assert warning.call_count == 1

    def test_failed_chunk_does_not_skip_later_chunks(self, cleanup):
        """A chunk that the database rejects does not skip the next chunk."""
        with self._new_sync(BOOK_RATING) as sync:
            node = sync.tree.get_node("rating", "public")
            error = sa.exc.DataError("SELECT", {}, Exception("value"))
            filters = []
            with mock.patch("pgsync.sync.settings.FILTER_CHUNK_SIZE", 1):
                with mock.patch.object(
                    sync, "fetchall", side_effect=[error, [("abc",)]]
                ):
                    succeeded = sync._query_root_filters(
                        node,
                        sync._root_path_query(node),
                        [node.model.c.id],
                        [(1,), (2,)],
                        filters,
                    )
        assert succeeded is False
        assert filters == [{"isbn": "abc"}]

    def test_root_query_connection_error_propagates(
        self, cleanup, book_cls, rating_cls
    ):
        """A database error that is not about a value stops the sync, so
        the change is not lost, e.g. a timeout or a lost connection."""
        session = cleanup
        self._add(
            session,
            book_cls(isbn="abc", title="The Tiger Club"),
            rating_cls(id=1, book_isbn="abc", value=1.1),
        )
        sync = self._sync(session, BOOK_RATING)
        payloads = [
            Payload(
                tg_op="UPDATE",
                table="rating",
                schema="public",
                old={"id": 1, "book_isbn": "abc"},
                new={"id": 1, "book_isbn": "abc"},
            )
        ]
        error = sa.exc.OperationalError("SELECT", {}, Exception("timeout"))
        try:
            with mock.patch.object(sync, "fetchall", side_effect=error):
                with pytest.raises(sa.exc.OperationalError):
                    list(sync._payloads(payloads))
        finally:
            self._close(sync)

    @pytest.mark.parametrize(
        "kwargs, indexed",
        [
            ({}, True),
            ({"postgresql_using": "hash"}, True),
            ({"postgresql_using": "gin"}, False),
            ({"mysql_prefix": "FULLTEXT"}, False),
            ({"mariadb_prefix": "SPATIAL"}, False),
        ],
    )
    def test_is_indexed_access_methods(self, kwargs, indexed):
        """Only a btree or hash index counts."""
        table = sa.Table(
            "t",
            sa.MetaData(),
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("a", sa.Integer),
        )
        sa.Index("t_a_idx", table.c.a, **kwargs)
        assert Sync._is_indexed(table, ["a"]) is indexed

    @pytest.mark.parametrize(
        "columns, indexed",
        [
            (["a"], True),
            (["b", "a"], True),
            # not the first column of the index
            (["b"], False),
        ],
    )
    def test_is_indexed_first_columns(self, columns, indexed):
        """The columns must be the first columns of the index."""
        table = sa.Table(
            "t",
            sa.MetaData(),
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("a", sa.Integer),
            sa.Column("b", sa.Integer),
        )
        sa.Index("t_a_b_idx", table.c.a, table.c.b)
        assert Sync._is_indexed(table, columns) is indexed

    def test_is_indexed_expression(self, cleanup):
        """An index with an expression does not count.

        The reflected index on (lower(body), book_ref) has only book_ref in
        its columns, so book_ref looks like its first column.
        """
        session = cleanup
        statements = [
            "CREATE TABLE review ("
            "id INTEGER PRIMARY KEY, "
            "book_ref VARCHAR(256), "
            "body VARCHAR(256))",
            "CREATE INDEX review_body_idx ON review (LOWER(body), book_ref)",
        ]
        for statement in statements:
            self._execute(session, sa.text(statement))
        table = sa.Table(
            "review", sa.MetaData(), autoload_with=session.get_bind()
        )
        assert [
            column.name for column in next(iter(table.indexes)).columns
        ] == ["book_ref"]
        assert Sync._is_indexed(table, ["book_ref"]) is False

    # trigger mode

    def test_trigger_delete_sends_foreign_keys(
        self, cleanup, book_cls, rating_cls
    ):
        """A trigger child delete finds the root without a search."""
        session = cleanup
        self._two_books_with_ratings(session, book_cls, rating_cls)
        sync = self._sync(session, BOOK_RATING, refresh=False)
        table = rating_cls.__table__
        with self._triggers(sync) as listener:
            self._execute(session, table.delete().where(table.c.id == 1))
            notifications = self._notifications(listener)

        assert len(notifications) == 1
        assert notifications[0]["tg_op"] == "DELETE"
        assert notifications[0]["old"] == {"id": 1, "book_isbn": "abc"}

        sync.search_client.bulk(
            sync.index, sync._payloads([Payload(**notifications[0])])
        )
        sync.search_client.refresh("testdb")
        docs = {
            doc["isbn"]: doc for doc in search(sync.search_client, "testdb")
        }
        self._close(sync)
        assert docs["abc"]["rating"] is None
        assert docs["def"]["rating"] == {"id": 2, "value": 2.2}

    def test_trigger_delete_without_primary_key(self, cleanup, book_cls):
        """A trigger delete of a table with a primary key only in the schema
        sends no keys, and does not stop the sync.

        The view has no primary keys for the table. If the trigger sends
        only the foreign keys, the primary key check of the payload fails.
        """
        session = cleanup
        self._execute(
            session,
            sa.text(
                "CREATE TABLE review ("
                "id INTEGER, "
                "book_isbn VARCHAR(256) REFERENCES book (isbn), "
                "body VARCHAR(256))"
            ),
        )
        self._add(session, book_cls(isbn="abc", title="The Tiger Club"))
        self._execute(
            session,
            sa.text("INSERT INTO review VALUES (1, 'abc', 'Great')"),
        )
        sync = self._sync(
            session,
            {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        **object_child(
                            "review", ["id", "body"], "one_to_many"
                        ),
                        "primary_key": ["id"],
                    }
                ],
            },
        )
        with self._triggers(sync) as listener:
            self._execute(session, sa.text("DELETE FROM review WHERE id = 1"))
            notifications = self._notifications(listener)

        assert len(notifications) == 1
        assert notifications[0]["tg_op"] == "DELETE"
        assert notifications[0]["old"] is None
        try:
            list(sync._payloads([Payload(**notifications[0])]))
        finally:
            self._close(sync)
