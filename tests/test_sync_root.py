"""Tests for `pgsync` package."""

import mock
import pytest

from pgsync.base import subtransactions
from pgsync.exc import (
    ColumnNotFoundError,
    NodeAttributeError,
    TableNotInNodeError,
)
from pgsync.node import Tree
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.singleton import Singleton
from pgsync.sync import Sync

from .testing_utils import assert_resync_empty, noop, search, sort_list


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestRoot(object):
    """Root only node tests."""

    @pytest.fixture(scope="function")
    def data(self, sync, book_cls, publisher_cls):
        session = sync.session

        books = [
            book_cls(
                isbn="abc",
                title="The Tiger Club",
                description="Tigers are fierce creatures",
                publisher=publisher_cls(name="Tiger publishing"),
            ),
            book_cls(
                isbn="def",
                title="The Lion Club",
                description="Lion and the mouse",
                publisher=publisher_cls(name="Lion publishing"),
            ),
            book_cls(
                isbn="ghi",
                title="The Rabbit Club",
                description="Rabbits on the run",
                publisher=publisher_cls(name="Hop Bunny publishing"),
            ),
        ]

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            cursor = conn.cursor()
            channel = sync.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            session.add_all(books)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )
        Singleton._instances = {}

        yield books

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            cursor = conn.cursor()
            channel = session.connection().engine.url.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            sync.truncate_tables(
                [book_cls.__table__.name, publisher_cls.__table__.name]
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        try:
            sync.search_client.teardown(index="testdb")
        except Exception:
            raise

        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.search_client.close()

    def test_sync2(self, sync, data):
        """Test the sync with a root only node."""
        nodes = {"table": "book", "columns": ["isbn", "title", "description"]}
        txmin = sync.checkpoint
        txmax = sync.txid_current
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [doc for doc in sync.sync(txmin=txmin, txmax=txmax)]
        assert docs == [
            {
                "_id": "abc",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "book": {"isbn": ["abc"]},
                    },
                    "description": "Tigers are fierce creatures",
                    "isbn": "abc",
                    "title": "The Tiger Club",
                },
            },
            {
                "_id": "def",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "book": {"isbn": ["def"]},
                    },
                    "description": "Lion and the mouse",
                    "isbn": "def",
                    "title": "The Lion Club",
                },
            },
            {
                "_id": "ghi",
                "_index": "testdb",
                "_source": {
                    "_meta": {"book": {"isbn": ["ghi"]}},
                    "description": "Rabbits on the run",
                    "isbn": "ghi",
                    "title": "The Rabbit Club",
                },
            },
        ]
        assert_resync_empty(sync, nodes, txmin=txmax)

    def test_label(self, sync, data):
        """There is no possible test for label at root level."""
        nodes = {"table": "book", "label": "some_label", "columns": ["isbn"]}
        nodes

    def test_transform(self, sync, data):
        """Test transform for node attributes."""
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "transform": {
                "rename": {"isbn": "book_isbn", "title": "book_title"}
            },
        }
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [doc for doc in sync.sync()]
        assert docs == [
            {
                "_id": "abc",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "book": {"isbn": ["abc"]},
                    },
                    "description": "Tigers are fierce creatures",
                    "book_isbn": "abc",
                    "book_title": "The Tiger Club",
                },
            },
            {
                "_id": "def",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "book": {"isbn": ["def"]},
                    },
                    "description": "Lion and the mouse",
                    "book_isbn": "def",
                    "book_title": "The Lion Club",
                },
            },
            {
                "_id": "ghi",
                "_index": "testdb",
                "_source": {
                    "_meta": {"book": {"isbn": ["ghi"]}},
                    "description": "Rabbits on the run",
                    "book_isbn": "ghi",
                    "book_title": "The Rabbit Club",
                },
            },
        ]
        assert_resync_empty(sync, nodes)

    def test_doc_includes_all_columns(self, sync, data):
        """Test the doc includes all selected columns."""
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description", "xmin"],
        }
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert sorted(docs[0]["_source"].keys()) == sorted(
            ["isbn", "title", "description", "xmin", "_meta"]
        )
        assert_resync_empty(sync, nodes)

    def test_select_xmin_column(self, sync, data):
        """Test the doc includes xmin column."""
        nodes = {"table": "book", "columns": ["isbn", "xmin"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert "xmin" in docs[0]["_source"]
        assert_resync_empty(sync, nodes)

    def test_no_column_specified(self, sync, data):
        """Test we include all columns when no columns are specified."""
        nodes = {"table": "book", "columns": []}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert sorted(docs[0]["_source"].keys()) == sorted(
            [
                "_meta",
                "copyright",
                "description",
                "isbn",
                "publisher_id",
                "title",
                "buyer_id",
                "seller_id",
                "tags",
            ]
        )

        nodes = {"table": "book"}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert sorted(docs[0]["_source"].keys()) == sorted(
            [
                "_meta",
                "copyright",
                "description",
                "isbn",
                "publisher_id",
                "title",
                "buyer_id",
                "seller_id",
                "tags",
            ]
        )
        assert_resync_empty(sync, nodes)

    def test_invalid_column(self, sync, data):
        """Test an invalid column raises ColumnNotFoundError."""
        nodes = {"table": "book", "columns": ["foo"]}
        sync.nodes = nodes
        sync.tree.__post_init__()

        with pytest.raises(ColumnNotFoundError) as excinfo:
            sync.tree.build(nodes)
        assert 'Column "foo" not present on table "book"' in str(excinfo.value)

    def test_primary_key_is_doc_id(self, sync, data):
        """Test the db primary key is used as the doc_id."""
        # TODO also repeat this test for composite primary key
        nodes = {"table": "book", "columns": ["title"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert "abc" == docs[0]["_id"]
        assert "def" == docs[1]["_id"]
        assert "ghi" == docs[2]["_id"]
        assert_resync_empty(sync, nodes)

    def test_meta_in_docs(self, sync, data):
        """Test the private key is contained in the doc."""
        nodes = {"table": "book", "columns": ["isbn"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        assert "_meta" in docs[0]["_source"]
        assert_resync_empty(sync, nodes)

    def test_doc_only_includes_selected_columns(self, sync, data):
        """Ensure the doc only selected columns and builtins."""
        nodes = {"table": "book", "columns": ["isbn", "xmin"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}

        assert sorted(sources["abc"].keys()) == sorted(
            ["isbn", "xmin", "_meta"]
        )
        nodes = {
            "table": "book",
            "columns": [
                "isbn",
                "xmin",
                "description",
                "copyright",
                "publisher_id",
            ],
        }
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [sort_list(doc) for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}

        assert sorted(sources["abc"].keys()) == sorted(
            [
                "isbn",
                "xmin",
                "_meta",
                "description",
                "copyright",
                "publisher_id",
            ]
        )
        nodes = {"table": "book", "columns": ["copyright", "publisher_id"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)

        docs = [sort_list(doc) for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}

        assert sorted(sources["abc"].keys()) == sorted(
            [
                "_meta",
                "copyright",
                "publisher_id",
            ]
        )
        assert_resync_empty(sync, nodes)

    def test_doc_includes_nulls(self, sync, data):
        """Elasticsearch doc should include nulls from db."""
        nodes = {
            "table": "book",
            "columns": ["isbn", "description", "copyright"],
        }
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [doc for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}

        assert sources == {
            "abc": {
                "_meta": {"book": {"isbn": ["abc"]}},
                "copyright": None,
                "description": "Tigers are fierce creatures",
                "isbn": "abc",
            },
            "def": {
                "_meta": {"book": {"isbn": ["def"]}},
                "copyright": None,
                "description": "Lion and the mouse",
                "isbn": "def",
            },
            "ghi": {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "copyright": None,
                "description": "Rabbits on the run",
                "isbn": "ghi",
            },
        }
        assert_resync_empty(sync, nodes)

    def test_meta_keys(self, sync, data):
        """Private keys should be included even if null."""
        nodes = {"table": "book", "columns": ["description"]}
        sync.nodes = nodes
        sync.tree = Tree(sync.models, nodes, database=sync.database)
        docs = [doc for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}

        assert sources["abc"]["_meta"] == {"book": {"isbn": ["abc"]}}
        assert sources["def"]["_meta"] == {"book": {"isbn": ["def"]}}
        assert sources["ghi"]["_meta"] == {"book": {"isbn": ["ghi"]}}
        assert_resync_empty(sync, nodes)

    def test_node_include_table(self, sync, data):
        """All node must include the table name."""
        nodes = {"no_table_specified": "book", "columns": ["description"]}
        sync.nodes = nodes
        sync.tree.__post_init__()
        with pytest.raises(TableNotInNodeError):
            sync.tree.build(nodes)

    def test_node_valid_attributes(self, sync, data):
        """All node must have valid attributes."""
        nodes = {"table": "book", "unknown": "xyz", "columns": ["description"]}
        sync.nodes = nodes
        sync.tree.__post_init__()
        with pytest.raises(NodeAttributeError):
            sync.tree.build(nodes)

    def test_update_primary_key_non_concurrent(self, data, book_cls):
        """
        Test sync updates primary_key and then sync in non-concurrent mode.
        TODO: Note this test highlights a potential undesired bahaviour
        i.e we have a duplicate doc at a point in time.
        Note to self. I think this has been fixed. i.e we delete and then
        query ibsert
        """
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]

        session = sync.session
        with subtransactions(session):
            session.execute(
                book_cls.__table__.update()
                .where(book_cls.__table__.c.isbn == "abc")
                .values(isbn="cba")
            )

        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["cba"]}},
                "isbn": "cba",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    # TODO: Add another test like this and change
    # both primary key and non primary key column
    def test_update_primary_key_concurrent(self, data, book_cls):
        """Test sync updates primary_key and then sync in concurrent mode."""
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]

        session = sync.session

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.execute(
                    book_cls.__table__.update()
                    .where(book_cls.__table__.c.isbn == "abc")
                    .values(isbn="cba")
                )
                session.commit()

        with mock.patch("pgsync.sync.Sync.poll_redis", side_effect=poll_redis):
            with mock.patch("pgsync.sync.Sync.poll_db", side_effect=poll_db):
                with mock.patch("pgsync.sync.Sync.pull", side_effect=pull):
                    with mock.patch(
                        "pgsync.sync.Sync.truncate_slots",
                        side_effect=noop,
                    ):
                        with mock.patch(
                            "pgsync.sync.Sync.status",
                            side_effect=noop,
                        ):
                            sync.receive()
                            sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert len(docs) == 3
        assert "cba" in [doc["isbn"] for doc in docs]
        assert "abc" not in [doc["isbn"] for doc in docs]

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["cba"]}},
                "isbn": "cba",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    def test_insert_non_concurrent(self, data, book_cls):
        """Test sync insert and then sync in non-concurrent mode."""
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        session = sync.session

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        with subtransactions(session):
            session.execute(
                book_cls.__table__.insert().values(
                    isbn="xyz", title="Encyclopedia"
                )
            )

        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
            {
                "_meta": {"book": {"isbn": ["xyz"]}},
                "isbn": "xyz",
                "title": "Encyclopedia",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    def test_update_non_concurrent(self, data, book_cls):
        """Test sync update and then sync in non-concurrent mode."""
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        session = sync.session

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]

        with subtransactions(session):
            session.execute(
                book_cls.__table__.update()
                .where(book_cls.__table__.c.isbn == "abc")
                .values(title="Tiger Club")
            )

        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    def test_update_concurrent(self, data, book_cls):
        """Test sync update and then sync in concurrent mode."""
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")

        session = sync.session

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.execute(
                    book_cls.__table__.update()
                    .where(book_cls.__table__.c.isbn == "abc")
                    .values(title="Tiger Club")
                )
                session.commit()

        with mock.patch("pgsync.sync.Sync.poll_redis", side_effect=poll_redis):
            with mock.patch("pgsync.sync.Sync.poll_db", side_effect=poll_db):
                with mock.patch("pgsync.sync.Sync.pull", side_effect=pull):
                    with mock.patch(
                        "pgsync.sync.Sync.truncate_slots",
                        side_effect=noop,
                    ):
                        with mock.patch(
                            "pgsync.sync.Sync.status",
                            side_effect=noop,
                        ):
                            sync.receive()
                            sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    def test_delete_concurrent(self, data, book_cls):
        """Test sync delete and then sync in concurrent mode."""
        doc = {
            "index": "testdb",
            "database": "testdb",
            "nodes": {"table": "book", "columns": ["isbn", "title"]},
        }
        sync = Sync(doc)
        sync.search_client.bulk(sync.index, sync.sync())
        sync.search_client.refresh("testdb")
        session = sync.session
        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["abc"]}},
                "isbn": "abc",
                "title": "The Tiger Club",
            },
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.execute(
                    book_cls.__table__.delete().where(
                        book_cls.__table__.c.isbn == "abc"
                    )
                )
                session.commit()

        with mock.patch("pgsync.sync.Sync.poll_redis", side_effect=poll_redis):
            with mock.patch("pgsync.sync.Sync.poll_db", side_effect=poll_db):
                with mock.patch("pgsync.sync.Sync.pull", side_effect=pull):
                    with mock.patch(
                        "pgsync.sync.Sync.truncate_slots",
                        side_effect=noop,
                    ):
                        with mock.patch(
                            "pgsync.sync.Sync.status",
                            side_effect=noop,
                        ):
                            sync.receive()
                            sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")

        assert docs == [
            {
                "_meta": {"book": {"isbn": ["def"]}},
                "isbn": "def",
                "title": "The Lion Club",
            },
            {
                "_meta": {"book": {"isbn": ["ghi"]}},
                "isbn": "ghi",
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, doc.get("node", {}))
        sync.search_client.close()

    def test_truncate(self, data, book_cls):
        """Test truncate."""
        # TODO: implement when truncate is supported
        pass
