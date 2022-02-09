"""Tests for `pgsync` package."""

import mock
import psycopg2
import pytest

from pgsync.base import subtransactions
from pgsync.exc import (
    ForeignKeyError,
    RelationshipAttributeError,
    RelationshipError,
    RelationshipTypeError,
    RelationshipVariantError,
)
from pgsync.node import Tree
from pgsync.sync import Sync

from .helpers.utils import assert_resync_empty, noop, search


@pytest.mark.usefixtures("table_creator")
class TestParentSingleChildFkOnChild(object):
    """Root and single child node tests."""

    @pytest.fixture(scope="function")
    def data(self, sync, book_cls, rating_cls):

        session = sync.session

        books = [
            book_cls(
                isbn="abc",
                title="The Tiger Club",
                description="Tigers are fierce creatures",
            ),
            book_cls(
                isbn="def",
                title="The Lion Club",
                description="Lion and the mouse",
            ),
            book_cls(
                isbn="ghi",
                title="The Rabbit Club",
                description="Rabbits on the run",
            ),
        ]

        with subtransactions(session):
            session.add_all(books)

        ratings = [
            rating_cls(id=1, book_isbn="abc", value=1.1),
            rating_cls(id=2, book_isbn="def", value=2.2),
            rating_cls(id=3, book_isbn="ghi", value=3.3),
        ]

        with subtransactions(session):
            session.add_all(ratings)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        yield (
            books,
            ratings,
        )

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cursor = conn.cursor()
            channel = session.connection().engine.url.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            sync.truncate_tables(
                [book_cls.__table__.name, rating_cls.__table__.name]
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        try:
            sync.es.teardown(index="testdb")
        except Exception:
            raise

        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.es.close()

    def test_relationship_object_one_to_one(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "columns": ["id", "value"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating": {"id": 1, "value": 1.1},
            "title": "The Tiger Club",
        }

        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "description": "Lion and the mouse",
            "isbn": "def",
            "rating": {"id": 2, "value": 2.2},
            "title": "The Lion Club",
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "rating": {"id": 3, "value": 3.3},
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_relationship_object_one_to_many(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "columns": ["id", "value"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        docs = sorted(docs, key=lambda k: k["_id"])

        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating": [{"id": 1, "value": 1.1}],
            "title": "The Tiger Club",
        }

        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "description": "Lion and the mouse",
            "isbn": "def",
            "rating": [{"id": 2, "value": 2.2}],
            "title": "The Lion Club",
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "rating": [{"id": 3, "value": 3.3}],
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_relationship_scalar_one_to_one(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "columns": ["value"],
                    "relationship": {
                        "variant": "scalar",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating": 1.1,
            "title": "The Tiger Club",
        }

        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "description": "Lion and the mouse",
            "isbn": "def",
            "rating": 2.2,
            "title": "The Lion Club",
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "rating": 3.3,
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_relationship_scalar_one_to_many(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "columns": ["value"],
                    "relationship": {
                        "variant": "scalar",
                        "type": "one_to_many",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        docs = sorted(docs, key=lambda k: k["_id"])
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating": [1.1],
            "title": "The Tiger Club",
        }

        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "description": "Lion and the mouse",
            "isbn": "def",
            "rating": [2.2],
            "title": "The Lion Club",
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "rating": [3.3],
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_label(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "label": "rating_x",
                    "columns": ["value"],
                    "relationship": {
                        "variant": "scalar",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating_x": 1.1,
            "title": "The Tiger Club",
        }

        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "description": "Lion and the mouse",
            "isbn": "def",
            "rating_x": 2.2,
            "title": "The Lion Club",
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "rating_x": 3.3,
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_null_label(self, sync, data):
        """null label should revert back to the table name"""
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "label": None,
                    "columns": ["value"],
                    "relationship": {
                        "variant": "scalar",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "description": "Tigers are fierce creatures",
            "isbn": "abc",
            "rating": 1.1,
            "title": "The Tiger Club",
        }
        assert_resync_empty(sync, nodes)

    def test_transform(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "transform": {
                "rename": {"isbn": "book_isbn", "title": "book_title"}
            },
            "children": [
                {
                    "table": "rating",
                    "columns": ["id", "value"],
                    "transform": {
                        "rename": {"id": "rating_id", "value": "rating_value"}
                    },
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[0]["_id"] == "abc"

        assert docs[0]["_source"] == {
            "_meta": {"rating": {"id": [1]}},
            "book_isbn": "abc",
            "book_title": "The Tiger Club",
            "description": "Tigers are fierce creatures",
            "rating": {"rating_id": 1, "rating_value": 1.1},
        }
        assert docs[1]["_id"] == "def"
        assert docs[1]["_source"] == {
            "_meta": {"rating": {"id": [2]}},
            "book_isbn": "def",
            "book_title": "The Lion Club",
            "description": "Lion and the mouse",
            "rating": {"rating_id": 2, "rating_value": 2.2},
        }

        assert docs[2]["_id"] == "ghi"
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "book_isbn": "ghi",
            "book_title": "The Rabbit Club",
            "description": "Rabbits on the run",
            "rating": {"rating_id": 3, "rating_value": 3.3},
        }
        assert_resync_empty(sync, nodes)

    def test_schema(self, sync, data):
        nodes = {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "rating",
                    "columns": ["id", "value"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]

        fields = ["_meta", "description", "isbn", "rating", "title"]
        assert sorted(docs[0]["_source"].keys()) == sorted(fields)
        assert sorted(docs[1]["_source"].keys()) == sorted(fields)
        assert sorted(docs[0]["_source"]["rating"].keys()) == sorted(
            ["id", "value"]
        )
        assert sorted(docs[1]["_source"]["rating"].keys()) == sorted(
            ["id", "value"]
        )
        assert_resync_empty(sync, nodes)

    def test_schema_with_no_column_specified(self, sync, data):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "rating",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert docs[2]["_source"] == {
            "_meta": {"rating": {"id": [3]}},
            "copyright": None,
            "description": "Rabbits on the run",
            "isbn": "ghi",
            "publisher_id": None,
            "rating": {"book_isbn": "ghi", "id": 3, "value": 3.3},
            "title": "The Rabbit Club",
        }
        assert_resync_empty(sync, nodes)

    def test_invalid_relationship_type(self, sync):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "rating",
                    "relationship": {"variant": "object", "type": "qwerty"},
                }
            ],
        }
        sync.es.close()
        with pytest.raises(RelationshipTypeError) as excinfo:
            Tree(sync).build(nodes)
        assert 'Relationship type "qwerty" is invalid' in str(excinfo.value)

    def test_invalid_relationship_variant(self, sync):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "rating",
                    "relationship": {
                        "variant": "abcdefg",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.es.close()
        with pytest.raises(RelationshipVariantError) as excinfo:
            Tree(sync).build(nodes)
        assert 'Relationship variant "abcdefg" is invalid' in str(
            excinfo.value
        )

    def test_invalid_relationship_attribute(self, sync):
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "rating",
                    "relationship": {"foo": "object", "type": "one_to_one"},
                }
            ],
        }
        sync.es.close()
        with pytest.raises(RelationshipAttributeError) as excinfo:
            Tree(sync).build(nodes)
        assert f"Relationship attribute {set(['foo'])} is invalid" in str(
            excinfo.value
        )

    def test_meta_keys(self, sync, data):
        """Private keys should be correct"""
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "rating",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        sources = {doc["_id"]: doc["_source"] for doc in docs}
        assert sources["abc"]["_meta"] == {"rating": {"id": [1]}}
        assert sources["def"]["_meta"] == {"rating": {"id": [2]}}
        assert sources["ghi"]["_meta"] == {"rating": {"id": [3]}}
        assert_resync_empty(sync, nodes)

    def test_missing_foreign_keys(self, sync, data):
        """Foreign keys must be present between parent and child"""
        nodes = {
            "table": "book",
            "children": [
                {
                    "table": "city",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                }
            ],
        }
        sync.nodes = nodes
        with pytest.raises(ForeignKeyError) as excinfo:
            [doc for doc in sync.sync()]
        msg = (
            "No foreign key relationship between "
            '"public.book" and "public.city"'
        )
        assert msg in str(excinfo.value)

    def test_missing_relationships(self, sync, data):
        """Relationships must be present between parent and child"""
        nodes = {"table": "book", "children": [{"table": "rating"}]}
        sync.nodes = nodes
        with pytest.raises(RelationshipError) as excinfo:
            [doc for doc in sync.sync()]
        assert 'Relationship not present on "public.rating"' in str(
            excinfo.value
        )

    def test_update_primary_key_non_concurrent(
        self, data, book_cls, rating_cls, engine
    ):
        """
        Test sync updates primary_key and then sync in non-concurrent mode.
        """
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
                "title": "The Rabbit Club",
            },
        ]
        session = sync.session

        try:
            session.execute(
                book_cls.__table__.insert().values(
                    isbn="xyz", title="Milli and the Ants"
                )
            )
            session.execute(
                rating_cls.__table__.update()
                .where(rating_cls.__table__.c.book_isbn == "ghi")
                .values(book_isbn="xyz")
            )
            session.commit()
        except Exception:
            session.rollback()
            raise

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")
        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {},
                "isbn": "ghi",
                "rating": None,
                "title": "The Rabbit Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "xyz",
                "rating": {"id": 3, "value": 3.3},
                "title": "Milli and the Ants",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()

    # TODO: Add another test like this and change
    # both primary key and non pkey column
    def test_update_primary_key_concurrent(self, data, book_cls, rating_cls):
        """Test sync updates primary_key and then sync in concurrent mode."""
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
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
            try:
                session.execute(
                    book_cls.__table__.insert().values(
                        isbn="xyz",
                        title="Milli and the Ants",
                    )
                )
                session.execute(
                    rating_cls.__table__.update()
                    .where(rating_cls.__table__.c.book_isbn == "ghi")
                    .values(book_isbn="xyz")
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

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
                            sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")
        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {},
                "isbn": "ghi",
                "rating": None,
                "title": "The Rabbit Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "xyz",
                "rating": {"id": 3, "value": 3.3},
                "title": "Milli and the Ants",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()

    def test_insert_non_concurrent(self, data, book_cls, rating_cls):
        """Test sync insert and then sync in non-concurrent mode."""
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        session = sync.session

        docs = search(sync.es, "testdb")
        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
                "title": "The Rabbit Club",
            },
        ]

        try:
            session.execute(
                book_cls.__table__.insert().values(
                    isbn="xyz",
                    title="Encyclopedia",
                )
            )
            session.execute(
                rating_cls.__table__.insert().values(
                    id=99,
                    book_isbn="xyz",
                    value=4.4,
                )
            )
            session.commit()
        except Exception:
            session.rollback()
            raise

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
                "title": "The Rabbit Club",
            },
            {
                "_meta": {"rating": {"id": [99]}},
                "isbn": "xyz",
                "rating": {"id": 99, "value": 4.4},
                "title": "Encyclopedia",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()

    def test_update_non_primary_key_non_concurrent(
        self, data, book_cls, rating_cls
    ):
        """Test sync update and then sync in non-concurrent mode."""
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
                "title": "The Rabbit Club",
            },
        ]

        session = sync.session

        try:
            session.execute(
                rating_cls.__table__.update()
                .where(rating_cls.__table__.c.id == 3)
                .values(value=4.4)
            )
            session.commit()
        except Exception:
            session.rollback()
            raise

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 4.4},
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()

    def test_update_non_primary_key_concurrent(
        self, data, book_cls, rating_cls
    ):
        """Test sync update and then sync in concurrent mode."""
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
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
                    rating_cls.__table__.update()
                    .where(rating_cls.__table__.c.id == 3)
                    .values(value=4.4)
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
                            sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 4.4},
                "title": "The Rabbit Club",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()

    def test_delete_concurrent(self, data, book_cls, rating_cls):
        """Test sync delete and then sync in concurrent mode."""
        document = {
            "index": "testdb",
            "nodes": {
                "table": "book",
                "columns": ["isbn", "title"],
                "children": [
                    {
                        "table": "rating",
                        "columns": ["id", "value"],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                    }
                ],
            },
        }

        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "ghi",
                "rating": {"id": 3, "value": 3.3},
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
            try:
                session.execute(
                    book_cls.__table__.insert().values(
                        isbn="xyz",
                        title="The End of time",
                    )
                )
                session.execute(
                    rating_cls.__table__.update()
                    .where(rating_cls.__table__.c.id == 3)
                    .values(book_isbn="xyz")
                )
                session.execute(
                    book_cls.__table__.delete().where(
                        book_cls.__table__.c.isbn == "ghi"
                    )
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

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
                            sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")
        assert docs == [
            {
                "_meta": {"rating": {"id": [1]}},
                "isbn": "abc",
                "rating": {"id": 1, "value": 1.1},
                "title": "The Tiger Club",
            },
            {
                "_meta": {"rating": {"id": [2]}},
                "isbn": "def",
                "rating": {"id": 2, "value": 2.2},
                "title": "The Lion Club",
            },
            {
                "_meta": {"rating": {"id": [3]}},
                "isbn": "xyz",
                "rating": {"id": 3, "value": 3.3},
                "title": "The End of time",
            },
        ]
        assert_resync_empty(sync, document.get("node", {}))
        sync.es.close()
