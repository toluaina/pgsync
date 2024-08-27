"""Tests for `pgsync` package."""

import pytest

from pgsync.base import subtransactions
from pgsync.node import Tree

from .testing_utils import assert_resync_empty, sort_list


@pytest.mark.usefixtures("table_creator")
class TestUniqueBehaviour(object):
    """Unique behaviour tests."""

    @pytest.fixture(scope="function")
    def data(
        self,
        sync,
        book_cls,
        user_cls,
        contact_cls,
        contact_item_cls,
    ):
        session = sync.session
        contacts = [
            contact_cls(name="Contact 1"),
            contact_cls(name="Contact 2"),
        ]
        contact_items = [
            # contact_item_cls(name="Contact Item 1", contact=contacts[0]),
            # contact_item_cls(name="Contact Item 2", contact=contacts[1]),
        ]
        users = [
            user_cls(name="Fonzy Bear", contact=contacts[0]),
            user_cls(name="Jack Jones", contact=contacts[1]),
        ]
        books = [
            book_cls(
                isbn="abc",
                title="The Tiger Club",
                description="Tigers are fierce creatures",
                buyer=users[0],
                seller=users[1],
            ),
        ]
        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            cursor = conn.cursor()
            channel = sync.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            session.add_all(contacts)
            session.add_all(contact_items)
            session.add_all(users)
            session.add_all(books)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        yield (
            books,
            contacts,
            contact_items,
            users,
        )

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            cursor = conn.cursor()
            channel = session.connection().engine.url.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            sync.truncate_tables(
                [
                    book_cls.__table__.name,
                    contact_item_cls.__table__.name,
                    contact_cls.__table__.name,
                    user_cls.__table__.name,
                ]
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        try:
            sync.search_client.teardown(index="testdb")
            sync.search_client.close()
        except Exception:
            raise

        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.search_client.close()

    @pytest.fixture(scope="function")
    def nodes(self):
        return {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "user",
                    "label": "seller",
                    "columns": ["id", "name"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                        "foreign_key": {
                            "parent": ["seller_id"],
                            "child": ["id"],
                        },
                    },
                    "children": [
                        {
                            "table": "contact",
                            "label": "contacts",
                            "columns": ["id", "name"],
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_many",
                                "foreign_key": {
                                    "parent": ["contact_id"],
                                    "child": ["id"],
                                },
                            },
                            "children": [
                                {
                                    "table": "contact_item",
                                    "label": "contact_items",
                                    "columns": ["id", "name"],
                                    "relationship": {
                                        "variant": "object",
                                        "type": "one_to_many",
                                        "foreign_key": {
                                            "parent": ["id"],
                                            "child": ["contact_id"],
                                        },
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "table": "user",
                    "label": "buyer",
                    "columns": ["id", "name"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                        "foreign_key": {
                            "parent": ["buyer_id"],
                            "child": ["id"],
                        },
                    },
                    "children": [
                        {
                            "table": "contact",
                            "label": "contacts",
                            "columns": ["id", "name"],
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_many",
                                "foreign_key": {
                                    "parent": ["contact_id"],
                                    "child": ["id"],
                                },
                            },
                            "children": [
                                {
                                    "table": "contact_item",
                                    "label": "contact_items",
                                    "columns": ["id", "name"],
                                    "relationship": {
                                        "variant": "object",
                                        "type": "one_to_many",
                                        "foreign_key": {
                                            "parent": ["id"],
                                            "child": ["contact_id"],
                                        },
                                    },
                                }
                            ],
                        }
                    ],
                },
            ],
        }

    def test_sync_multiple_children_empty_leaf(
        self,
        sync,
        data,
        nodes,
        book_cls,
        user_cls,
        contact_cls,
        contact_item_cls,
    ):
        """
                 ----> User(buyer) ----> Contact ----> ContactItem
        Book ----|
                  ----> User(seller) ----> Contact ----> ContactItem
        Test regular sync produces the correct result
        """
        sync.tree.__nodes = {}
        sync.tree = Tree(sync.models, nodes)
        docs = [sort_list(doc) for doc in sync.sync()]
        docs = sorted(docs, key=lambda k: k["_id"])
        assert docs == [
            {
                "_id": "abc",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "book": {"isbn": ["abc"]},
                        "contact": {"id": [1, 2]},
                        "user": {"id": [1, 2]},
                    },
                    "buyer": {
                        "contacts": [
                            {
                                "contact_items": None,
                                "id": 1,
                                "name": "Contact 1",
                            }
                        ],
                        "id": 1,
                        "name": "Fonzy Bear",
                    },
                    "description": "Tigers are fierce creatures",
                    "isbn": "abc",
                    "seller": {
                        "contacts": [
                            {
                                "contact_items": None,
                                "id": 2,
                                "name": "Contact 2",
                            }
                        ],
                        "id": 2,
                        "name": "Jack Jones",
                    },
                    "title": "The Tiger Club",
                },
            }
        ]

        assert_resync_empty(sync, nodes)
