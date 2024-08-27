import pytest
import psycopg2
import typing as t

from pgsync.base import subtransactions, Payload
from pgsync.singleton import Singleton
from pgsync.node import Tree

@pytest.mark.usefixtures("table_creator")
class TestSyncBetweenSchemas(object):
    @pytest.fixture(scope="function")
    def data(self, sync, book_cls, review_cls):
        session = sync.session

        books = [
            book_cls(
                isbn="abc",
                title="The Tiger Club",
                description="Tigers are fierce creatures",
            )
        ]

        with subtransactions(session):
            session.add_all(books)

        reviews = [
            review_cls(
                text="Great book",
                book_isbn="abc",
            ),
        ]

        with subtransactions(session):
            session.add_all(reviews)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )
        Singleton._instances = {}

        yield (
            books,
            reviews,
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
                [book_cls.__table__.name, review_cls.__table__.name],
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )


        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.search_client.close()

    def test_sync_between_schemas(self, sync, data, review_cls):
        nodes = {
            "label": "books",
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "label": "reviews",
                    "table": "review",
                    "columns": ["id", "text"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                        "foreign_key": {
                            "child": [
                                "book_isbn"
                            ],
                            "parent": [
                                "isbn"
                            ]
                        }
                    }
                }
            ]
        }
        sync.tree = Tree(sync.models, nodes)
        docs = list(sync.sync())
        assert docs[0]["_id"] == "abc"
        assert docs[0]["_source"]["reviews"] == [{
            "id": 1,
            "text": "Great book",
        }]
        sync.search_client.bulk(
            sync.index,
            docs,
        )

        session = sync.session
        review = review_cls(
            text="The best book",
            book_isbn="abc",
        )

        with subtransactions(session):
            # Insert a new review
            session.add(review)
            session.commit()

        payloads: t.List[Payload] = [
            Payload(
                tg_op="INSERT",
                table="review",
                schema="public",
                new={"id": "2", "book_isbn": "abc", "text": "The best book"},
                xmin=1234,
            )
        ]
        sync.on_publish(payloads)
        sync.search_client.refresh("testdb")
        docs = sync.search_client.search(
            "testdb", body={"query": {"match_all": {}}}
        )
        assert docs["hits"]["total"]["value"] == 1
        assert docs["hits"]["hits"][0]["_source"]["reviews"] == [
            {"id": 1, "text": "Great book"},
            {"id": 2, "text": "The best book"},
        ]



