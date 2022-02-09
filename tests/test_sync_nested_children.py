"""Tests for `pgsync` package."""

import mock
import psycopg2
import pytest

from pgsync.base import subtransactions
from pgsync.sync import Sync

from .helpers.utils import assert_resync_empty, noop, search


@pytest.mark.usefixtures("table_creator")
class TestNestedChildren(object):
    """Root and nested childred node tests."""

    @pytest.fixture(scope="function")
    def data(
        self,
        sync,
        book_cls,
        publisher_cls,
        author_cls,
        city_cls,
        country_cls,
        continent_cls,
        book_author_cls,
        book_language_cls,
        language_cls,
        book_subject_cls,
        subject_cls,
        book_shelf_cls,
        shelf_cls,
    ):
        session = sync.session

        books = [
            book_cls(
                isbn="abc",
                title="The Tiger Club",
                description="Tigers are fierce creatures",
                publisher=publisher_cls(id=1, name="Tiger publishing"),
            ),
            book_cls(
                isbn="def",
                title="The Lion Club",
                description="Lion and the mouse",
                publisher=publisher_cls(id=2, name="Lion publishing"),
            ),
            book_cls(
                isbn="ghi",
                title="The Rabbit Club",
                description="Rabbits on the run",
                publisher=publisher_cls(id=3, name="Hop Bunny publishing"),
            ),
        ]

        authors = [
            author_cls(
                id=1,
                name="Roald Dahl",
                birth_year=1916,
                city=city_cls(
                    id=1,
                    name="Cardiff",
                    country=country_cls(
                        id=1,
                        name="United Kingdom",
                        continent=continent_cls(id=1, name="Europe"),
                    ),
                ),
            ),
            author_cls(
                id=2,
                name="Haruki Murakami",
                birth_year=1949,
                city=city_cls(
                    id=2,
                    name="Kyoto",
                    country=country_cls(
                        id=2,
                        name="Japan",
                        continent=continent_cls(
                            id=2,
                            name="Asia",
                        ),
                    ),
                ),
            ),
            author_cls(
                id=3,
                name="Alejo Carpentier",
                birth_year=1900,
                city=city_cls(
                    id=3,
                    name="Havana",
                    country=country_cls(
                        id=3,
                        name="Cuba",
                        continent=continent_cls(
                            id=3,
                            name="Americas",
                        ),
                    ),
                ),
            ),
            author_cls(
                id=4,
                name="Kermit D Frog",
                birth_year=1901,
                city=city_cls(
                    id=4,
                    name="Muppet Land",
                    country=country_cls(
                        id=4,
                        name="Mupworld",
                        continent=continent_cls(
                            id=4,
                            name="America",
                        ),
                    ),
                ),
            ),
        ]

        book_authors = [
            book_author_cls(id=1, book=books[0], author=authors[0]),
            book_author_cls(
                id=2,
                book=books[1],
                author=authors[1],
            ),
            book_author_cls(
                id=3,
                book=books[2],
                author=authors[2],
            ),
            book_author_cls(
                id=4,
                book=books[0],
                author=authors[3],
            ),
            book_author_cls(
                id=5,
                book=books[1],
                author=authors[0],
            ),
            book_author_cls(
                id=6,
                book=books[2],
                author=authors[1],
            ),
        ]

        languages = [
            language_cls(id=1, code="EN"),
            language_cls(id=2, code="FR"),
            language_cls(id=3, code="JP"),
            language_cls(id=4, code="CH"),
        ]

        # all books are in EN AND FR
        # 2 books in JP
        # 1 book in CH
        book_languages = [
            book_language_cls(id=1, book=books[0], language=languages[0]),
            book_language_cls(id=2, book=books[1], language=languages[0]),
            book_language_cls(id=3, book=books[2], language=languages[0]),
            book_language_cls(id=4, book=books[0], language=languages[1]),
            book_language_cls(id=5, book=books[1], language=languages[1]),
            book_language_cls(id=6, book=books[2], language=languages[1]),
            book_language_cls(id=7, book=books[0], language=languages[2]),
            book_language_cls(id=8, book=books[1], language=languages[2]),
            book_language_cls(id=9, book=books[0], language=languages[3]),
        ]

        subjects = [
            subject_cls(id=1, name="Fiction"),
            subject_cls(id=2, name="Classic"),
            subject_cls(id=3, name="Literature"),
            subject_cls(id=4, name="Poetry"),
            subject_cls(id=5, name="Romance"),
        ]

        book_subjects = [
            book_subject_cls(id=1, book=books[0], subject=subjects[0]),
            book_subject_cls(id=2, book=books[1], subject=subjects[1]),
            book_subject_cls(id=3, book=books[2], subject=subjects[2]),
            book_subject_cls(id=4, book=books[0], subject=subjects[3]),
            book_subject_cls(id=5, book=books[1], subject=subjects[3]),
            book_subject_cls(id=6, book=books[0], subject=subjects[4]),
            book_subject_cls(id=7, book=books[1], subject=subjects[4]),
            book_subject_cls(id=8, book=books[2], subject=subjects[4]),
        ]

        shelves = [
            shelf_cls(id=1, shelf="Shelf A"),
            shelf_cls(id=2, shelf="Shelf B"),
            shelf_cls(id=3, shelf="Shelf X"),
        ]

        book_shelves = [
            book_shelf_cls(id=1, book=books[0], shelf=shelves[0]),
            book_shelf_cls(id=2, book=books[1], shelf=shelves[0]),
            book_shelf_cls(id=3, book=books[2], shelf=shelves[1]),
            book_shelf_cls(id=4, book=books[0], shelf=shelves[1]),
            book_shelf_cls(id=5, book=books[1], shelf=shelves[1]),
        ]

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cursor = conn.cursor()
            channel = sync.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            session.add_all(books)
            session.add_all(authors)
            session.add_all(book_authors)
            session.add_all(book_languages)
            session.add_all(book_subjects)
            session.add_all(book_shelves)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        yield (
            books,
            authors,
            book_authors,
            book_languages,
            book_subjects,
            book_shelves,
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
                [
                    book_cls.__table__.name,
                    publisher_cls.__table__.name,
                    author_cls.__table__.name,
                    city_cls.__table__.name,
                    country_cls.__table__.name,
                    continent_cls.__table__.name,
                    book_author_cls.__table__.name,
                    book_language_cls.__table__.name,
                    language_cls.__table__.name,
                    book_subject_cls.__table__.name,
                    subject_cls.__table__.name,
                    book_shelf_cls.__table__.name,
                    shelf_cls.__table__.name,
                ]
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        try:
            sync.es.teardown(index="testdb")
            sync.es.close()
        except Exception:
            raise

        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.es.close()

    @pytest.fixture(scope="function")
    def nodes(self):
        return {
            "table": "book",
            "columns": ["isbn", "title", "description"],
            "children": [
                {
                    "table": "publisher",
                    "columns": ["name", "id"],
                    "label": "publisher_label",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "children": [],
                    "transform": {},
                },
                {
                    "table": "book_language",
                    "columns": ["book_isbn", "language_id"],
                    "label": "book_languages",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
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
                            "columns": ["name", "id"],
                            "label": "city_label",
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                            },
                            "children": [
                                {
                                    "table": "country",
                                    "columns": ["name", "id"],
                                    "label": "country_label",
                                    "relationship": {
                                        "variant": "object",
                                        "type": "one_to_one",
                                    },
                                    "children": [
                                        {
                                            "table": "continent",
                                            "columns": ["name"],
                                            "label": "continent_label",
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
            ],
        }

    def test_sync(self, sync, nodes, data):
        """test regular sync produces the correct result."""
        sync.nodes = nodes
        docs = [doc for doc in sync.sync()]
        assert len(docs) == 3
        docs = sorted(docs, key=lambda k: k["_id"])
        expected = [
            {
                "_id": "abc",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [1, 4]},
                        "book_author": {"id": [1, 4]},
                        "book_language": {"id": [1, 4, 7, 9]},
                        "book_subject": {"id": [1, 4, 6]},
                        "city": {"id": [1, 4]},
                        "continent": {"id": [1, 4]},
                        "country": {"id": [1, 4]},
                        "language": {"id": [1, 2, 3, 4]},
                        "publisher": {"id": [1]},
                        "subject": {"id": [1, 4, 5]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "America"},
                                    "id": 4,
                                    "name": "Mupworld",
                                },
                                "id": 4,
                                "name": "Muppet Land",
                            },
                            "id": 4,
                            "name": "Kermit D Frog",
                        },
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Europe"},
                                    "id": 1,
                                    "name": "United Kingdom",
                                },
                                "id": 1,
                                "name": "Cardiff",
                            },
                            "id": 1,
                            "name": "Roald Dahl",
                        },
                    ],
                    "book_languages": [
                        {"book_isbn": "abc", "language_id": 1},
                        {"book_isbn": "abc", "language_id": 2},
                        {"book_isbn": "abc", "language_id": 3},
                        {"book_isbn": "abc", "language_id": 4},
                    ],
                    "description": "Tigers are fierce creatures",
                    "isbn": "abc",
                    "languages": ["CH", "EN", "FR", "JP"],
                    "publisher_label": {"id": 1, "name": "Tiger publishing"},
                    "subjects": ["Fiction", "Poetry", "Romance"],
                    "title": "The Tiger Club",
                },
            },
            {
                "_id": "def",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [1, 2]},
                        "book_author": {"id": [2, 5]},
                        "book_language": {"id": [2, 5, 8]},
                        "book_subject": {"id": [2, 5, 7]},
                        "city": {"id": [1, 2]},
                        "continent": {"id": [1, 2]},
                        "country": {"id": [1, 2]},
                        "language": {"id": [1, 2, 3]},
                        "publisher": {"id": [2]},
                        "subject": {"id": [2, 4, 5]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Asia"},
                                    "id": 2,
                                    "name": "Japan",
                                },
                                "id": 2,
                                "name": "Kyoto",
                            },
                            "id": 2,
                            "name": "Haruki Murakami",
                        },
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Europe"},
                                    "id": 1,
                                    "name": "United Kingdom",
                                },
                                "id": 1,
                                "name": "Cardiff",
                            },
                            "id": 1,
                            "name": "Roald Dahl",
                        },
                    ],
                    "book_languages": [
                        {"book_isbn": "def", "language_id": 1},
                        {"book_isbn": "def", "language_id": 2},
                        {"book_isbn": "def", "language_id": 3},
                    ],
                    "description": "Lion and the mouse",
                    "isbn": "def",
                    "languages": ["EN", "FR", "JP"],
                    "publisher_label": {"id": 2, "name": "Lion publishing"},
                    "subjects": ["Classic", "Poetry", "Romance"],
                    "title": "The Lion Club",
                },
            },
            {
                "_id": "ghi",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [2, 3]},
                        "book_author": {"id": [3, 6]},
                        "book_language": {"id": [3, 6]},
                        "book_subject": {"id": [3, 8]},
                        "city": {"id": [2, 3]},
                        "continent": {"id": [2, 3]},
                        "country": {"id": [2, 3]},
                        "language": {"id": [1, 2]},
                        "publisher": {"id": [3]},
                        "subject": {"id": [3, 5]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Americas"},
                                    "id": 3,
                                    "name": "Cuba",
                                },
                                "id": 3,
                                "name": "Havana",
                            },
                            "id": 3,
                            "name": "Alejo Carpentier",
                        },
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Asia"},
                                    "id": 2,
                                    "name": "Japan",
                                },
                                "id": 2,
                                "name": "Kyoto",
                            },
                            "id": 2,
                            "name": "Haruki Murakami",
                        },
                    ],
                    "book_languages": [
                        {"book_isbn": "ghi", "language_id": 1},
                        {"book_isbn": "ghi", "language_id": 2},
                    ],
                    "description": "Rabbits on the run",
                    "isbn": "ghi",
                    "languages": ["EN", "FR"],
                    "publisher_label": {
                        "id": 3,
                        "name": "Hop Bunny publishing",
                    },
                    "subjects": ["Literature", "Romance"],
                    "title": "The Rabbit Club",
                },
            },
        ]

        for i, doc in enumerate(docs):
            assert doc["_id"] == expected[i]["_id"]
            assert doc["_index"] == expected[i]["_index"]
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors":
                    assert sorted(
                        doc["_source"][key], key=lambda k: k["id"]
                    ) == sorted(
                        expected[i]["_source"][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc["_source"][key] == expected[i]["_source"][key]

        assert_resync_empty(sync, nodes)

    def test_insert_root(
        self,
        data,
        nodes,
        book_cls,
        publisher_cls,
        author_cls,
        city_cls,
        country_cls,
        continent_cls,
        book_author_cls,
        book_language_cls,
        language_cls,
        book_subject_cls,
        subject_cls,
        book_shelf_cls,
        shelf_cls,
    ):
        """Test insert a new root item."""
        books = [
            book_cls(
                isbn="jkl",
                title="The Giraffe Express",
                description="Giraffes are funny animals",
                publisher=publisher_cls(id=4, name="Giraff publishing"),
            ),
            book_cls(
                isbn="mno",
                title="The Tortoise Orient",
                description="Tortoise and the hare",
                publisher=publisher_cls(id=5, name="Tortoise publishing"),
            ),
        ]

        authors = [
            author_cls(
                id=5,
                name="Mr. Pig",
                birth_year=2099,
                city=city_cls(
                    id=5,
                    name="Rio",
                    country=country_cls(
                        id=5,
                        name="Brazil",
                        continent=continent_cls(id=5, name="South America"),
                    ),
                ),
            )
        ]

        book_authors = [
            book_author_cls(id=7, book=books[0], author=authors[0]),
            book_author_cls(id=8, book=books[1], author=authors[0]),
        ]

        languages = [
            language_cls(id=5, code="PO"),
            language_cls(id=6, code="ZH"),
        ]

        book_languages = [
            book_language_cls(id=10, book=books[0], language=languages[0]),
            book_language_cls(id=11, book=books[0], language=languages[1]),
            book_language_cls(id=12, book=books[1], language=languages[0]),
            book_language_cls(id=13, book=books[1], language=languages[1]),
        ]

        subjects = [
            subject_cls(id=6, name="Self Help"),
        ]

        book_subjects = [
            book_subject_cls(id=9, book=books[0], subject=subjects[0]),
            book_subject_cls(id=10, book=books[1], subject=subjects[0]),
        ]

        shelves = [
            shelf_cls(id=3, shelf="Shelf C"),
            shelf_cls(id=4, shelf="Shelf D"),
        ]

        book_shelves = [
            book_shelf_cls(id=6, book=books[0], shelf=shelves[0]),
            book_shelf_cls(id=7, book=books[0], shelf=shelves[1]),
        ]

        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current

        session = sync.session

        with subtransactions(session):
            session.add_all(books)
            session.add_all(authors)
            session.add_all(book_authors)
            session.add_all(book_languages)
            session.add_all(book_subjects)
            session.add_all(book_shelves)

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert len(docs) == 2
        docs = sorted(docs, key=lambda k: k["_id"])
        assert docs == [
            {
                "_id": "jkl",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [5]},
                        "book_author": {"id": [7]},
                        "book_language": {"id": [10, 11]},
                        "book_subject": {"id": [9]},
                        "city": {"id": [5]},
                        "continent": {"id": [5]},
                        "country": {"id": [5]},
                        "language": {"id": [5, 6]},
                        "publisher": {"id": [4]},
                        "subject": {"id": [6]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {
                                        "name": "South America"
                                    },
                                    "id": 5,
                                    "name": "Brazil",
                                },
                                "id": 5,
                                "name": "Rio",
                            },
                            "id": 5,
                            "name": "Mr. Pig",
                        }
                    ],
                    "book_languages": [
                        {"book_isbn": "jkl", "language_id": 5},
                        {"book_isbn": "jkl", "language_id": 6},
                    ],
                    "description": "Giraffes are funny animals",
                    "isbn": "jkl",
                    "languages": ["PO", "ZH"],
                    "publisher_label": {"id": 4, "name": "Giraff publishing"},
                    "subjects": ["Self Help"],
                    "title": "The Giraffe Express",
                },
            },
            {
                "_id": "mno",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [5]},
                        "book_author": {"id": [8]},
                        "book_language": {"id": [12, 13]},
                        "book_subject": {"id": [10]},
                        "city": {"id": [5]},
                        "continent": {"id": [5]},
                        "country": {"id": [5]},
                        "language": {"id": [5, 6]},
                        "publisher": {"id": [5]},
                        "subject": {"id": [6]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {
                                        "name": "South America"
                                    },
                                    "id": 5,
                                    "name": "Brazil",
                                },
                                "id": 5,
                                "name": "Rio",
                            },
                            "id": 5,
                            "name": "Mr. Pig",
                        }
                    ],
                    "book_languages": [
                        {"book_isbn": "mno", "language_id": 5},
                        {"book_isbn": "mno", "language_id": 6},
                    ],
                    "description": "Tortoise and the hare",
                    "isbn": "mno",
                    "languages": ["PO", "ZH"],
                    "publisher_label": {
                        "id": 5,
                        "name": "Tortoise publishing",
                    },
                    "subjects": ["Self Help"],
                    "title": "The Tortoise Orient",
                },
            },
        ]
        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_update_root(self, data, nodes, book_cls):
        document = {
            "index": "testdb",
            "nodes": nodes,
        }
        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current

        session = sync.session

        with subtransactions(session):
            session.execute(
                book_cls.__table__.update()
                .where(book_cls.__table__.c.isbn == "abc")
                .values(description="xcaliber")
            )

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]

        assert len(docs) == 1
        docs = sorted(docs, key=lambda k: k["_id"])
        expected = [
            {
                "_id": "abc",
                "_index": "testdb",
                "_source": {
                    "_meta": {
                        "author": {"id": [1, 4]},
                        "book_author": {"id": [1, 4]},
                        "book_language": {"id": [1, 4, 7, 9]},
                        "book_subject": {"id": [1, 4, 6]},
                        "city": {"id": [1, 4]},
                        "continent": {"id": [1, 4]},
                        "country": {"id": [1, 4]},
                        "language": {"id": [1, 2, 3, 4]},
                        "publisher": {"id": [1]},
                        "subject": {"id": [1, 4, 5]},
                    },
                    "authors": [
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "America"},
                                    "id": 4,
                                    "name": "Mupworld",
                                },
                                "id": 4,
                                "name": "Muppet Land",
                            },
                            "id": 4,
                            "name": "Kermit D Frog",
                        },
                        {
                            "city_label": {
                                "country_label": {
                                    "continent_label": {"name": "Europe"},
                                    "id": 1,
                                    "name": "United Kingdom",
                                },
                                "id": 1,
                                "name": "Cardiff",
                            },
                            "id": 1,
                            "name": "Roald Dahl",
                        },
                    ],
                    "book_languages": [
                        {"book_isbn": "abc", "language_id": 1},
                        {"book_isbn": "abc", "language_id": 2},
                        {"book_isbn": "abc", "language_id": 3},
                        {"book_isbn": "abc", "language_id": 4},
                    ],
                    "description": "xcaliber",
                    "isbn": "abc",
                    "languages": ["CH", "EN", "FR", "JP"],
                    "publisher_label": {"id": 1, "name": "Tiger publishing"},
                    "subjects": ["Fiction", "Poetry", "Romance"],
                    "title": "The Tiger Club",
                },
            }
        ]
        for i, doc in enumerate(docs):
            assert doc["_id"] == expected[i]["_id"]
            assert doc["_index"] == expected[i]["_index"]
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors":
                    assert sorted(
                        doc["_source"][key], key=lambda k: k["id"]
                    ) == sorted(
                        expected[i]["_source"][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc["_source"][key] == expected[i]["_source"][key]

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_delete_root(
        self,
        data,
        nodes,
        book_cls,
        book_shelf_cls,
        book_language_cls,
        book_subject_cls,
        book_author_cls,
    ):
        document = {
            "index": "testdb",
            "nodes": nodes,
        }
        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current

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
                    book_shelf_cls.__table__.delete().where(
                        book_shelf_cls.__table__.c.book_isbn == "abc"
                    )
                )
                session.execute(
                    book_language_cls.__table__.delete().where(
                        book_language_cls.__table__.c.book_isbn == "abc"
                    )
                )
                session.execute(
                    book_subject_cls.__table__.delete().where(
                        book_subject_cls.__table__.c.book_isbn == "abc"
                    )
                )
                session.execute(
                    book_author_cls.__table__.delete().where(
                        book_author_cls.__table__.c.book_isbn == "abc"
                    )
                )
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
                            sync.es.refresh("testdb")

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert len(docs) == 0

        docs = search(sync.es, "testdb")

        assert len(docs) == 2
        docs = sorted(docs, key=lambda k: k["isbn"])
        expected = [
            {
                "_meta": {
                    "author": {"id": [1, 2]},
                    "book_author": {"id": [2, 5]},
                    "book_language": {"id": [2, 5, 8]},
                    "book_subject": {"id": [2, 5, 7]},
                    "city": {"id": [1, 2]},
                    "continent": {"id": [1, 2]},
                    "country": {"id": [1, 2]},
                    "language": {"id": [1, 2, 3]},
                    "publisher": {"id": [2]},
                    "subject": {"id": [2, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Europe"},
                                "id": 1,
                                "name": "United Kingdom",
                            },
                            "id": 1,
                            "name": "Cardiff",
                        },
                        "id": 1,
                        "name": "Roald Dahl",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "def", "language_id": 1},
                    {"book_isbn": "def", "language_id": 2},
                    {"book_isbn": "def", "language_id": 3},
                ],
                "description": "Lion and the mouse",
                "isbn": "def",
                "languages": ["EN", "FR", "JP"],
                "publisher_label": {"id": 2, "name": "Lion publishing"},
                "subjects": ["Classic", "Poetry", "Romance"],
                "title": "The Lion Club",
            },
            {
                "_meta": {
                    "author": {"id": [2, 3]},
                    "book_author": {"id": [3, 6]},
                    "book_language": {"id": [3, 6]},
                    "book_subject": {"id": [3, 8]},
                    "city": {"id": [2, 3]},
                    "continent": {"id": [2, 3]},
                    "country": {"id": [2, 3]},
                    "language": {"id": [1, 2]},
                    "publisher": {"id": [3]},
                    "subject": {"id": [3, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Americas"},
                                "id": 3,
                                "name": "Cuba",
                            },
                            "id": 3,
                            "name": "Havana",
                        },
                        "id": 3,
                        "name": "Alejo Carpentier",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "ghi", "language_id": 1},
                    {"book_isbn": "ghi", "language_id": 2},
                ],
                "description": "Rabbits on the run",
                "isbn": "ghi",
                "languages": ["EN", "FR"],
                "publisher_label": {"id": 3, "name": "Hop Bunny publishing"},
                "subjects": ["Literature", "Romance"],
                "title": "The Rabbit Club",
            },
        ]
        for i, doc in enumerate(docs):
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors":
                    assert sorted(doc[key], key=lambda k: k["id"]) == sorted(
                        expected[i][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc[key] == expected[i][key]

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_insert_through_child_noop(self, sync, data):
        # insert a new through child with noop
        pass

    def test_update_through_child_noop(self, sync, data):
        # update a new through child with noop
        pass

    def test_delete_through_child_noop(self, sync, data):
        # delete a new through child with noop
        pass

    def test_insert_through_child_op(
        self,
        data,
        nodes,
        book_cls,
        author_cls,
        city_cls,
        country_cls,
        continent_cls,
        book_author_cls,
    ):
        """insert a new through child with op."""
        book_author = book_author_cls(
            id=7,
            book_isbn="abc",
            author=author_cls(
                id=5,
                name="Mr. Bird",
                birth_year=2039,
                city=city_cls(
                    id=5,
                    name="Lagos",
                    country=country_cls(
                        id=5,
                        name="Nigeria",
                        continent=continent_cls(
                            id=6,
                            name="Africa",
                        ),
                    ),
                ),
            ),
        )

        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)

        session = sync.session

        with subtransactions(session):
            session.add(book_author)

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3
        docs = sorted(docs, key=lambda k: k["isbn"])

        expected = [
            {
                "_meta": {
                    "author": {"id": [1, 4, 5]},
                    "book_author": {"id": [1, 4, 7]},
                    "book_language": {"id": [1, 4, 7, 9]},
                    "book_subject": {"id": [1, 4, 6]},
                    "city": {"id": [1, 4, 5]},
                    "continent": {"id": [1, 4, 6]},
                    "country": {"id": [1, 4, 5]},
                    "language": {"id": [1, 2, 3, 4]},
                    "publisher": {"id": [1]},
                    "subject": {"id": [1, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Europe"},
                                "id": 1,
                                "name": "United Kingdom",
                            },
                            "id": 1,
                            "name": "Cardiff",
                        },
                        "id": 1,
                        "name": "Roald Dahl",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "America"},
                                "id": 4,
                                "name": "Mupworld",
                            },
                            "id": 4,
                            "name": "Muppet Land",
                        },
                        "id": 4,
                        "name": "Kermit D Frog",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Africa"},
                                "id": 5,
                                "name": "Nigeria",
                            },
                            "id": 5,
                            "name": "Lagos",
                        },
                        "id": 5,
                        "name": "Mr. Bird",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "abc", "language_id": 1},
                    {"book_isbn": "abc", "language_id": 2},
                    {"book_isbn": "abc", "language_id": 3},
                    {"book_isbn": "abc", "language_id": 4},
                ],
                "description": "Tigers are fierce creatures",
                "isbn": "abc",
                "languages": ["CH", "EN", "FR", "JP"],
                "publisher_label": {"id": 1, "name": "Tiger publishing"},
                "subjects": ["Fiction", "Poetry", "Romance"],
                "title": "The Tiger Club",
            },
            {
                "_meta": {
                    "author": {"id": [1, 2]},
                    "book_author": {"id": [2, 5]},
                    "book_language": {"id": [2, 5, 8]},
                    "book_subject": {"id": [2, 5, 7]},
                    "city": {"id": [1, 2]},
                    "continent": {"id": [1, 2]},
                    "country": {"id": [1, 2]},
                    "language": {"id": [1, 2, 3]},
                    "publisher": {"id": [2]},
                    "subject": {"id": [2, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Europe"},
                                "id": 1,
                                "name": "United Kingdom",
                            },
                            "id": 1,
                            "name": "Cardiff",
                        },
                        "id": 1,
                        "name": "Roald Dahl",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "def", "language_id": 1},
                    {"book_isbn": "def", "language_id": 2},
                    {"book_isbn": "def", "language_id": 3},
                ],
                "description": "Lion and the mouse",
                "isbn": "def",
                "languages": ["EN", "FR", "JP"],
                "publisher_label": {"id": 2, "name": "Lion publishing"},
                "subjects": ["Classic", "Poetry", "Romance"],
                "title": "The Lion Club",
            },
            {
                "_meta": {
                    "author": {"id": [2, 3]},
                    "book_author": {"id": [3, 6]},
                    "book_language": {"id": [3, 6]},
                    "book_subject": {"id": [3, 8]},
                    "city": {"id": [2, 3]},
                    "continent": {"id": [2, 3]},
                    "country": {"id": [2, 3]},
                    "language": {"id": [1, 2]},
                    "publisher": {"id": [3]},
                    "subject": {"id": [3, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Americas"},
                                "id": 3,
                                "name": "Cuba",
                            },
                            "id": 3,
                            "name": "Havana",
                        },
                        "id": 3,
                        "name": "Alejo Carpentier",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "ghi", "language_id": 1},
                    {"book_isbn": "ghi", "language_id": 2},
                ],
                "description": "Rabbits on the run",
                "isbn": "ghi",
                "languages": ["EN", "FR"],
                "publisher_label": {"id": 3, "name": "Hop Bunny publishing"},
                "subjects": ["Literature", "Romance"],
                "title": "The Rabbit Club",
            },
        ]
        for i, doc in enumerate(docs):
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors":
                    assert sorted(doc[key], key=lambda k: k["id"]) == sorted(
                        expected[i][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc[key] == expected[i][key]

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_update_through_child_op(
        self,
        sync,
        nodes,
        data,
        book_author_cls,
        author_cls,
        city_cls,
        country_cls,
        continent_cls,
    ):
        # update a new through child with op
        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())

        author = author_cls(
            id=5,
            name="Mr. Horse",
            birth_year=1999,
            city=city_cls(
                id=5,
                name="Laos",
                country=country_cls(
                    id=5,
                    name="Mauritius",
                    continent=continent_cls(id=6, name="Americana"),
                ),
            ),
        )

        session = sync.session
        with subtransactions(session):
            session.add(author)

        with subtransactions(session):
            session.execute(
                book_author_cls.__table__.update()
                .where(book_author_cls.__table__.c.id == 1)
                .values(author_id=5)
            )

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3
        docs = sorted(docs, key=lambda k: k["isbn"])
        expected = [
            {
                "_meta": {
                    "author": {"id": [4, 5]},
                    "book_author": {"id": [1, 4]},
                    "book_language": {"id": [1, 4, 7, 9]},
                    "book_subject": {"id": [1, 4, 6]},
                    "city": {"id": [4, 5]},
                    "continent": {"id": [4, 6]},
                    "country": {"id": [4, 5]},
                    "language": {"id": [1, 2, 3, 4]},
                    "publisher": {"id": [1]},
                    "subject": {"id": [1, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "America"},
                                "id": 4,
                                "name": "Mupworld",
                            },
                            "id": 4,
                            "name": "Muppet Land",
                        },
                        "id": 4,
                        "name": "Kermit D Frog",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Americana"},
                                "id": 5,
                                "name": "Mauritius",
                            },
                            "id": 5,
                            "name": "Laos",
                        },
                        "id": 5,
                        "name": "Mr. Horse",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "abc", "language_id": 1},
                    {"book_isbn": "abc", "language_id": 2},
                    {"book_isbn": "abc", "language_id": 3},
                    {"book_isbn": "abc", "language_id": 4},
                ],
                "description": "Tigers are fierce creatures",
                "isbn": "abc",
                "languages": ["CH", "EN", "FR", "JP"],
                "publisher_label": {"id": 1, "name": "Tiger publishing"},
                "subjects": ["Fiction", "Poetry", "Romance"],
                "title": "The Tiger Club",
            },
            {
                "_meta": {
                    "author": {"id": [1, 2]},
                    "book_author": {"id": [2, 5]},
                    "book_language": {"id": [2, 5, 8]},
                    "book_subject": {"id": [2, 5, 7]},
                    "city": {"id": [1, 2]},
                    "continent": {"id": [1, 2]},
                    "country": {"id": [1, 2]},
                    "language": {"id": [1, 2, 3]},
                    "publisher": {"id": [2]},
                    "subject": {"id": [2, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Europe"},
                                "id": 1,
                                "name": "United Kingdom",
                            },
                            "id": 1,
                            "name": "Cardiff",
                        },
                        "id": 1,
                        "name": "Roald Dahl",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "def", "language_id": 1},
                    {"book_isbn": "def", "language_id": 2},
                    {"book_isbn": "def", "language_id": 3},
                ],
                "description": "Lion and the mouse",
                "isbn": "def",
                "languages": ["EN", "FR", "JP"],
                "publisher_label": {"id": 2, "name": "Lion publishing"},
                "subjects": ["Classic", "Poetry", "Romance"],
                "title": "The Lion Club",
            },
            {
                "_meta": {
                    "author": {"id": [2, 3]},
                    "book_author": {"id": [3, 6]},
                    "book_language": {"id": [3, 6]},
                    "book_subject": {"id": [3, 8]},
                    "city": {"id": [2, 3]},
                    "continent": {"id": [2, 3]},
                    "country": {"id": [2, 3]},
                    "language": {"id": [1, 2]},
                    "publisher": {"id": [3]},
                    "subject": {"id": [3, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Americas"},
                                "id": 3,
                                "name": "Cuba",
                            },
                            "id": 3,
                            "name": "Havana",
                        },
                        "id": 3,
                        "name": "Alejo Carpentier",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "ghi", "language_id": 1},
                    {"book_isbn": "ghi", "language_id": 2},
                ],
                "description": "Rabbits on the run",
                "isbn": "ghi",
                "languages": ["EN", "FR"],
                "publisher_label": {"id": 3, "name": "Hop Bunny publishing"},
                "subjects": ["Literature", "Romance"],
                "title": "The Rabbit Club",
            },
        ]
        for i, doc in enumerate(docs):
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors":
                    assert sorted(doc[key], key=lambda k: k["id"]) == sorted(
                        expected[i][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc[key] == expected[i][key]

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_delete_through_child_op(self, sync, data, nodes, book_author_cls):
        # delete a new through child with op
        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())

        session = sync.session

        with subtransactions(session):
            session.execute(
                book_author_cls.__table__.delete().where(
                    book_author_cls.__table__.c.book_isbn == "abc"
                )
            )
            session.commit()

        sync.es.bulk(sync.index, sync.sync())
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3
        docs = sorted(docs, key=lambda k: k["isbn"])
        expected = [
            {
                "_meta": {
                    "book_language": {"id": [1, 4, 7, 9]},
                    "book_subject": {"id": [1, 4, 6]},
                    "language": {"id": [1, 2, 3, 4]},
                    "publisher": {"id": [1]},
                    "subject": {"id": [1, 4, 5]},
                },
                "authors": None,
                "book_languages": [
                    {"book_isbn": "abc", "language_id": 1},
                    {"book_isbn": "abc", "language_id": 2},
                    {"book_isbn": "abc", "language_id": 3},
                    {"book_isbn": "abc", "language_id": 4},
                ],
                "description": "Tigers are fierce creatures",
                "isbn": "abc",
                "languages": ["CH", "EN", "FR", "JP"],
                "publisher_label": {"id": 1, "name": "Tiger publishing"},
                "subjects": ["Fiction", "Poetry", "Romance"],
                "title": "The Tiger Club",
            },
            {
                "_meta": {
                    "author": {"id": [1, 2]},
                    "book_author": {"id": [2, 5]},
                    "book_language": {"id": [2, 5, 8]},
                    "book_subject": {"id": [2, 5, 7]},
                    "city": {"id": [1, 2]},
                    "continent": {"id": [1, 2]},
                    "country": {"id": [1, 2]},
                    "language": {"id": [1, 2, 3]},
                    "publisher": {"id": [2]},
                    "subject": {"id": [2, 4, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Europe"},
                                "id": 1,
                                "name": "United Kingdom",
                            },
                            "id": 1,
                            "name": "Cardiff",
                        },
                        "id": 1,
                        "name": "Roald Dahl",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "def", "language_id": 1},
                    {"book_isbn": "def", "language_id": 2},
                    {"book_isbn": "def", "language_id": 3},
                ],
                "description": "Lion and the mouse",
                "isbn": "def",
                "languages": ["EN", "FR", "JP"],
                "publisher_label": {"id": 2, "name": "Lion publishing"},
                "subjects": ["Classic", "Poetry", "Romance"],
                "title": "The Lion Club",
            },
            {
                "_meta": {
                    "author": {"id": [2, 3]},
                    "book_author": {"id": [3, 6]},
                    "book_language": {"id": [3, 6]},
                    "book_subject": {"id": [3, 8]},
                    "city": {"id": [2, 3]},
                    "continent": {"id": [2, 3]},
                    "country": {"id": [2, 3]},
                    "language": {"id": [1, 2]},
                    "publisher": {"id": [3]},
                    "subject": {"id": [3, 5]},
                },
                "authors": [
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Asia"},
                                "id": 2,
                                "name": "Japan",
                            },
                            "id": 2,
                            "name": "Kyoto",
                        },
                        "id": 2,
                        "name": "Haruki Murakami",
                    },
                    {
                        "city_label": {
                            "country_label": {
                                "continent_label": {"name": "Americas"},
                                "id": 3,
                                "name": "Cuba",
                            },
                            "id": 3,
                            "name": "Havana",
                        },
                        "id": 3,
                        "name": "Alejo Carpentier",
                    },
                ],
                "book_languages": [
                    {"book_isbn": "ghi", "language_id": 1},
                    {"book_isbn": "ghi", "language_id": 2},
                ],
                "description": "Rabbits on the run",
                "isbn": "ghi",
                "languages": ["EN", "FR"],
                "publisher_label": {"id": 3, "name": "Hop Bunny publishing"},
                "subjects": ["Literature", "Romance"],
                "title": "The Rabbit Club",
            },
        ]
        for i, doc in enumerate(docs):
            for key in [
                "_meta",
                "authors",
                "book_languages",
                "description",
                "isbn",
                "languages",
                "publisher_label",
                "subjects",
                "title",
            ]:
                if key == "authors" and doc[key] is not None:
                    assert sorted(doc[key], key=lambda k: k["id"]) == sorted(
                        expected[i][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc[key] == expected[i][key]

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_insert_nonthrough_child_noop(
        self,
        data,
        nodes,
        city_cls,
        country_cls,
        continent_cls,
    ):
        """insert a new non-through child with noop."""
        city = city_cls(
            id=5,
            name="Moscow",
            country=country_cls(
                id=5,
                name="Russia",
                continent=continent_cls(id=6, name="Eastern Europe"),
            ),
        )

        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3

        session = sync.session
        with subtransactions(session):
            session.add(city)

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert len(docs) == 0

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_update_nonthrough_child_noop(self, data, nodes, shelf_cls):
        # update a new non-through child with noop
        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3

        session = sync.session
        with subtransactions(session):
            session.execute(
                shelf_cls.__table__.update()
                .where(shelf_cls.__table__.c.id == 3)
                .values(shelf="Shelf Y")
            )

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert len(docs) == 0

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_delete_nonthrough_child_noop(self, data, nodes, shelf_cls):
        # delete a new non-through child with noop
        document = {
            "index": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current
        sync.checkpoint = sync.txid_current
        sync.es.refresh("testdb")

        docs = search(sync.es, "testdb")

        assert len(docs) == 3

        session = sync.session
        with subtransactions(session):
            session.execute(
                shelf_cls.__table__.delete().where(
                    shelf_cls.__table__.c.id == 3
                )
            )

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert len(docs) == 0

        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_insert_nonthrough_child_op(self, sync, data):
        # insert a new non-through child with op
        pass

    def test_update_nonthrough_child_op(self, sync, data):
        # update a new non-through child with op
        pass

    def test_delete_nonthrough_child_op(self, sync, data):
        # delete a new non-through child with op
        pass

    @pytest.fixture(scope="function")
    def node2(self):
        return [
            {
                "table": "book",
                "columns": ["isbn", "title", "description"],
                "children": [
                    {
                        "table": "publisher",
                        "columns": ["name", "id"],
                        "label": "publisher_label",
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_one",
                        },
                        "children": [],
                        "transform": {},
                    },
                    {
                        "table": "book_language",
                        "columns": ["book_isbn", "language_id"],
                        "label": "book_languages",
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_many",
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
                                "columns": ["name", "id"],
                                "label": "city_label",
                                "relationship": {
                                    "variant": "object",
                                    "type": "one_to_one",
                                },
                                "children": [
                                    {
                                        "table": "country",
                                        "columns": ["name", "id"],
                                        "label": "country_label",
                                        "relationship": {
                                            "variant": "object",
                                            "type": "one_to_many",
                                        },
                                        "children": [
                                            {
                                                "table": "continent",
                                                "columns": ["name"],
                                                "label": "continent_label",
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
                ],
            }
        ]

    def test_insert_deep_nested_nonthrough_child_noop(
        self,
        data,
        nodes,
        city_cls,
        country_cls,
        continent_cls,
    ):
        """insert a new deep nested non-through child with noop."""
        country = country_cls(
            id=5,
            name="Marioworld",
            continent=continent_cls(id=6, name="Bowser Land"),
        )

        document = {
            "index": "testdb",
            "nodes": nodes,
        }
        # sync first to add the initial document
        sync = Sync(document)
        sync.es.bulk(sync.index, sync.sync())
        sync.checkpoint = sync.txid_current
        session = sync.session
        sync.es.refresh("testdb")

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.add(country)

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

        txmin = sync.checkpoint
        sync.nodes = nodes
        docs = [doc for doc in sync.sync(txmin=txmin)]
        assert docs == []
        docs = search(sync.es, "testdb")
        assert_resync_empty(sync, nodes)
        sync.es.close()

    def test_insert_deep_nested_fk_nonthrough_child_op(
        self,
        data,
        nodes,
        city_cls,
        country_cls,
        continent_cls,
    ):
        """insert a new deep nested non-through fk child with op."""
        nodes["children"].append(
            {
                "table": "book_rating",
                "columns": [
                    "book_isbn",
                    "rating",
                ],
                "label": "book_ratings",
                "relationship": {
                    "variant": "object",
                    "type": "one_to_many",
                },
            },
        )
        # TODO
        # {
        #            "table": "book_language",
        #            "columns": [
        #                "book_isbn",
        #                "language_id"
        #            ],
        #            "label": "book_languages",
        #            "relationship": {
        #                "variant": "object",
        #                "type": "one_to_many"
        #            }
        #        },
