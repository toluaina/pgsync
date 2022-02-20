"""Node tests."""
import pytest

from pgsync.node import Tree


@pytest.mark.usefixtures("table_creator")
class TestNode(object):
    """Node tests."""

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

    def test_traverse_breadth_first(self, sync, nodes):
        tree = Tree(sync)
        root = tree.build(nodes)
        root.display()
        for i, node in enumerate(root.traverse_breadth_first()):
            if i == 0:
                assert node.table == "book"
            if i == 1:
                assert node.table == "publisher"
            if i == 2:
                assert node.table == "book_language"
            if i == 3:
                assert node.table == "author"
            if i == 4:
                assert node.table == "language"
            if i == 5:
                assert node.table == "subject"
            if i == 6:
                assert node.table == "city"
            if i == 7:
                assert node.table == "country"
            if i == 8:
                assert node.table == "continent"
        sync.es.close()

    def test_traverse_post_order(self, sync, nodes):
        tree = Tree(sync)
        root = tree.build(nodes)
        root.display()
        for i, node in enumerate(root.traverse_post_order()):
            if i == 0:
                assert node.table == "publisher"
            if i == 1:
                assert node.table == "book_language"
            if i == 2:
                assert node.table == "continent"
            if i == 3:
                assert node.table == "country"
            if i == 4:
                assert node.table == "city"
            if i == 5:
                assert node.table == "author"
            if i == 6:
                assert node.table == "language"
            if i == 7:
                assert node.table == "subject"
            if i == 8:
                assert node.table == "book"
        sync.es.close()
