"""Transform tests."""

import pytest

from pgsync.constants import CONCAT_TRANSFORM, RENAME_TRANSFORM
from pgsync.transform import Transform


@pytest.mark.usefixtures("table_creator")
class TestTransform(object):
    """Transform tests."""

    def test_get_transform(self):
        nodes = {
            "table": "tableau",
            "columns": [
                "id",
                "code",
                "level",
            ],
            "children": [
                {
                    "table": "child_1",
                    "columns": [
                        "column_1",
                        "column_2",
                    ],
                    "label": "Child1",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {
                        "rename": {"column_1": "column1"},
                    },
                    "children": [
                        {
                            "table": "grandchild_1",
                            "columns": [
                                "column_1",
                                "column_2",
                            ],
                            "label": "Grandchild_1",
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                            },
                            "transform": {
                                "rename": {"column_2": "column2"},
                                "concat": {
                                    "columns": ["column_1", "column_2"],
                                    "destination": "column_3",
                                    "delimiter": "_",
                                },
                            },
                        },
                    ],
                },
            ],
            "transform": {
                "rename": {
                    "id": "my_id",
                    "code": "my_code",
                    "level": "levelup",
                },
                "concat": {
                    "columns": ["column_1", "column_2", "column_3"],
                    "destination": "column_3",
                    "delimiter": "x",
                },
            },
        }

        transform_node = Transform.get(nodes, RENAME_TRANSFORM)
        assert transform_node == {
            "id": "my_id",
            "code": "my_code",
            "level": "levelup",
            "Child1": {
                "Grandchild_1": {
                    "column_2": "column2",
                },
                "column_1": "column1",
            },
        }

        transform_node = Transform.get(nodes, CONCAT_TRANSFORM)
        assert transform_node == {
            "Child1": {
                "Grandchild_1": {
                    "columns": ["column_1", "column_2"],
                    "delimiter": "_",
                    "destination": "column_3",
                },
            },
            "columns": ["column_1", "column_2", "column_3"],
            "delimiter": "x",
            "destination": "column_3",
        }

    def test_transform_rename(self):
        nodes = {
            "table": "tableau",
            "columns": [
                "id",
                "code",
                "level",
                "foo",
                "bar",
            ],
            "children": [
                {
                    "table": "child_1",
                    "columns": [
                        "column_1",
                        "column_2",
                    ],
                    "label": "Child1",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {"rename": {"column_1": "column1"}},
                },
                {
                    "table": "child_2",
                    "columns": [
                        "column_1",
                        "column_2",
                    ],
                    "label": "Child2",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                    },
                    "transform": {"rename": {"column_2": "column2"}},
                },
            ],
            "transform": {
                "rename": {
                    "id": "my_id",
                    "code": "my_code",
                    "level": "levelup",
                    "foo": "foos",
                    "bar": "bars",
                    "xxx": 42,
                }
            },
        }

        row = {
            "level": 1,
            "id": "007",
            "code": "be",
            "foo": ["a", "b"],
            "bar": {"a": 1, "b": 2},
            "xxx": "42.0",
            "Child1": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb", "column_3": [1, "a"]},
            ],
        }
        row = Transform.transform(row, nodes)
        assert row == {
            "Child1": [
                {"column1": 2, "column_2": "aa"},
                {"column1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column2": "aa", "column_1": 2},
                {"column2": "bb", "column_1": 3, "column_3": [1, "a"]},
            ],
            "levelup": 1,
            "my_code": "be",
            "my_id": "007",
            "foos": ["a", "b"],
            "bars": {"a": 1, "b": 2},
            "42": "42.0",
        }

    def test_rename_fields(self):
        nodes = {
            "transform": {
                "rename": {
                    "id": "my_id",
                    "code": "my_code",
                    "level": "levelup",
                    "Child1": {"column_1": "column1"},
                }
            }
        }
        row = {
            "level": 1,
            "id": "007",
            "code": "be",
            "Child1": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
        }
        row = Transform.rename(row, nodes)

        assert row == {
            "levelup": 1,
            "my_id": "007",
            "my_code": "be",
            "Child1": [
                {"column1": 2, "column_2": "aa"},
                {"column1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
        }

    def test_transform_concat(self):
        nodes = {
            "table": "tableau",
            "columns": [
                "id",
                "code",
                "level",
            ],
            "children": [
                {
                    "table": "Child1",
                    "columns": [
                        "column_1",
                        "column_2",
                    ],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {
                        "concat": {
                            "columns": ["column_1", "column_2"],
                            "destination": "column_3",
                            "delimiter": "_",
                        }
                    },
                },
                {
                    "table": "Child2",
                    "columns": [
                        "column1",
                        "column2",
                    ],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                    },
                    "transform": {
                        "concat": [
                            {
                                "columns": [
                                    "http",
                                    "://",
                                    "column1",
                                    "/",
                                    "path",
                                ],
                                "destination": "column3",
                            },
                            {
                                "columns": [
                                    "http",
                                    "://",
                                    "column1",
                                    "/",
                                    "path",
                                ],
                                "destination": "column4",
                            },
                        ]
                    },
                },
                {
                    "table": "Child3",
                    "columns": [
                        "column_1",
                        "column_2",
                    ],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_many",
                    },
                    "transform": {
                        "concat": {
                            "columns": ["column_1", "column_2"],
                            "destination": "column_9",
                            "delimiter": "@",
                        }
                    },
                },
            ],
            "transform": {
                "concat": {
                    "columns": ["id", "level"],
                    "destination": "column_x",
                    "delimiter": "=",
                }
            },
        }

        row = {
            "level": 1,
            "id": "007",
            "code": "be",
            "Child1": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column1": 2, "column2": "cc"},
                {"column1": 3, "column2": "dd"},
            ],
            "Child3": {
                "column_1": 4,
                "column_2": "ee",
            },
        }
        row = Transform.transform(row, nodes)
        assert row == {
            "Child1": [
                {"column_1": 2, "column_2": "aa", "column_3": "2_aa"},
                {"column_1": 3, "column_2": "bb", "column_3": "3_bb"},
            ],
            "Child2": [
                {
                    "column1": 2,
                    "column2": "cc",
                    "column3": "http://2/path",
                    "column4": "http://2/path",
                },
                {
                    "column1": 3,
                    "column2": "dd",
                    "column3": "http://3/path",
                    "column4": "http://3/path",
                },
            ],
            "Child3": {"column_1": 4, "column_2": "ee", "column_9": "4@ee"},
            "code": "be",
            "column_x": "007=1",
            "id": "007",
            "level": 1,
        }

    def test_concat_fields(self):
        nodes = {
            "transform": {
                "concat": {
                    "Child1": {
                        "columns": ["column_1", "column_2"],
                        "delimiter": "_",
                        "destination": "column_3",
                    },
                    "Child2": {
                        "columns": ["column_1"],
                        "destination": "column_3",
                    },
                }
            }
        }

        row = {
            "level": 1,
            "id": "007",
            "code": "be",
            "Child1": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
            "Child2": [
                {"column_1": 2, "column_2": "aa"},
                {"column_1": 3, "column_2": "bb"},
            ],
        }
        row = Transform.concat(row, nodes)
        assert row == {
            "level": 1,
            "id": "007",
            "code": "be",
            "Child1": [
                {"column_1": 2, "column_2": "aa", "column_3": "2_aa"},
                {"column_1": 3, "column_2": "bb", "column_3": "3_bb"},
            ],
            "Child2": [
                {"column_1": 2, "column_2": "aa", "column_3": "2"},
                {"column_1": 3, "column_2": "bb", "column_3": "3"},
            ],
        }
        # - with predefined setting!!!!!!
        # - without predefined setting!!!!!!
        # - with delimiter
        # - withour delimiter
        # - with list order maintained
        # - without destination specified

    def test_get_primary_keys(self):
        primary_keys = [
            {"publisher": {"id": [4]}},
            {"book_language": [{"id": [7]}, {"id": [15]}]},
            [
                [
                    {"author": [{"id": [4]}]},
                    [
                        {"city": {"id": 2}},
                        [{"country": {"id": 2}}, {"continent": {"id": 1}}],
                    ],
                    {"book_author": [{"id": [7]}]},
                ],
                [
                    {"author": [{"id": [5]}]},
                    [
                        {"city": {"id": 1}},
                        [{"country": {"id": 1}}, {"continent": {"id": 1}}],
                    ],
                    {"book_author": [{"id": [9]}]},
                ],
            ],
            [
                {"language": [{"id": [1]}], "book_language": [{"id": [7]}]},
                {"language": [{"id": [6]}], "book_language": [{"id": [15]}]},
            ],
            [{"subject": [{"id": [4]}], "book_subject": [{"id": [7]}]}],
            {"rating": {"id": [7]}},
        ]
        assert Transform.get_primary_keys(primary_keys) == {
            "publisher": {"id": [4]},
            "book_language": {"id": [7, 15]},
            "author": {"id": [4, 5]},
            "city": {"id": [1, 2]},
            "country": {"id": [1, 2]},
            "continent": {"id": [1]},
            "book_author": {"id": [7, 9]},
            "language": {"id": [1, 6]},
            "subject": {"id": [4]},
            "book_subject": {"id": [7]},
            "rating": {"id": [7]},
        }
