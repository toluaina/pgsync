"""Transform tests."""

import pytest

from pgsync.constants import (
    CONCAT_TRANSFORM,
    RENAME_TRANSFORM,
    REPLACE_TRANSFORM,
)
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

    def test_replace_fields(self):
        """Test basic replace transform on string fields."""
        nodes = {
            "transform": {
                "replace": {
                    "code": {
                        "-": "=",
                        "_": " ",
                    }
                }
            }
        }
        row = {
            "id": 1,
            "code": "ABC-DEF_GHI",
            "name": "unchanged",
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "code": "ABC=DEF GHI",
            "name": "unchanged",
        }

    def test_replace_multiple_fields(self):
        """Test replace transform on multiple fields."""
        nodes = {
            "transform": {
                "replace": {
                    "code": {"-": "="},
                    "name": {"@": " at "},
                }
            }
        }
        row = {
            "id": 1,
            "code": "A-B-C",
            "name": "user@example.com",
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "code": "A=B=C",
            "name": "user at example.com",
        }

    def test_replace_nested_dict(self):
        """Test replace transform on nested dictionary fields."""
        nodes = {
            "transform": {
                "replace": {
                    "Child1": {
                        "code": {"-": "_"},
                    }
                }
            }
        }
        row = {
            "id": 1,
            "Child1": {
                "code": "A-B-C",
                "name": "unchanged",
            },
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "Child1": {
                "code": "A_B_C",
                "name": "unchanged",
            },
        }

    def test_replace_list_of_strings(self):
        """Test replace transform on list of strings."""
        nodes = {
            "transform": {
                "replace": {
                    "codes": {"-": "="},
                }
            }
        }
        row = {
            "id": 1,
            "codes": ["A-B", "C-D", "E-F"],
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "codes": ["A=B", "C=D", "E=F"],
        }

    def test_replace_list_of_dicts(self):
        """Test replace transform on list of dictionaries."""
        nodes = {
            "transform": {
                "replace": {
                    "Child1": {
                        "code": {"-": "="},
                    }
                }
            }
        }
        row = {
            "id": 1,
            "Child1": [
                {"code": "A-B", "name": "first"},
                {"code": "C-D", "name": "second"},
            ],
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "Child1": [
                {"code": "A=B", "name": "first"},
                {"code": "C=D", "name": "second"},
            ],
        }

    def test_replace_with_child_nodes(self):
        """Test replace transform defined on child nodes in schema."""
        nodes = {
            "table": "parent",
            "columns": ["id", "code"],
            "children": [
                {
                    "table": "child_1",
                    "columns": ["code", "name"],
                    "label": "Child1",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {
                        "replace": {
                            "code": {"-": "_"},
                        }
                    },
                },
            ],
            "transform": {
                "replace": {
                    "code": {"@": "#"},
                }
            },
        }
        row = {
            "id": 1,
            "code": "A@B@C",
            "Child1": {"code": "X-Y-Z", "name": "test"},
        }
        row = Transform.transform(row, nodes)
        assert row == {
            "id": 1,
            "code": "A#B#C",
            "Child1": {"code": "X_Y_Z", "name": "test"},
        }

    def test_get_replace_transform(self):
        """Test getting replace transform from nodes."""
        nodes = {
            "table": "parent",
            "columns": ["id", "code"],
            "children": [
                {
                    "table": "child_1",
                    "columns": ["code"],
                    "label": "Child1",
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {
                        "replace": {
                            "code": {"-": "_"},
                        }
                    },
                },
            ],
            "transform": {
                "replace": {
                    "code": {"@": "#"},
                }
            },
        }
        transform_node = Transform.get(nodes, REPLACE_TRANSFORM)
        assert transform_node == {
            "code": {"@": "#"},
            "Child1": {"code": {"-": "_"}},
        }

    def test_replace_no_match(self):
        """Test replace transform when no match is found."""
        nodes = {
            "transform": {
                "replace": {
                    "code": {"-": "="},
                }
            }
        }
        row = {
            "id": 1,
            "code": "ABCDEF",
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "code": "ABCDEF",
        }

    def test_replace_empty_string(self):
        """Test replace transform with empty replacement string."""
        nodes = {
            "transform": {
                "replace": {
                    "code": {"-": ""},
                }
            }
        }
        row = {
            "id": 1,
            "code": "A-B-C",
        }
        row = Transform.replace(row, nodes)
        assert row == {
            "id": 1,
            "code": "ABC",
        }

    def test_rename_empty_data(self):
        """Test rename transform with empty data dict."""
        nodes = {
            "transform": {
                "rename": {"id": "my_id"},
            }
        }
        row = {}
        row = Transform.rename(row, nodes)
        assert row == {}

    def test_rename_nested_dict(self):
        """Test rename transform on nested dictionary."""
        nodes = {
            "transform": {
                "rename": {
                    "Child1": {
                        "inner_id": "id",
                        "inner_name": "name",
                    }
                }
            }
        }
        row = {
            "id": 1,
            "Child1": {
                "inner_id": 100,
                "inner_name": "test",
                "other": "unchanged",
            },
        }
        row = Transform.rename(row, nodes)
        assert row == {
            "id": 1,
            "Child1": {
                "id": 100,
                "name": "test",
                "other": "unchanged",
            },
        }

    def test_rename_with_none_value(self):
        """Test rename transform when field has None value."""
        nodes = {
            "transform": {
                "rename": {
                    "code": "my_code",
                }
            }
        }
        row = {
            "id": 1,
            "code": None,
        }
        row = Transform.rename(row, nodes)
        assert row == {
            "id": 1,
            "my_code": None,
        }

    def test_concat_empty_columns(self):
        """Test concat transform with missing columns in data."""
        nodes = {
            "transform": {
                "concat": {
                    "columns": ["first", "second", "third"],
                    "destination": "combined",
                    "delimiter": "-",
                }
            }
        }
        row = {
            "id": 1,
            "first": "A",
        }
        row = Transform.concat(row, nodes)
        # Missing columns should use the key as literal value
        assert row == {
            "id": 1,
            "first": "A",
            "combined": "A-second-third",
        }

    def test_concat_with_none_values(self):
        """Test concat transform with None values filtered out."""
        nodes = {
            "transform": {
                "concat": {
                    "columns": ["first", "second", "third"],
                    "destination": "combined",
                    "delimiter": "-",
                }
            }
        }
        row = {
            "first": "A",
            "second": None,
            "third": "C",
        }
        row = Transform.concat(row, nodes)
        # None values should be filtered out
        assert row == {
            "first": "A",
            "second": None,
            "third": "C",
            "combined": "A-C",
        }

    def test_concat_no_delimiter(self):
        """Test concat transform without delimiter (default empty)."""
        nodes = {
            "transform": {
                "concat": {
                    "columns": ["first", "second"],
                    "destination": "combined",
                }
            }
        }
        row = {
            "first": "A",
            "second": "B",
        }
        row = Transform.concat(row, nodes)
        assert row == {
            "first": "A",
            "second": "B",
            "combined": "AB",
        }

    def test_transform_order_of_operations(self):
        """Test that transform applies replace, then rename, then concat."""
        nodes = {
            "transform": {
                "replace": {
                    "code": {"-": "_"},
                },
                "rename": {
                    "code": "product_code",
                },
                "concat": {
                    "columns": ["id", "product_code"],
                    "destination": "full_code",
                    "delimiter": ":",
                },
            }
        }
        row = {
            "id": "001",
            "code": "A-B-C",
        }
        row = Transform.transform(row, nodes)
        # Replace happens first: code becomes "A_B_C"
        # Rename happens second: code becomes product_code
        # Concat happens third: uses renamed key
        assert row == {
            "id": "001",
            "product_code": "A_B_C",
            "full_code": "001:A_B_C",
        }

    def test_get_empty_nodes(self):
        """Test get transform with empty nodes."""
        nodes = {}
        transform_node = Transform.get(nodes, RENAME_TRANSFORM)
        assert transform_node == {}

    def test_get_no_transform_key(self):
        """Test get transform when nodes have no transform key."""
        nodes = {
            "table": "test",
            "columns": ["id", "name"],
        }
        transform_node = Transform.get(nodes, RENAME_TRANSFORM)
        assert transform_node == {}

    def test_get_no_children(self):
        """Test get transform with nodes that have no children."""
        nodes = {
            "table": "test",
            "columns": ["id", "name"],
            "transform": {
                "rename": {"id": "my_id"},
            },
        }
        transform_node = Transform.get(nodes, RENAME_TRANSFORM)
        assert transform_node == {"id": "my_id"}

    def test_get_child_without_label(self):
        """Test get transform uses table name when label is missing."""
        nodes = {
            "table": "parent",
            "columns": ["id"],
            "children": [
                {
                    "table": "child_table",
                    "columns": ["name"],
                    "relationship": {
                        "variant": "object",
                        "type": "one_to_one",
                    },
                    "transform": {
                        "rename": {"name": "child_name"},
                    },
                },
            ],
        }
        transform_node = Transform.get(nodes, RENAME_TRANSFORM)
        assert transform_node == {
            "child_table": {"name": "child_name"},
        }

    def test_get_primary_keys_empty(self):
        """Test get_primary_keys with empty input."""
        assert Transform.get_primary_keys([]) == {}

    def test_get_primary_keys_simple(self):
        """Test get_primary_keys with simple input."""
        primary_keys = [{"user": {"id": [1, 2, 3]}}]
        assert Transform.get_primary_keys(primary_keys) == {
            "user": {"id": [1, 2, 3]},
        }

    def test_apply_replacements_non_string(self):
        """Test _apply_replacements with non-string value."""
        replacements = {"-": "_"}
        # Integer should be returned unchanged
        assert Transform._apply_replacements(123, replacements) == 123
        # None should be returned unchanged
        assert Transform._apply_replacements(None, replacements) is None

    def test_replace_non_dict_data(self):
        """Test _replace with non-dict data returns empty dict."""
        nodes = {"code": {"-": "_"}}
        result = Transform._replace("not a dict", nodes)
        assert result == {}
        result = Transform._replace(None, nodes)
        assert result == {}
        result = Transform._replace([1, 2, 3], nodes)
        assert result == {}
