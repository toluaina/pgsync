"""Constants tests."""

import pytest

from pgsync.constants import (
    ELASTICSEARCH_MAPPING_PARAMETERS,
    ELASTICSEARCH_TYPES,
    LOGICAL_SLOT_PREFIX,
    LOGICAL_SLOT_SUFFIX,
)


class TestConstants(object):
    """Constants tests."""

    def test_row_complex_update_prefix_and_suffix(self):
        row = """
        table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'It' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' publisher_id[integer]:1 publish_date[timestamp without time zone]:'1980-01-01 00:00:00' quad[double precision]:2e+58
        """

        m = LOGICAL_SLOT_PREFIX.search(row.strip())
        assert m is not None
        assert m.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "UPDATE",
        }

        fields = [
            mm.groupdict() for mm in LOGICAL_SLOT_SUFFIX.finditer(row.strip())
        ]
        # sanity: first field
        assert fields[0] == {"key": "id", "type": "integer", "value": "1"}

        # spot-check a few tricky ones
        assert {
            "key": "copyright",
            "type": "character varying",
            "value": "null",
        } in fields
        assert {
            "key": "tags",
            "type": "jsonb",
            "value": '\'["a", "b", "c"]\'',
        } in fields
        assert {
            "key": "publisher_id",
            "type": "integer",
            "value": "1",
        } in fields
        assert {
            "key": "publish_date",
            "type": "timestamp without time zone",
            "value": "'1980-01-01 00:00:00'",
        } in fields
        assert {
            "key": "quad",
            "type": "double precision",
            "value": "2e+58",
        } in fields

    def test_logical_slot_prefix_insert(self):
        insert = "table public.book: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"  # noqa E501
        match = LOGICAL_SLOT_PREFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "INSERT",
        }

        insert = "table public.bo-ok: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"  # noqa E501
        match = LOGICAL_SLOT_PREFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "bo-ok",
            "tg_op": "INSERT",
        }

    def test_logical_slot_prefix_update(self):
        update = """table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""  # noqa E501
        match = LOGICAL_SLOT_PREFIX.search(update)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "UPDATE",
        }

        update = """table public.book-: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""  # noqa E501
        match = LOGICAL_SLOT_PREFIX.search(update)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book-",
            "tg_op": "UPDATE",
        }

    def test_logical_slot_prefix_delete(self):
        delete = "table public.book: DELETE: id[integer]:12"
        match = LOGICAL_SLOT_PREFIX.search(delete)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "DELETE",
        }

    def test_logical_slot_suffix_insert(self):
        insert = "table public.book: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"  # noqa E501
        match = LOGICAL_SLOT_SUFFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "key": "id",
            "type": "integer",
            "value": "9",
        }

    def test_logical_slot_suffix_update(self):
        update = """table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""  # noqa E501
        match = LOGICAL_SLOT_SUFFIX.search(update)
        assert match is not None
        assert match.groupdict() == {
            "key": "id",
            "type": "integer",
            "value": "1",
        }

    def test_logical_slot_suffix_delete(self):
        delete = "table public.book: DELETE: id[integer]:12"
        match = LOGICAL_SLOT_SUFFIX.search(delete)
        assert match is not None
        assert match.groupdict() == {
            "key": "id",
            "type": "integer",
            "value": "12",
        }

    @pytest.mark.parametrize(
        "line, expected",
        [
            (
                "table public.book: INSERT: id[integer]:9",
                {"schema": "public", "table": "book", "tg_op": "INSERT"},
            ),
            (
                'table public."cars": INSERT: brand[character varying]:' "'a'",
                {"schema": "public", "table": "cars", "tg_op": "INSERT"},
            ),
            (
                "table public.bo-ok: UPDATE: id[integer]:1",
                {"schema": "public", "table": "bo-ok", "tg_op": "UPDATE"},
            ),
            (
                "table public.book-: DELETE: id[integer]:12",
                {"schema": "public", "table": "book-", "tg_op": "DELETE"},
            ),
            # Quoted schema/table
            (
                'table "public"."book": INSERT: id[integer]:9',
                {"schema": "public", "table": "book", "tg_op": "INSERT"},
            ),
            # Schema with dash (works only if your schema group allows "-")
            (
                'table "my-schema"."book": INSERT: id[integer]:9',
                {"schema": "my-schema", "table": "book", "tg_op": "INSERT"},
            ),
            pytest.param(
                'table public."cars$xxx": INSERT: brand[character varying]:'
                "'a' model[character varying]:'b' year[integer]:1",
                {"schema": "public", "table": "cars$xxx", "tg_op": "INSERT"},
            ),
            # Unquoted table with dollar (Postgres allows $ unquoted too)
            pytest.param(
                "table public.cars$xxx: INSERT: id[integer]:1",
                {"schema": "public", "table": "cars$xxx", "tg_op": "INSERT"},
            ),
        ],
    )
    def test_logical_slot_prefix_variants(self, line, expected):
        match = LOGICAL_SLOT_PREFIX.search(line)
        assert match is not None
        assert match.groupdict() == expected

    def test_logical_slot_suffix_first_field_insert(self):
        insert = (
            "table public.book: INSERT: id[integer]:9 "
            "isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' "
            "title[character varying]:'Certainly state million dog son night.' "
            "copyright[character varying]:null"
        )
        match = LOGICAL_SLOT_SUFFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "key": "id",
            "type": "integer",
            "value": "9",
        }

    def test_logical_slot_suffix_find_multiple_fields(self):
        line = (
            "table public.book: INSERT: "
            "id[integer]:9 "
            "title[character varying]:'Hello world' "
            "is_active[boolean]:true "
            "copyright[character varying]:null"
        )
        matches = [m.groupdict() for m in LOGICAL_SLOT_SUFFIX.finditer(line)]
        assert matches == [
            {"key": "id", "type": "integer", "value": "9"},
            {
                "key": "title",
                "type": "character varying",
                "value": "'Hello world'",
            },
            {"key": "is_active", "type": "boolean", "value": "true"},
            {"key": "copyright", "type": "character varying", "value": "null"},
        ]

    def test_logical_slot_suffix_key_can_be_quoted_simple(self):
        line = 'table public.book: INSERT: "id"[integer]:9'
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None
        assert match.groupdict() == {
            "key": '"id"',
            "type": "integer",
            "value": "9",
        }

    def test_logical_slot_suffix_empty_strings(self):
        line = (
            "table public.book: INSERT: "
            "empty_single[character varying]:'' "
            'empty_double[character varying]:""'
        )
        matches = [m.groupdict() for m in LOGICAL_SLOT_SUFFIX.finditer(line)]
        assert matches == [
            {
                "key": "empty_single",
                "type": "character varying",
                "value": "''",
            },
            {
                "key": "empty_double",
                "type": "character varying",
                "value": '""',
            },
        ]

    def test_logical_slot_suffix_json_in_single_quotes(self):
        line = (
            "table public.book: UPDATE: "
            'tags[jsonb]:\'["a", "b", "c"]\' '
            'doc[jsonb]:\'{"a": {"b": 1}, "ok": true}\''
        )
        matches = [m.groupdict() for m in LOGICAL_SLOT_SUFFIX.finditer(line)]
        assert matches == [
            {"key": "tags", "type": "jsonb", "value": '\'["a", "b", "c"]\''},
            {
                "key": "doc",
                "type": "jsonb",
                "value": '\'{"a": {"b": 1}, "ok": true}\'',
            },
        ]

    def test_logical_slot_suffix_type_with_spaces(self):
        line = (
            "table public.book: INSERT: "
            "created_at[timestamp without time zone]:'2025-12-17 10:11:12'"
        )
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None
        assert match.groupdict() == {
            "key": "created_at",
            "type": "timestamp without time zone",
            "value": "'2025-12-17 10:11:12'",
        }

    def test_logical_slot_suffix_scientific_lower_e(self):
        line = "table public.book: INSERT: ratio[double precision]:9e-3"
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None
        assert match.groupdict() == {
            "key": "ratio",
            "type": "double precision",
            "value": "9e-3",
        }

    def test_logical_slot_suffix_scientific_upper_E(self):
        line = "table public.book: INSERT: ratio[double precision]:9E-3"
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None

    def test_logical_slot_suffix_negative_int(self):
        line = "table public.book: INSERT: delta[integer]:-1"
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None

    def test_logical_slot_suffix_float(self):
        line = "table public.book: INSERT: price[numeric]:3.14"
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None

    def test_logical_slot_suffix_type_with_parens(self):
        line = "table public.book: INSERT: price[numeric(10,2)]:123.45"
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None

    def test_logical_slot_suffix_quoted_key_with_space(self):
        line = 'table public.book: INSERT: "Weird Key"[integer]:1'
        match = LOGICAL_SLOT_SUFFIX.search(line)
        assert match is not None

    def test_logical_slot_suffix_unquoted_key_with_dash_or_dollar(self):
        line = "table public.book: INSERT: my-col[integer]:1 my$col[integer]:2"
        matches = list(LOGICAL_SLOT_SUFFIX.finditer(line))
        assert len(matches) == 2

    def test_elasticsearch_types(self):
        assert ELASTICSEARCH_TYPES == sorted(
            [
                "binary",
                "boolean",
                "byte",
                "completion",
                "constant_keyword",
                "date",
                "double",
                "dense_vector",
                "float",
                "geo_point",
                "geo_shape",
                "half_float",
                "integer",
                "interval_day",
                "interval_day_to_hour",
                "interval_day_to_minute",
                "interval_day_to_second",
                "interval_hour",
                "interval_hour_to_minute",
                "interval_hour_to_second",
                "interval_minute",
                "interval_minute_to_second",
                "interval_month",
                "interval_second",
                "interval_year",
                "interval_year_to_month",
                "ip",
                "keyword",
                "knn_vector",
                "long",
                "nested",
                "null",
                "object",
                "search_as_you_type",
                "scaled_float",
                "shape",
                "short",
                "text",
                "time",
                "integer_range",
                "float_range",
                "long_range",
                "double_range",
                "date_range",
                "flattened",
            ]
        )

    def test_elasticsearch_mapping_parameters(self):
        assert ELASTICSEARCH_MAPPING_PARAMETERS == sorted(
            [
                "analyzer",
                "boost",
                "coerce",
                "copy_to",
                "doc_values",
                "dimension",
                "dynamic",
                "eager_global_ordinals",
                "enabled",
                "fielddata",
                "fielddata_frequency_filter",
                "fields",
                "format",
                "ignore_above",
                "ignore_malformed",
                "index_options",
                "index_phrases",
                "index_prefixes",
                "index",
                "meta",
                "normalizer",
                "norms",
                "null_value",
                "position_increment_gap",
                "properties",
                "search_analyzer",
                "similarity",
                "store",
                "term_vector",
            ]
        )
