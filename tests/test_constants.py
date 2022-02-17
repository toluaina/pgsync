"""Constants tests."""
from pgsync.constants import (
    ELASTICSEARCH_MAPPING_PARAMETERS,
    ELASTICSEARCH_TYPES,
    LOGICAL_SLOT_PREFIX,
    LOGICAL_SLOT_SUFFIX,
)


class TestConstants(object):
    """Constants tests."""

    def test_logical_slot_prefix_insert(self):
        insert = "table public.book: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"
        match = LOGICAL_SLOT_PREFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "INSERT",
        }

        insert = "table public.bo-ok: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"
        match = LOGICAL_SLOT_PREFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "bo-ok",
            "tg_op": "INSERT",
        }

    def test_logical_slot_prefix_update(self):
        update = """table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""
        match = LOGICAL_SLOT_PREFIX.search(update)
        assert match is not None
        assert match.groupdict() == {
            "schema": "public",
            "table": "book",
            "tg_op": "UPDATE",
        }

        update = """table public.book-: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""
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
        insert = "table public.book: INSERT: id[integer]:9 isbn[character varying]:'978-0-924595-91-2a51f2c9f-930d-403c-8687-eeffd0fbfe6f' title[character varying]:'Certainly state million dog son night.' description[character varying]:'Idea prepare how push candidate page. Physical easy sister by let.' copyright[character varying]:null tags[jsonb]:null doc[jsonb]:null point[geometry]:null polygon[geometry]:null publisher_id[integer]:1"
        match = LOGICAL_SLOT_SUFFIX.search(insert)
        assert match is not None
        assert match.groupdict() == {
            "key": "id",
            "type": "integer",
            "value": "9",
        }

    def test_logical_slot_suffix_update(self):
        update = """table public.book: UPDATE: id[integer]:1 isbn[character varying]:'001' title[character varying]:'xyz' description[character varying]:'Stephens Kings It' copyright[character varying]:null tags[jsonb]:'["a", "b", "c"]' doc[jsonb]:'{"a": {"b": {"c": [0, 1, 2, 3, 4]}}, "i": 73, "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}], "bool": true, "lastname": "Judye", "firstname": "Glenda", "generation": {"name": "X"}, "nick_names": ["Beatriz", "Jean", "Carilyn", "Carol-Jean", "Sara-Ann"], "coordinates": {"lat": 21.1, "lon": 32.9}}' point[geometry]:'01010000000000000000001040000000000000F03F' polygon[geometry]:'0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000' publisher_id[integer]:1"""
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

    def test_elasticsearch_types(self):
        assert ELASTICSEARCH_TYPES == sorted(
            [
                "binary",
                "boolean",
                "byte",
                "constant_keyword",
                "date",
                "double",
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
