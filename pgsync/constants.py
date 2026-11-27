"""PGSync constants.

This module defines constants used throughout PGSync, organized into:

- Schema configuration: relationship types, variants, node/relationship attributes
- Database operations: trigger operations (INSERT, UPDATE, DELETE, TRUNCATE)
- PostgreSQL: JSONB operators, default schema, logical decoding plugin
- Elasticsearch/OpenSearch: field types and mapping parameters
- Transforms: field transformation types (concat, rename, replace, etc.)
- Internal: materialized view definitions, replication slot patterns
"""

import re

# Relationship types
ONE_TO_ONE = "one_to_one"
ONE_TO_MANY = "one_to_many"

RELATIONSHIP_TYPES = [
    ONE_TO_MANY,
    ONE_TO_ONE,
]

# Relationship variants
SCALAR = "scalar"
OBJECT = "object"

RELATIONSHIP_VARIANTS = [
    SCALAR,
    OBJECT,
]

# Node attributes
NODE_ATTRIBUTES = [
    "base_tables",
    "children",
    "columns",
    "label",
    "primary_key",
    "relationship",
    "schema",
    "table",
    "transform",
]

# Relationship attributes
RELATIONSHIP_ATTRIBUTES = [
    "foreign_key",
    "through_tables",
    "type",
    "variant",
]

# Relationship foreign keys
RELATIONSHIP_FOREIGN_KEYS = [
    "child",
    "parent",
]


# tg_op
UPDATE = "UPDATE"
INSERT = "INSERT"
DELETE = "DELETE"
TRUNCATE = "TRUNCATE"

TG_OPS = [
    DELETE,
    INSERT,
    TRUNCATE,
    UPDATE,
]

# https://www.postgresql.org/docs/current/functions-json.html
JSONB_OPERATORS = [
    "->",
    "->>",
    "#>",
    "#>>",
]

# Field (mapping) types for Elasticsearch and OpenSearch.
# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
# https://docs.opensearch.org/latest/field-types/supported-field-types/index/
ELASTICSEARCH_TYPES = [
    "aggregate_metric_double",
    "alias",
    "binary",
    "boolean",
    "byte",
    "completion",
    "constant_keyword",
    "counted_keyword",
    "date",
    "date_nanos",
    "date_range",
    "dense_vector",
    "double",
    "double_range",
    "flat_object",
    "flattened",
    "float",
    "float_range",
    "geo_point",
    "geo_shape",
    "half_float",
    "histogram",
    "integer",
    "integer_range",
    "ip",
    "ip_range",
    "join",
    "keyword",
    "knn_vector",
    "long",
    "long_range",
    "match_only_text",
    "nested",
    "object",
    "percolator",
    "rank_feature",
    "rank_features",
    "scaled_float",
    "search_as_you_type",
    "semantic_text",
    "shape",
    "short",
    "sparse_vector",
    "text",
    "token_count",
    "unsigned_long",
    "version",
    "wildcard",
    "xy_point",
    "xy_shape",
]

# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html
ELASTICSEARCH_MAPPING_PARAMETERS = [
    "analyzer",
    "boost",
    "coerce",
    "copy_to",
    "default_metric",
    "dimension",
    "dims",
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
    "index",
    "index_options",
    "index_phrases",
    "index_prefixes",
    "meta",
    "metrics",
    "normalizer",
    "norms",
    "null_value",
    "path",
    "position_increment_gap",
    "properties",
    "relations",
    "search_analyzer",
    "similarity",
    "store",
    "term_vector",
]

CONCAT_TRANSFORM = "concat"
MAPPING_TRANSFORM = "mapping"
MOVE_TRANSFORM = "move"
RENAME_TRANSFORM = "rename"
REPLACE_TRANSFORM = "replace"

TRANSFORM_TYPES = [
    CONCAT_TRANSFORM,
    MAPPING_TRANSFORM,
    MOVE_TRANSFORM,
    RENAME_TRANSFORM,
    REPLACE_TRANSFORM,
]

# default postgres schema
DEFAULT_SCHEMA = "public"

BUILTIN_SCHEMAS = ["information_schema"]

# Primary key identifier
META = "_meta"

# Logical decoding output plugin
PLUGIN = "test_decoding"

# Trigger function
TRIGGER_FUNC = "table_notify"

# Views
# added underscore to reduce chance of collisions
MATERIALIZED_VIEW = "_view"

MATERIALIZED_VIEW_COLUMNS = [
    "foreign_keys",
    "indices",
    "primary_keys",
    "table_name",
    "columns",
]

# Primary key delimiter
PRIMARY_KEY_DELIMITER = "|"

# Replication slot patterns
LOGICAL_SLOT_PREFIX = re.compile(
    r"^table\s+"
    r'"?(?P<schema>[^"]+?)"?\.'  # schema up to the dot
    r'"?(?P<table>[^"]+?)"?'  # table up to the colon
    r"\s*:\s*(?P<tg_op>[A-Z]+):"
)
LOGICAL_SLOT_SUFFIX = re.compile(
    r"\s"
    r'(?P<key>"(?:[^"]|"")+"|[\w$-]+)'  # "Weird Key" or my_col or my-col or my$col
    r"\[(?P<type>[^\]]+)\]"  # anything until ]
    r":"
    r"(?P<value>"
    r"null|true|false|NaN|Infinity|-Infinity|"
    r'"(?:[^"]|"")*"|'  # double-quoted, supports "" escape
    r"'(?:[^']|'')*'|"  # single-quoted, supports '' escape
    r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|"  # numbers: -1, 3.14, 9e-3, 9E+2
    r"[\w$-]+"  # bare tokens like uuid-ish or identifiers
    r")"
)
