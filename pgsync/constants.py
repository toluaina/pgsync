"""PGSync Constants."""
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

TG_OP = [
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

# https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
ELASTICSEARCH_TYPES = [
    "binary",
    "boolean",
    "byte",
    "constant_keyword",
    "date",
    "date_range",
    "double",
    "double_range",
    "flattened",
    "float",
    "float_range",
    "geo_point",
    "geo_shape",
    "half_float",
    "integer",
    "integer_range",
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
    "long_range",
    "nested",
    "null",
    "object",
    "scaled_float",
    "search_as_you_type",
    "shape",
    "short",
    "text",
    "time",
]

# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html
ELASTICSEARCH_MAPPING_PARAMETERS = [
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
    "index",
    "index_options",
    "index_phrases",
    "index_prefixes",
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

ELASTICSEARCH_TAGLINE = "You Know, for Search"

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

# Primary key delimiter
PRIMARY_KEY_DELIMITER = "|"

# Replication slot patterns
LOGICAL_SLOT_PREFIX = re.compile(
    r"table\s\"?(?P<schema>[\w-]+)\"?.\"?(?P<table>[\w-]+)\"?:\s(?P<tg_op>[A-Z]+):"
)
LOGICAL_SLOT_SUFFIX = re.compile(
    '\s(?P<key>"?\w+"?)\[(?P<type>[\w\s]+)\]:(?P<value>[\w\'"\-]+)'
)
