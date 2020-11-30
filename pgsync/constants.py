"""PGSync Constants."""
import re

# Relationship types
ONE_TO_ONE = 'one_to_one'
ONE_TO_MANY = 'one_to_many'
MANY_TO_ONE = 'many_to_one'

RELATIONSHIP_TYPES = [
    MANY_TO_ONE,
    ONE_TO_MANY,
    ONE_TO_ONE,
]

# Relationship variants
SCALAR = 'scalar'
OBJECT = 'object'

RELATIONSHIP_VARIANTS = [
    SCALAR,
    OBJECT,
]

# Node attributes
NODE_ATTRIBUTES = [
    'children',
    'columns',
    'label',
    'primary_key',
    'relationship',
    'schema',
    'table',
    'transform',
]

# Relationship attributes
RELATIONSHIP_ATTRIBUTES = [
    'through_tables',
    'type',
    'variant',
]

# tg_op
UPDATE = 'UPDATE'
INSERT = 'INSERT'
DELETE = 'DELETE'
TRUNCATE = 'TRUNCATE'

TG_OP = [
    DELETE,
    INSERT,
    TRUNCATE,
    UPDATE,
]

# https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
ELASTICSEARCH_TYPES = [
    'binary',
    'boolean',
    'byte',
    'constant_keyword',
    'date',
    'double',
    'float',
    'half_float',
    'integer',
    'ip',
    'keyword',
    'long',
    'null',
    'scaled_float',
    'short',
    'text',
    'object',
    'nested',
]

CONCAT_TRANSFORM = 'concat'
MAPPING_TRANSFORM = 'mapping'
MOVE_TRANSFORM = 'move'
RENAME_TRANSFORM = 'rename'

TRANSFORM_TYPES = [
    CONCAT_TRANSFORM,
    MAPPING_TRANSFORM,
    MOVE_TRANSFORM,
    RENAME_TRANSFORM,
]

# default postgres schema
SCHEMA = 'public'

BUILTIN_SCHEMAS = ['information_schema']

# Primary key identifier
META = '_meta'

# Logical decoding output plugin
PLUGIN = 'test_decoding'

# Trigger function
TRIGGER_FUNC = 'table_notify'

# Primary key delimiter
PRIMARY_KEY_DELIMITER = '|'

# Replication slot patterns
LOGICAL_SLOT_PREFIX = re.compile(
    r'table\s(?P<schema>\w+).\"?(?P<table>\w+)\"?:\s(?P<tg_op>[A-Z]+):'
)
LOGICAL_SLOT_SUFFIX = re.compile(
    "\s(?P<key>\w+)\[(?P<type>[\w\s]+)\]:(?P<value>[\w\'\"\-]+)"
)
