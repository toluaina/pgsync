"""PGSync utils."""
import logging
import os
import sys
import threading
import time
from datetime import timedelta
from urllib.parse import quote_plus

from six import string_types

from .exc import SchemaError
from .settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_SCHEME,
    ELASTICSEARCH_USER,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    REDIS_AUTH,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    SCHEMA,
)

logger = logging.getLogger(__name__)


def progress(
    iteration,
    total,
    prefix='',
    suffix='',
    decimals=1,
    bar_length=50,
):
    """
    Call in a loop to create terminal progress bar.

    Args:
        iteration (int): current iteration
        total (int): total iterations
        prefix (str): prefix string
        suffix (str): suffix string
        decimals (int): positive number of decimals in percent complete
        bar_length (int): character length of bar
    """
    str_format = '{0:.' + str(decimals) + 'f}'
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f'\r{prefix} [{bar}] {percents}% {suffix}')
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def timeit(func):
    def timed(*args, **kwargs):
        since = time.time()
        retval = func(*args, **kwargs)
        until = time.time()
        sys.stdout.write(
            f'{func.__name__} ({args}, {kwargs}) {until-since} secs\n'
        )
        return retval
    return timed


class Timer:
    def __init__(self, message=None):
        self._message = message or ''

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.elapsed = self.end - self.start
        sys.stdout.write(
            f'{self._message} {str(timedelta(seconds=self.elapsed))} '
            f'({self.elapsed:2.2f} sec)\n'
        )


def show_settings(schema=None, params={}):
    """Show configuration."""
    logger.info('\033[4mSettings\033[0m:')
    logger.info(f'{"Schema":<10s}: {schema or SCHEMA}')
    logger.info('-' * 65)
    logger.info('\033[4mPostgres\033[0m:')
    logger.info(
        f'URL: postgresql://{params.get("user", PG_USER)}:*****@'
        f'{params.get("host", PG_HOST)}:'
        f'{params.get("port", PG_PORT)}'
    )
    logger.info('\033[4mElasticsearch\033[0m:')
    if ELASTICSEARCH_USER:
        logger.info(
            f'URL: {ELASTICSEARCH_SCHEME}://{ELASTICSEARCH_USER}:*****@'
            f'{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}'
        )
    else:
        logger.info(
            f'URL: {ELASTICSEARCH_SCHEME}://'
            f'{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}'
        )
    logger.info('\033[4mRedis\033[0m:')
    logger.info(f'URL: redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
    logger.info('-' * 65)


def map_fields(init_dict, map_dict, result_dict=None):
    """Rename keys in a nested dictionary based on mapping."""
    result_dict = result_dict or {}
    if isinstance(init_dict, dict):
        for key, value in init_dict.items():
            if isinstance(value, dict):
                if key in map_dict:
                    value = map_fields(value, map_dict[key])
            elif isinstance(value, list) and value and not isinstance(
                value[0],
                dict,
            ):
                try:
                    value = sorted(value)
                except TypeError:
                    pass
            elif key in map_dict.keys():
                if isinstance(value, list):
                    value = [map_fields(v, map_dict[key]) for v in value]
                elif isinstance(value, (string_types, int, float)):
                    if map_dict[key]:
                        key = str(map_dict[key])
            result_dict[key] = value
    return result_dict


def get_transform(node):
    structure = {}
    for key in node:
        if key == 'children':
            for child in node['children']:
                label = child.get('label', child['table'])
                structure[label] = get_transform(child)
        elif key == 'transform':
            if 'rename' in node['transform']:
                structure = node['transform']['rename']
            # for name in ('rename', 'mapping'):
            #     if name in node['transform']:
            #         structure = node['transform'][name]
    return structure


def transform(key, row, node):
    structure = get_transform(node)
    return map_fields(row, structure)


def get_private_keys(primary_keys):
    """
    Get private keys entry from a nested dict.
    """
    def squash_list(values, _values=None):
        if not _values:
            _values = []
        if isinstance(values, dict):
            if len(values) == 1:
                _values.append(values)
            else:
                for key, value in values.items():
                    _values.extend(
                        squash_list({key: value})
                    )
        elif isinstance(values, list):
            for value in values:
                _values.extend(
                    squash_list(value)
                )
        return _values

    target = []
    for values in squash_list(primary_keys):
        if len(values) > 1:
            for key, value in values.items():
                target.append({key: value})
            continue
        target.append(values)

    target3 = []
    for values in target:
        for key, value in values.items():
            if isinstance(value, dict):
                target3.append({key: value})
            elif isinstance(value, list):
                _value = {}
                for v in value:
                    for _k, _v in v.items():
                        _value.setdefault(_k, [])
                        if isinstance(_v, list):
                            _value[_k].extend(_v)
                        else:
                            _value[_k].append(_v)
                target3.append({key: _value})

    target4 = {}
    for values in target3:
        for key, value in values.items():
            if key not in target4:
                target4[key] = {}
            for k, v in value.items():
                if k not in target4[key]:
                    target4[key][k] = []
                if isinstance(v, list):
                    for _v in v:
                        if _v not in target4[key][k]:
                            target4[key][k].append(_v)
                    target4[key][k] = sorted(target4[key][k])

                else:
                    if v not in target4[key][k]:
                        target4[key][k].append(v)
    return target4


def threaded(fn):
    """Decorator for threaded code execution."""
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper


def get_elasticsearch_url(
    scheme=None,
    user=None,
    host=None,
    password=None,
    port=None,
):
    """
    Return the URL to connect to Elasticsearch.
    """
    scheme = scheme or ELASTICSEARCH_SCHEME
    host = host or ELASTICSEARCH_HOST
    port = port or ELASTICSEARCH_PORT
    user = user or ELASTICSEARCH_USER
    password = password or ELASTICSEARCH_PASSWORD
    if user:
        return f'{scheme}://{user}:{quote_plus(password)}@{host}:{port}'
    logger.debug('Connecting to Elasticsearch without authentication.')
    return f'{scheme}://{host}:{port}'


def get_postgres_url(
    database,
    user=None,
    host=None,
    password=None,
    port=None,
):
    """
    Return the URL to connect to Postgres.
    """
    user = user or PG_USER
    host = host or PG_HOST
    password = password or PG_PASSWORD
    port = port or PG_PORT
    if password:
        return f'postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}'
    logger.debug('Connecting to Postgres without password.')
    return f'postgresql://{user}@{host}:{port}/{database}'


def get_redis_url(host=None, password=None, port=None, db=None):
    """
    Return the URL to connect to Redis.
    """
    host = host or REDIS_HOST
    password = password or REDIS_AUTH
    port = port or REDIS_PORT
    db = db or REDIS_DB
    if password:
        return f'redis://:{quote_plus(password)}@{host}:{port}/{db}'
    logger.debug('Connecting to Redis without password.')
    return f'redis://{host}:{port}/{db}'


def get_config(config=None):
    """
    Return the schema config for PGSync.
    """
    config = config or SCHEMA
    if not config:
        raise SchemaError(
            'Schema config not set\n. '
            'Set env SCHEMA=/path/to/schema.json or '
            'provide args --config /path/to/schema.json'
        )
    if not os.path.exists(config):
        raise IOError(f'Schema config "{config}" not found')
    return config
