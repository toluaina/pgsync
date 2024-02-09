"""Test helper methods."""

import os
import typing as t
from contextlib import contextmanager

from pgsync.node import Node


def noop():
    pass


def get_docs(results, _id=None):
    """Sorted by _id."""
    sources: list = []
    for doc in results["hits"]["hits"]:
        sources.append(doc)
    sources = sorted(sources, key=lambda k: k["_id"])
    docs = []
    for doc in sources:
        if _id is not None:
            if doc["_id"] == _id:
                docs.append(doc["_source"])
                break
            continue
        docs.append(doc["_source"])
    return docs


def search(
    es, index: str, body: t.Optional[str] = None, _id: t.Optional[str] = None
):
    body: dict = body or {
        "size": 10000,
        "query": {"match_all": {}},
    }
    results = es.search(index, body)
    if results:
        return get_docs(results, _id=_id)


def assert_resync_empty(
    sync, node: Node, txmin: t.Optional[int] = None
) -> None:
    # re-sync and ensure we are not syncing more data
    txmin = txmin or sync.txid_current
    docs = [doc for doc in sync.sync(node, txmin=txmin)]
    assert docs == []


def sort_list(data: dict) -> dict:
    result: dict = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result[key] = sort_list(value)
        elif (
            isinstance(value, list)
            and value
            and not isinstance(
                value[0],
                dict,
            )
        ):
            result[key] = sorted(value)
        else:
            result[key] = value
    return result


@contextmanager
def override_env_var(**kwargs):
    """Set the given value of the given environment variable or
    unset if value is None.
    """
    original_values: dict = {}
    envs_to_delete: list = []
    for env_name, env_value in kwargs.items():
        try:
            original_values[env_name] = os.environ[env_name]
            if env_value is None:
                del os.environ[env_name]
        except KeyError:
            # Env var did not previouslt exist.
            # If we are not setting it, we need to remove it.
            if env_value is not None:
                envs_to_delete.append(env_name)

        if env_value is not None:
            os.environ[env_name] = env_value

    yield

    for env_name in envs_to_delete:
        del os.environ[env_name]
    for env_name, original_env_value in original_values.items():
        os.environ[env_name] = original_env_value
