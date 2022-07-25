"""Test helper methods."""
from typing import Optional

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
    es, index: str, body: Optional[str] = None, _id: Optional[str] = None
):
    body: dict = body or {
        "size": 10000,
        "query": {"match_all": {}},
    }
    results = es.search(index, body)
    if results:
        return get_docs(results, _id=_id)


def assert_resync_empty(sync, node: Node, txmin: Optional[int] = None) -> None:
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
