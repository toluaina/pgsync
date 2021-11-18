"""Test helper methods."""


def noop():
    pass


def get_docs(results, _id=None):
    """Sorted by _id."""
    sources = []
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


def search(es, index, body=None, _id=None):
    body = body or {
        "size": 10000,
        "query": {"match_all": {}},
    }
    results = es.search(index, body)
    if results:
        return get_docs(results, _id=_id)


def assert_resync_empty(sync, node, txmin=None):
    # re-sync and ensure we are not syncing more data
    txmin = txmin or sync.txid_current
    docs = [doc for doc in sync.sync(node, txmin=txmin)]
    assert docs == []
