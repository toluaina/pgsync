"""Tests for the dependency-free defaults in the sink contract."""

import pytest

from pgsync.sink import Sink


class MinimalSink(Sink):
    def bulk(self, index, actions, **kwargs):
        self.bulk_call = (index, list(actions), kwargs)

    def _create_setting(
        self,
        index,
        tree,
        setting=None,
        mapping=None,
        mappings=None,
        routing=None,
    ):
        self.setting_call = (index, tree, setting, mapping, mappings, routing)


class UnimplementedSink(MinimalSink):
    def bulk(self, index, actions, **kwargs):
        return Sink.bulk(self, index, actions, **kwargs)

    def _create_setting(
        self,
        index,
        tree,
        setting=None,
        mapping=None,
        mappings=None,
        routing=None,
    ):
        return Sink._create_setting(
            self, index, tree, setting, mapping, mappings, routing
        )


def test_sink_safe_defaults():
    sink = MinimalSink()
    document = {"id": 1}

    assert sink.prepare_action(document) is document
    assert list(sink._search("index", "book")) == []
    assert sink.refresh(["index"]) is None
    assert sink.close() is None

    with pytest.raises(NotImplementedError):
        sink.delete_by_query("index", ["1"])
    with pytest.raises(NotImplementedError):
        sink.search("index", {"query": {}})
    with pytest.raises(NotImplementedError):
        sink.teardown("index")


def test_abstract_default_errors_are_explicit():
    sink = UnimplementedSink()

    with pytest.raises(NotImplementedError):
        sink.bulk("index", [])
    with pytest.raises(NotImplementedError):
        sink._create_setting("index", object())
