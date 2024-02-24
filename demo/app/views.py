"""PGSync Demo views."""

import logging
from copy import deepcopy

from aiohttp import web
from app.settings import (
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_TIMEOUT,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_VERIFY_CERTS,
    MAX_RESULTS,
    VECTOR_SEARCH,
)
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.query import Bool, Match, ScriptScore

from .utils import get_embedding

logger = logging.getLogger(__name__)

HIGHLIGHT = True
META = "_meta"
PRE_HIGHLIGHT_TAG = "<u><mark><b>"
POST_HIGHLIGHT_TAG = "</b></mark></u>"


class DictQuery(dict):
    def get(self, path, default=None):
        value = None
        for key in path.split("/"):
            if value:
                if isinstance(value, list):
                    value = [v.get(key, default) if v else None for v in value]
                else:
                    value = value.get(key, default)
            else:
                value = dict.get(self, key, default)
            if not value:
                break
        return value

    def set(self, path, value):
        for key in path.split("/"):
            self.nested_update(self, key, value)

    def nested_update(self, obj, key, value):
        old_value = value.replace(PRE_HIGHLIGHT_TAG, "").replace(
            POST_HIGHLIGHT_TAG, ""
        )

        for k, v in obj.items():
            if key == k:
                if isinstance(obj[k], list):
                    for _k in obj[k]:
                        if _k == old_value:
                            obj[k] = value
                else:
                    if obj[k] == old_value:
                        obj[k] = value
            elif isinstance(v, dict):
                self.nested_update(v, key, value)
            elif isinstance(v, list):
                for o in v:
                    if isinstance(o, dict):
                        self.nested_update(o, key, value)


class TypeAheadHandler(web.View):
    """TypeAheadHandler view."""

    def _build_queries(self, key, value, search_param, parents=[]):
        queries = []
        if "fields" in value:
            param = f"{key}__ngram"
            if parents:
                param = ""
                for parent in parents:
                    param += f"{parent}__"
                param += f"{key}__ngram"
            query = Match(**{param: {"query": search_param}})
            queries.append(query)
            for parent in parents:
                parents.remove(parent)

        if "properties" in value:
            parents.append(key)
            for k, v in value["properties"].items():
                queries.extend(
                    self._build_queries(
                        k, v, search_param, parents=deepcopy(parents)
                    )
                )
            for parent in parents:
                parents.remove(parent)
        return queries

    def build_queries(self, mapping, search_param):
        """Build matching query by search_param from mapping."""
        queries = []
        for key, value in mapping.items():
            if key == META:
                continue
            if isinstance(value, dict):
                queries.extend(self._build_queries(key, value, search_param))
        return queries

    def _build_highlight(self, key, value, parents=[]):
        highlights = []
        if "fields" in value:
            param = f"{key}.ngram"
            if parents:
                param = ""
                for parent in parents:
                    param += f"{parent}."
                param += f"{key}.ngram"
            highlights.append(param)
            for parent in parents:
                parents.remove(parent)

        if "properties" in value:
            parents.append(key)
            for k, v in value["properties"].items():
                highlights.extend(
                    self._build_highlight(k, v, parents=deepcopy(parents))
                )
            for parent in parents:
                parents.remove(parent)
        return highlights

    def build_highlight(self, mapping):
        """Build matching highlight from mapping."""
        highlights = []
        for key, value in mapping.items():
            if key == META:
                continue
            if isinstance(value, dict):
                highlights.extend(self._build_highlight(key, value))
        return highlights

    def queries(self, mapping, search_param):
        """Return matching query by search_param."""
        should = self.build_queries(
            mapping[ELASTICSEARCH_INDEX]["mappings"]["properties"],
            search_param.lower(),
        )
        return Bool(
            minimum_should_match=1,
            should=should,
        )

    async def get(self):
        """Get the results from Elasticsearch."""
        qs: str = self.request.query.get("q")
        if not qs:
            return web.json_response([])

        es: Elasticsearch = Elasticsearch(
            hosts=ELASTICSEARCH_URL,
            timeout=ELASTICSEARCH_TIMEOUT,
            verify_certs=ELASTICSEARCH_VERIFY_CERTS,
        )
        mapping: dict = es.indices.get_mapping(index=ELASTICSEARCH_INDEX)
        search: Search = Search(index=ELASTICSEARCH_INDEX, using=es)
        search = search.highlight_options(
            pre_tags=[PRE_HIGHLIGHT_TAG],
            post_tags=[POST_HIGHLIGHT_TAG],
        )

        if VECTOR_SEARCH:
            search = search.query(
                ScriptScore(
                    query=Q("match_all"),
                    script={
                        "source": "cosineSimilarity(params.query_vector, \u0027embedding\u0027) + 1.0",
                        "params": {"query_vector": get_embedding(qs)},
                    },
                )
            )
        else:
            query = self.queries(mapping, qs)
            search = search.query(query)

        highlights = self.build_highlight(
            mapping[ELASTICSEARCH_INDEX]["mappings"]["properties"]
        )

        for highlight in highlights:
            search = search.highlight(highlight, type="plain")

        search = search.extra(
            from_=0,
            size=MAX_RESULTS,
            _source={"excludes": ["embedding"]},
        )

        values = []
        for hit in search.execute():
            hit._d_.pop(META, None)
            if HIGHLIGHT and hasattr(hit.meta, "highlight"):
                highlight = hit.meta.highlight
                query = DictQuery(hit._d_)
                for key in highlight:
                    path = key.split(".")[:-1]
                    value = highlight[key][0]
                    query.set("/".join(path), value)
                values.append(query)
            else:
                values.append(hit._d_)
        return web.json_response(values)


class TypeAheadView(web.View):
    """TypeAheadView view."""

    async def get(self):
        return web.FileResponse("index.html")
