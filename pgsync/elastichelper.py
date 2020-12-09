"""PGSync Elasticsearch helper."""
import logging
from collections import defaultdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.query import Bool

from .constants import ELASTICSEARCH_TYPES, META
from .node import traverse_post_order
from .settings import (
    ELASTICSEARCH_CHUNK_SIZE,
    ELASTICSEARCH_MAX_CHUNK_BYTES,
    ELASTICSEARCH_QUEUE_SIZE,
    ELASTICSEARCH_THREAD_COUNT,
    ELASTICSEARCH_TIMEOUT,
    ELASTICSEARCH_VERIFY_CERTS,
)
from .utils import get_elasticsearch_url

logger = logging.getLogger(__name__)


class ElasticHelper(object):
    """Elasticsearch Helper."""

    def __init__(self):
        """
        Return an Elasticsearch client.

        The default connection parameters are:
        host = 'localhost', port = 9200
        """
        url = get_elasticsearch_url()
        self.__es = Elasticsearch(
            hosts=[url],
            timeout=ELASTICSEARCH_TIMEOUT,
            verify_certs=ELASTICSEARCH_VERIFY_CERTS,
        )

    def teardown(self, index):
        """
        Teardown the Elasticsearch index.

        :arg index: index (or list of indices) to read documents from
        """
        try:
            logger.debug(f'Deleting index {index}')
            self.__es.indices.delete(index=index, ignore=[400, 404])
        except Exception as e:
            logger.exception(f'Exception {e}')
            raise

    def bulk(
        self,
        index,
        docs,
        chunk_size=None,
        max_chunk_bytes=None,
        queue_size=None,
        thread_count=None,
    ):
        """Bulk index, update, delete docs to Elasticsearch."""
        chunk_size = chunk_size or ELASTICSEARCH_CHUNK_SIZE
        max_chunk_bytes = max_chunk_bytes or ELASTICSEARCH_MAX_CHUNK_BYTES
        thread_count = thread_count or ELASTICSEARCH_THREAD_COUNT
        queue_size = queue_size or ELASTICSEARCH_QUEUE_SIZE

        for _ in parallel_bulk(
            self.__es,
            docs,
            index=index,
            thread_count=thread_count,
            chunk_size=chunk_size,
            max_chunk_bytes=max_chunk_bytes,
            queue_size=queue_size,
            refresh=False,
        ):
            pass

    def refresh(self, indices):
        """Refresh the Elasticsearch index."""
        self.__es.indices.refresh(index=indices)

    def _search(self, index, table, fields):
        """
        Search private area for matching docs in Elasticsearch.

        only returns the _id of the matching document.

        fields = {
            'id': [1, 2],
            'uid': ['a002', 'a009']
        }
        """
        search = Search(using=self.__es, index=index)
        # explicitly exclude all fields since we only need the doc _id
        search = search.source(excludes=['*'])
        for key, values in fields.items():
            search = search.query(
                Bool(
                    filter=[
                        Q('terms', **{f'{META}.{table}.{key}': values}) |
                        Q('terms', **{f'{META}.{table}.{key}.keyword': values})
                    ]
                )
            )
        for hit in search.scan():
            yield hit.meta.id

    def search(self, index, body):
        """
        Search in Elasticsearch.

        NB: doc_type has been removed since Elasticsearch 7.x onwards
        """
        return self.__es.search(index=index, body=body)

    def _create_setting(self, index, node, setting=None):
        """Create Elasticsearch setting and mapping if required."""
        body = defaultdict(lambda: defaultdict(dict))

        if not self.__es.indices.exists(index):

            if setting:
                body.update(
                    **{
                        'settings': {
                            'index': setting
                        }
                    }
                )

            mapping = self._build_mapping(node)
            if mapping:
                body.update(**mapping)

            try:
                response = self.__es.indices.create(
                    index=index,
                    body=body,
                )
            except Exception:
                raise

            # check the response of the request
            logger.debug(f'create index response {response}')
            # check the result of the mapping on the index
            logger.debug(
                f'created mapping: {self.__es.indices.get_mapping(index)}'
            )
            logger.debug(
                f'created setting: {self.__es.indices.get_settings(index)}'
            )

    def _build_mapping(self, root):
        """Get the Elasticsearch mapping from the schema transform."""

        for node in traverse_post_order(root):

            rename = node.transform.get('rename', {})
            mapping = node.transform.get('mapping', {})

            for column in node.column_names:

                column = rename.get(column, column)
                if column not in mapping:
                    continue
                column_type = mapping[column]['type']
                if column_type not in ELASTICSEARCH_TYPES:
                    raise RuntimeError(
                        f'Invalid Elasticsearch type {column_type}'
                    )

                fields = mapping[column].get('fields')

                if 'properties' not in node._mapping:
                    node._mapping['properties'] = {}
                node._mapping['properties'][column] = {
                    'type': column_type
                }
                if fields:
                    node._mapping['properties'][column]['fields'] = fields

            if node.parent and node._mapping:
                if 'properties' not in node.parent._mapping:
                    node.parent._mapping['properties'] = {}
                node.parent._mapping['properties'][node.label] = node._mapping

        if root._mapping:
            return dict(mappings=root._mapping)
