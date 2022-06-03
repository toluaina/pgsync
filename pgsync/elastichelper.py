"""PGSync Elasticsearch helper."""
import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import boto3
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.query import Bool
from requests_aws4auth import AWS4Auth

from .constants import (
    ELASTICSEARCH_MAPPING_PARAMETERS,
    ELASTICSEARCH_TAGLINE,
    ELASTICSEARCH_TYPES,
    META,
)
from .node import Node
from .settings import (
    ELASTICSEARCH_API_KEY,
    ELASTICSEARCH_API_KEY_ID,
    ELASTICSEARCH_AWS_HOSTED,
    ELASTICSEARCH_AWS_REGION,
    ELASTICSEARCH_BASIC_AUTH,
    ELASTICSEARCH_BEARER_AUTH,
    ELASTICSEARCH_CA_CERTS,
    ELASTICSEARCH_CHUNK_SIZE,
    ELASTICSEARCH_CLIENT_CERT,
    ELASTICSEARCH_CLIENT_KEY,
    ELASTICSEARCH_CLOUD_ID,
    ELASTICSEARCH_HTTP_COMPRESS,
    ELASTICSEARCH_INITIAL_BACKOFF,
    ELASTICSEARCH_MAX_BACKOFF,
    ELASTICSEARCH_MAX_CHUNK_BYTES,
    ELASTICSEARCH_MAX_RETRIES,
    ELASTICSEARCH_OPAQUE_ID,
    ELASTICSEARCH_QUEUE_SIZE,
    ELASTICSEARCH_RAISE_ON_ERROR,
    ELASTICSEARCH_RAISE_ON_EXCEPTION,
    ELASTICSEARCH_SSL_ASSERT_FINGERPRINT,
    ELASTICSEARCH_SSL_ASSERT_HOSTNAME,
    ELASTICSEARCH_SSL_CONTEXT,
    ELASTICSEARCH_SSL_SHOW_WARN,
    ELASTICSEARCH_SSL_VERSION,
    ELASTICSEARCH_STREAMING_BULK,
    ELASTICSEARCH_THREAD_COUNT,
    ELASTICSEARCH_TIMEOUT,
    ELASTICSEARCH_USE_SSL,
    ELASTICSEARCH_VERIFY_CERTS,
)
from .urls import get_elasticsearch_url

logger = logging.getLogger(__name__)


class ElasticHelper(object):
    """Elasticsearch Helper."""

    def __init__(self):
        """
        Return an Elasticsearch client.

        The default connection parameters are:
        host = 'localhost', port = 9200
        """
        url: str = get_elasticsearch_url()
        self.__client: Elasticsearch = get_elasticsearch_client(url)
        self.is_opensearch: bool = False
        try:
            self.major_version: int = int(
                self.__client.info()["version"]["number"].split(".")[0]
            )
            self.is_opensearch = (
                self.__client.info()["tagline"] != ELASTICSEARCH_TAGLINE
            )
        except (IndexError, KeyError, ValueError):
            self.major_version: int = 0
        self.doc_count: int = 0

    def close(self) -> None:
        """Close transport connection."""
        self.__client.transport.close()

    def teardown(self, index: str) -> None:
        """
        Teardown the Elasticsearch index.

        :arg index: index (or list of indices) to read documents from
        """
        try:
            logger.debug(f"Deleting index {index}")
            self.__client.indices.delete(index=index, ignore=[400, 404])
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise

    def bulk(
        self,
        index: str,
        actions: Iterable[Union[bytes, str, Dict[str, Any]]],
        chunk_size: Optional[int] = None,
        max_chunk_bytes: Optional[int] = None,
        queue_size: Optional[int] = None,
        thread_count: Optional[int] = None,
        refresh: bool = False,
        max_retries: Optional[int] = None,
        initial_backoff: Optional[float] = None,
        max_backoff: Optional[float] = None,
        raise_on_exception: Optional[bool] = None,
        raise_on_error: Optional[bool] = None,
    ) -> None:
        """Pull sync data from generator to Elasticsearch."""
        chunk_size: int = chunk_size or ELASTICSEARCH_CHUNK_SIZE
        max_chunk_bytes: int = max_chunk_bytes or ELASTICSEARCH_MAX_CHUNK_BYTES
        thread_count: int = thread_count or ELASTICSEARCH_THREAD_COUNT
        queue_size: int = queue_size or ELASTICSEARCH_QUEUE_SIZE
        # max_retries, initial_backoff & max_backoff are only applicable when
        # streaming bulk is in use
        max_retries: int = max_retries or ELASTICSEARCH_MAX_RETRIES
        initial_backoff: float = (
            initial_backoff or ELASTICSEARCH_INITIAL_BACKOFF
        )
        max_backoff: float = max_backoff or ELASTICSEARCH_MAX_BACKOFF
        raise_on_exception: bool = (
            raise_on_exception or ELASTICSEARCH_RAISE_ON_EXCEPTION
        )
        raise_on_error: bool = raise_on_error or ELASTICSEARCH_RAISE_ON_ERROR

        try:
            self._bulk(
                index,
                actions,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                queue_size=queue_size,
                thread_count=thread_count,
                refresh=refresh,
                max_retries=max_retries,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff,
                raise_on_exception=raise_on_exception,
                raise_on_error=raise_on_error,
            )
        except Exception as e:
            logger.exception(f"Exception {e}")
            if raise_on_exception or raise_on_error:
                raise

    def _bulk(
        self,
        index: str,
        actions: Iterable[Union[bytes, str, Dict[str, Any]]],
        chunk_size: int,
        max_chunk_bytes: int,
        queue_size: int,
        thread_count: int,
        refresh: bool,
        max_retries: int,
        initial_backoff: float,
        max_backoff: float,
        raise_on_exception: bool,
        raise_on_error: bool,
    ):
        """Bulk index, update, delete docs to Elasticsearch."""
        # when using multiple threads for poll_db we need to account for other
        # threads performing deletions
        ignore_status: Tuple[int] = (400, 404)

        if ELASTICSEARCH_STREAMING_BULK:
            for _ in helpers.streaming_bulk(
                self.__client,
                actions,
                index=index,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                max_retries=max_retries,
                max_backoff=max_backoff,
                initial_backoff=initial_backoff,
                refresh=refresh,
                raise_on_exception=raise_on_exception,
                raise_on_error=raise_on_error,
            ):
                self.doc_count += 1
        else:
            # parallel bulk consumes more memory and is also more likely
            # to result in 429 errors.
            for _ in helpers.parallel_bulk(
                self.__client,
                actions,
                thread_count=thread_count,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                queue_size=queue_size,
                refresh=refresh,
                raise_on_exception=raise_on_exception,
                raise_on_error=raise_on_error,
                ignore_status=ignore_status,
            ):
                self.doc_count += 1

    def refresh(self, indices: List[str]) -> None:
        """Refresh the Elasticsearch index."""
        self.__client.indices.refresh(index=indices)

    def _search(self, index: str, table: str, fields: Optional[dict] = None):
        """
        Search private area for matching docs in Elasticsearch.

        only returns the _id of the matching document.

        fields = {
            'id': [1, 2],
            'uid': ['a002', 'a009'],
        }
        """
        fields: dict = fields or {}
        search: Search = Search(using=self.__client, index=index)
        # explicitly exclude all fields since we only need the doc _id
        search = search.source(excludes=["*"])
        for key, values in fields.items():
            search = search.query(
                Bool(
                    filter=[
                        Q("terms", **{f"{META}.{table}.{key}": values})
                        | Q(
                            "terms",
                            **{f"{META}.{table}.{key}.keyword": values},
                        )
                    ]
                )
            )
        for hit in search.scan():
            yield hit.meta.id

    def search(self, index: str, body: dict):
        """
        Search in Elasticsearch.

        NB: doc_type has been removed since Elasticsearch 7.x onwards
        """
        return self.__client.search(index=index, body=body)

    def _create_setting(
        self,
        index: str,
        node: Node,
        setting: Optional[dict] = None,
        mapping: Optional[dict] = None,
        routing: Optional[str] = None,
    ) -> None:
        """Create Elasticsearch setting and mapping if required."""
        body: dict = defaultdict(lambda: defaultdict(dict))

        if not self.__client.indices.exists(index):

            if setting:
                body.update(**{"settings": {"index": setting}})

            if mapping:
                if "dynamic_templates" in mapping:
                    body.update(**{"mappings": mapping})
                else:
                    body.update(**{"mappings": {"properties": mapping}})
            else:
                mapping: dict = self._build_mapping(node, routing)
                if mapping:
                    body.update(**mapping)
            try:
                response = self.__client.indices.create(index=index, body=body)
            except Exception:
                raise

            # check the response of the request
            logger.debug(f"create index response {response}")
            # check the result of the mapping on the index
            logger.debug(
                f"created mapping: {self.__client.indices.get_mapping(index)}"
            )
            logger.debug(
                f"created setting: {self.__client.indices.get_settings(index)}"
            )

    def _build_mapping(self, root: Node, routing: str) -> Optional[dict]:
        """Get the Elasticsearch mapping from the schema transform."""
        for node in root.traverse_post_order():

            rename: dict = node.transform.get("rename", {})
            mapping: dict = node.transform.get("mapping", {})

            for key, value in mapping.items():
                column: str = rename.get(key, key)
                column_type: str = mapping[column]["type"]
                if column_type not in ELASTICSEARCH_TYPES:
                    raise RuntimeError(
                        f"Invalid Elasticsearch type {column_type}"
                    )

                if "properties" not in node._mapping:
                    node._mapping["properties"] = {}
                node._mapping["properties"][column] = {"type": column_type}

                for parameter, parameter_value in mapping[column].items():
                    if parameter == "type":
                        continue

                    if parameter not in ELASTICSEARCH_MAPPING_PARAMETERS:
                        raise RuntimeError(
                            f"Invalid Elasticsearch mapping parameter "
                            f"{parameter}"
                        )

                    node._mapping["properties"][column][
                        parameter
                    ] = parameter_value

            if node.parent and node._mapping:
                if "properties" not in node.parent._mapping:
                    node.parent._mapping["properties"] = {}
                node.parent._mapping["properties"][node.label] = node._mapping

        if routing:
            root._mapping["_routing"] = {"required": True}

        if root._mapping:
            if self.major_version < 7 and not self.is_opensearch:
                root._mapping = {"_doc": root._mapping}

            return dict(mappings=root._mapping)


def get_elasticsearch_client(url: str) -> Elasticsearch:
    if ELASTICSEARCH_AWS_HOSTED:
        credentials = boto3.Session().get_credentials()
        return Elasticsearch(
            hosts=[url],
            http_auth=AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                ELASTICSEARCH_AWS_REGION,
                "es",
                session_token=credentials.token,
            ),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
    else:
        hosts: List[str] = [url]
        # API
        cloud_id: Optional[str] = ELASTICSEARCH_CLOUD_ID
        api_key: Optional[Union[str, Tuple[str, str]]] = None
        if ELASTICSEARCH_API_KEY_ID and ELASTICSEARCH_API_KEY:
            api_key = (ELASTICSEARCH_API_KEY_ID, ELASTICSEARCH_API_KEY)
        basic_auth: Optional[str] = ELASTICSEARCH_BASIC_AUTH
        bearer_auth: Optional[str] = ELASTICSEARCH_BEARER_AUTH
        opaque_id: Optional[str] = ELASTICSEARCH_OPAQUE_ID
        # Node
        http_compress: bool = ELASTICSEARCH_HTTP_COMPRESS
        verify_certs: bool = ELASTICSEARCH_VERIFY_CERTS
        ca_certs: Optional[str] = ELASTICSEARCH_CA_CERTS
        client_cert: Optional[str] = ELASTICSEARCH_CLIENT_CERT
        client_key: Optional[str] = ELASTICSEARCH_CLIENT_KEY
        ssl_assert_hostname: Optional[str] = ELASTICSEARCH_SSL_ASSERT_HOSTNAME
        ssl_assert_fingerprint: Optional[
            str
        ] = ELASTICSEARCH_SSL_ASSERT_FINGERPRINT
        ssl_version: Optional[int] = ELASTICSEARCH_SSL_VERSION
        ssl_context: Optional[Any] = ELASTICSEARCH_SSL_CONTEXT
        ssl_show_warn: bool = ELASTICSEARCH_SSL_SHOW_WARN
        # Transport
        use_ssl: bool = ELASTICSEARCH_USE_SSL
        timeout: float = ELASTICSEARCH_TIMEOUT

        return Elasticsearch(
            hosts=hosts,
            cloud_id=cloud_id,
            api_key=api_key,
            basic_auth=basic_auth,
            bearer_auth=bearer_auth,
            opaque_id=opaque_id,
            http_compress=http_compress,
            verify_certs=verify_certs,
            ca_certs=ca_certs,
            client_cert=client_cert,
            client_key=client_key,
            ssl_assert_hostname=ssl_assert_hostname,
            ssl_assert_fingerprint=ssl_assert_fingerprint,
            ssl_version=ssl_version,
            ssl_context=ssl_context,
            ssl_show_warn=ssl_show_warn,
            use_ssl=use_ssl,
            timeout=timeout,
        )
