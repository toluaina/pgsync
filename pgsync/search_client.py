"""PGSync SearchClient helper."""

import logging
import typing as t
from collections import defaultdict

import boto3
import elastic_transport
import elasticsearch
import elasticsearch_dsl
import opensearch_dsl
import opensearchpy
from requests_aws4auth import AWS4Auth

from . import settings
from .constants import (
    ELASTICSEARCH_MAPPING_PARAMETERS,
    ELASTICSEARCH_TYPES,
    META,
)
from .node import Tree
from .urls import get_search_url

logger = logging.getLogger(__name__)


class SearchClient(object):
    """SearchClient."""

    def __init__(self):
        """
        Return an Elasticsearch/OpenSearch client.

        The default connection parameters are:
        host = 'localhost', port = 9200
        """
        url: str = get_search_url()
        self.is_opensearch: bool = False
        self.major_version: int = 0
        if settings.ELASTICSEARCH:
            self.name = "Elasticsearch"
            self.__client: elasticsearch.Elasticsearch = get_search_client(
                url,
                client=elasticsearch.Elasticsearch,
                node_class=elastic_transport.RequestsHttpNode,
            )
            try:
                self.major_version: int = int(
                    self.__client.info()["version"]["number"].split(".")[0]
                )
            except (IndexError, KeyError, ValueError):
                pass
            self.streaming_bulk: t.Callable = (
                elasticsearch.helpers.streaming_bulk
            )
            self.parallel_bulk: t.Callable = (
                elasticsearch.helpers.parallel_bulk
            )
            self.Search: t.Callable = elasticsearch_dsl.Search
            self.Bool: t.Callable = elasticsearch_dsl.query.Bool
            self.Q: t.Callable = elasticsearch_dsl.Q

        elif settings.OPENSEARCH:
            self.is_opensearch = True
            self.name = "OpenSearch"
            self.__client: opensearchpy.OpenSearch = get_search_client(
                url,
                client=opensearchpy.OpenSearch,
                connection_class=opensearchpy.RequestsHttpConnection,
            )
            self.streaming_bulk: t.Callable = (
                opensearchpy.helpers.streaming_bulk
            )
            self.parallel_bulk: t.Callable = opensearchpy.helpers.parallel_bulk
            self.Search: t.Callable = opensearch_dsl.Search
            self.Bool: t.Callable = opensearch_dsl.query.Bool
            self.Q: t.Callable = opensearch_dsl.Q
        else:
            raise RuntimeError("Unknown search client")

        self.doc_count: int = 0

    def close(self) -> None:
        """Close transport connection."""
        self.__client.transport.close()

    def teardown(self, index: str) -> None:
        """
        Teardown the Elasticsearch/OpenSearch index.

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
        actions: t.Iterable[t.Union[bytes, str, t.Dict[str, t.Any]]],
        chunk_size: t.Optional[int] = None,
        max_chunk_bytes: t.Optional[int] = None,
        queue_size: t.Optional[int] = None,
        thread_count: t.Optional[int] = None,
        refresh: bool = False,
        max_retries: t.Optional[int] = None,
        initial_backoff: t.Optional[float] = None,
        max_backoff: t.Optional[float] = None,
        raise_on_exception: t.Optional[bool] = None,
        raise_on_error: t.Optional[bool] = None,
        ignore_status: t.Tuple[int] = None,
    ) -> None:
        """Pull sync data from generator to Elasticsearch/OpenSearch."""
        chunk_size = chunk_size or settings.ELASTICSEARCH_CHUNK_SIZE
        max_chunk_bytes = (
            max_chunk_bytes or settings.ELASTICSEARCH_MAX_CHUNK_BYTES
        )
        thread_count = thread_count or settings.ELASTICSEARCH_THREAD_COUNT
        queue_size = queue_size or settings.ELASTICSEARCH_QUEUE_SIZE
        # max_retries, initial_backoff & max_backoff are only applicable when
        # streaming bulk is in use
        max_retries = max_retries or settings.ELASTICSEARCH_MAX_RETRIES
        initial_backoff = (
            initial_backoff or settings.ELASTICSEARCH_INITIAL_BACKOFF
        )
        max_backoff = max_backoff or settings.ELASTICSEARCH_MAX_BACKOFF
        raise_on_exception = (
            raise_on_exception or settings.ELASTICSEARCH_RAISE_ON_EXCEPTION
        )
        raise_on_error = (
            raise_on_error or settings.ELASTICSEARCH_RAISE_ON_ERROR
        )
        ignore_status = ignore_status or settings.ELASTICSEARCH_IGNORE_STATUS

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
                ignore_status=ignore_status,
            )
        except Exception as e:
            logger.exception(f"Exception {e}")
            if raise_on_exception or raise_on_error:
                raise

    def _bulk(
        self,
        index: str,
        actions: t.Iterable[t.Union[bytes, str, t.Dict[str, t.Any]]],
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
        ignore_status: t.Tuple[int],
    ):
        """Bulk index, update, delete docs to Elasticsearch/OpenSearch."""
        if settings.ELASTICSEARCH_STREAMING_BULK:
            for ok, _ in self.streaming_bulk(
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
                if ok:
                    self.doc_count += 1
        else:
            # parallel bulk consumes more memory and is also more likely
            # to result in 429 errors.
            for ok, _ in self.parallel_bulk(
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
                if ok:
                    self.doc_count += 1

    def refresh(self, indices: t.List[str]) -> None:
        """Refresh the Elasticsearch/OpenSearch index."""
        self.__client.indices.refresh(index=indices)

    def _search(self, index: str, table: str, fields: t.Optional[dict] = None):
        """
        Search private area for matching docs in Elasticsearch/OpenSearch.

        only returns the _id of the matching document.

        fields = {
            'id': [1, 2],
            'uid': ['a002', 'a009'],
        }
        """
        fields = fields or {}
        search = self.Search(using=self.__client, index=index)
        # explicitly exclude all fields since we only need the doc _id
        search = search.source(excludes=["*"])
        for key, values in fields.items():
            search = search.query(
                self.Bool(
                    filter=[
                        self.Q("terms", **{f"{META}.{table}.{key}": values})
                        | self.Q(
                            "terms",
                            **{f"{META}.{table}.{key}.keyword": values},
                        )
                    ]
                )
            )
        try:
            for hit in search.scan():
                yield hit.meta.id
        except elasticsearch.exceptions.RequestError as e:
            logger.warning(f"RequestError: {e}")
            if "is out of range for a long" not in str(e):
                raise

    def search(self, index: str, body: dict) -> t.Any:
        """
        Search in Elasticsearch/OpenSearch.

        NB: doc_type has been removed since Elasticsearch 7.x onwards
        """
        return self.__client.search(index=index, body=body)

    def _create_setting(
        self,
        index: str,
        tree: Tree,
        setting: t.Optional[dict] = None,
        mapping: t.Optional[dict] = None,
        routing: t.Optional[str] = None,
    ) -> None:
        """Create Elasticsearch/OpenSearch setting and mapping if required."""
        body: dict = defaultdict(lambda: defaultdict(dict))

        if not self.__client.indices.exists(index=index):
            if setting:
                body.update(**{"settings": {"index": setting}})

            if mapping:
                if "dynamic_templates" in mapping:
                    body.update(**{"mappings": mapping})
                else:
                    body.update(**{"mappings": {"properties": mapping}})
            else:
                mapping = self._build_mapping(tree, routing)
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
                f"created mapping: "
                f"{self.__client.indices.get_mapping(index=index)}"
            )
            logger.debug(
                f"created setting: "
                f"{self.__client.indices.get_settings(index=index)}"
            )

    def _build_mapping(
        self, tree: Tree, routing: t.Optional[str] = None
    ) -> t.Optional[dict]:
        """
        Get the Elasticsearch/OpenSearch mapping from the schema transform.
        """  # noqa D200
        for node in tree.traverse_post_order():
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
            tree.root._mapping["_routing"] = {"required": True}

        if tree.root._mapping:
            if self.major_version < 7 and not self.is_opensearch:
                tree.root._mapping = {"_doc": tree.root._mapping}

            return dict(mappings=tree.root._mapping)

        return None


def get_search_client(
    url: str,
    client: t.Union[opensearchpy.OpenSearch, elasticsearch.Elasticsearch],
    connection_class: t.Optional[opensearchpy.RequestsHttpConnection] = None,
    node_class: t.Optional[elastic_transport.RequestsHttpNode] = None,
) -> t.Union[opensearchpy.OpenSearch, elasticsearch.Elasticsearch]:
    """
    Returns a search client based on the specified parameters.

    Args:
        url (str): The URL of the search client.
        client (Union[opensearchpy.OpenSearch, elasticsearch.Elasticsearch]): The search client to use.
        connection_class (opensearchpy.RequestsHttpConnection): The connection class to use.
        node_class (elastic_transport.RequestsHttpNode): The node class to use.

    Returns:
        Union[opensearchpy.OpenSearch, elasticsearch.Elasticsearch]: The search client.

    Raises:
        None
    """
    if settings.OPENSEARCH_AWS_HOSTED or settings.ELASTICSEARCH_AWS_HOSTED:
        credentials = boto3.Session().get_credentials()
        service: str = "aoss" if settings.OPENSEARCH_AWS_SERVERLESS else "es"
        if settings.OPENSEARCH:
            return client(
                hosts=[url],
                http_auth=AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    settings.ELASTICSEARCH_AWS_REGION,
                    service,
                    session_token=credentials.token,
                ),
                use_ssl=True,
                verify_certs=True,
                connection_class=connection_class,
            )
        elif settings.ELASTICSEARCH:
            return client(
                hosts=[url],
                http_auth=AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    settings.ELASTICSEARCH_AWS_REGION,
                    service,
                    session_token=credentials.token,
                ),
                use_ssl=True,
                verify_certs=True,
                node_class=node_class,
            )
    else:
        hosts: t.List[str] = [url]
        # API
        cloud_id: t.Optional[str] = settings.ELASTICSEARCH_CLOUD_ID
        api_key: t.Optional[t.Union[str, t.Tuple[str, str]]] = None
        http_auth: t.Optional[t.Union[str, t.Tuple[str, str]]] = (
            settings.ELASTICSEARCH_HTTP_AUTH
        )
        if (
            settings.ELASTICSEARCH_API_KEY_ID
            and settings.ELASTICSEARCH_API_KEY
        ):
            api_key = (
                settings.ELASTICSEARCH_API_KEY_ID,
                settings.ELASTICSEARCH_API_KEY,
            )
        basic_auth: t.Optional[str] = settings.ELASTICSEARCH_BASIC_AUTH
        bearer_auth: t.Optional[str] = settings.ELASTICSEARCH_BEARER_AUTH
        opaque_id: t.Optional[str] = settings.ELASTICSEARCH_OPAQUE_ID
        # Node
        http_compress: bool = settings.ELASTICSEARCH_HTTP_COMPRESS
        verify_certs: bool = settings.ELASTICSEARCH_VERIFY_CERTS
        ca_certs: t.Optional[str] = settings.ELASTICSEARCH_CA_CERTS
        client_cert: t.Optional[str] = settings.ELASTICSEARCH_CLIENT_CERT
        client_key: t.Optional[str] = settings.ELASTICSEARCH_CLIENT_KEY
        ssl_assert_hostname: t.Optional[str] = (
            settings.ELASTICSEARCH_SSL_ASSERT_HOSTNAME
        )
        ssl_assert_fingerprint: t.Optional[str] = (
            settings.ELASTICSEARCH_SSL_ASSERT_FINGERPRINT
        )
        ssl_version: t.Optional[int] = settings.ELASTICSEARCH_SSL_VERSION
        ssl_context: t.Optional[t.Any] = settings.ELASTICSEARCH_SSL_CONTEXT
        ssl_show_warn: bool = settings.ELASTICSEARCH_SSL_SHOW_WARN
        # Transport
        timeout: float = settings.ELASTICSEARCH_TIMEOUT
        return client(
            hosts=hosts,
            http_auth=http_auth,
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
            # use_ssl=use_ssl,
            timeout=timeout,
        )
