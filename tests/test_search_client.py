"""SearchClient tests."""

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import elastic_transport
import mock
import pytest
from mock import ANY, MagicMock

from pgsync.search_client import elasticsearch, get_search_client, SearchClient
from pgsync.sync import settings

from .testing_utils import override_env_var


class TestSearchClient(object):
    """Search Client tests."""

    def test_get_search_init(self, mocker):
        url = "http://some-domain:33"
        with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value=url,
            ) as mock_search_url:
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ) as mock_search_client:
                    SearchClient()
                    mock_search_url.assert_called_once()
                    mock_search_client.assert_called_once_with(
                        url,
                        client=elasticsearch.Elasticsearch,
                        node_class=elastic_transport.RequestsHttpNode,
                    )

    def test_get_search_client(self, mocker):
        url = "http://some-domain:33"

        with override_env_var(
            ELASTICSEARCH="True",
            OPENSEARCH="False",
            ELASTICSEARCH_HTTP_AUTH="user,passwd",
            ELASTICSEARCH_POOL_MAXSIZE="25",
        ):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.elasticsearch.Elasticsearch",
                return_value=MagicMock(),
            ) as mock_search_client:
                get_search_client(
                    url,
                    client=elasticsearch.Elasticsearch,
                    node_class=elastic_transport.RequestsHttpNode,
                )
                ssl_assert_hostname = (
                    settings.ELASTICSEARCH_SSL_ASSERT_HOSTNAME
                )
                ssl_assert_fingerprint = (
                    settings.ELASTICSEARCH_SSL_ASSERT_FINGERPRINT
                )
                mock_search_client.assert_called_once_with(
                    hosts=[url],
                    http_auth=("user", "passwd"),
                    cloud_id=settings.ELASTICSEARCH_CLOUD_ID,
                    api_key=None,
                    basic_auth=settings.ELASTICSEARCH_BASIC_AUTH,
                    bearer_auth=settings.ELASTICSEARCH_BEARER_AUTH,
                    opaque_id=settings.ELASTICSEARCH_OPAQUE_ID,
                    http_compress=settings.ELASTICSEARCH_HTTP_COMPRESS,
                    verify_certs=settings.ELASTICSEARCH_VERIFY_CERTS,
                    ca_certs=settings.ELASTICSEARCH_CA_CERTS,
                    client_cert=settings.ELASTICSEARCH_CLIENT_CERT,
                    client_key=settings.ELASTICSEARCH_CLIENT_KEY,
                    ssl_assert_hostname=ssl_assert_hostname,
                    ssl_assert_fingerprint=ssl_assert_fingerprint,
                    ssl_version=settings.ELASTICSEARCH_SSL_VERSION,
                    ssl_context=settings.ELASTICSEARCH_SSL_CONTEXT,
                    ssl_show_warn=settings.ELASTICSEARCH_SSL_SHOW_WARN,
                    timeout=settings.ELASTICSEARCH_TIMEOUT,
                    connections_per_node=settings.ELASTICSEARCH_POOL_MAXSIZE,
                )

            with override_env_var(
                ELASTICSEARCH_AWS_HOSTED="True",
                ELASTICSEARCH="True",
                OPENSEARCH="False",
            ):
                importlib.reload(settings)
                with mock.patch(
                    "pgsync.search_client.elasticsearch.Elasticsearch",
                    return_value=MagicMock(),
                ) as mock_search_client:
                    with mock.patch(
                        "pgsync.search_client.AWS4Auth",
                        return_value="foo",
                    ):
                        with mock.patch(
                            "pgsync.search_client.boto3",
                            return_value=MagicMock(),
                        ):
                            get_search_client(
                                url,
                                client=elasticsearch.Elasticsearch,
                                node_class=elastic_transport.RequestsHttpNode,
                            )
                            mock_search_client.assert_called_once_with(
                                hosts=[url],
                                http_auth=ANY,
                                verify_certs=True,
                                node_class=elastic_transport.RequestsHttpNode,
                                timeout=settings.ELASTICSEARCH_TIMEOUT,
                                connections_per_node=settings.ELASTICSEARCH_POOL_MAXSIZE,
                            )

    def test_opensearch_init(self, mocker):
        """Test SearchClient initialization with OpenSearch."""
        url = "http://opensearch-domain:9200"
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value=url,
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    assert client.is_opensearch is True
                    assert client.name == "OpenSearch"

    def test_unknown_search_client_raises_error(self):
        """Test that disabling both search backends raises ValueError in settings."""
        # The settings module raises ValueError when both backends are disabled,
        # so we can't test the SearchClient RuntimeError directly.
        # This test verifies the settings validation works.
        with pytest.raises(ValueError) as excinfo:
            with override_env_var(ELASTICSEARCH="False", OPENSEARCH="False"):
                importlib.reload(settings)
        assert "Enable one search backend" in str(excinfo.value)

    def test_close(self):
        """Test SearchClient close method."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_transport = MagicMock()
                    client._SearchClient__client = MagicMock()
                    client._SearchClient__client.transport = mock_transport
                    client.close()
                    mock_transport.close.assert_called_once()

    def test_teardown(self):
        """Test SearchClient teardown method."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_indices = MagicMock()
                    client._SearchClient__client.indices = mock_indices

                    client.teardown("test_index")
                    mock_indices.delete.assert_called_once_with(
                        index="test_index", ignore=[400, 404]
                    )

    def test_bulk_streaming(self):
        """Test SearchClient bulk with streaming_bulk."""
        with override_env_var(
            ELASTICSEARCH="False",
            OPENSEARCH="True",
            ELASTICSEARCH_STREAMING_BULK="True",
        ):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    client.streaming_bulk = MagicMock(
                        return_value=[(True, {})]
                    )

                    actions = [{"_id": "1", "_source": {"field": "value"}}]
                    client.bulk("test_index", actions)

                    assert client.doc_count == 1

    def test_bulk_parallel(self):
        """Test SearchClient bulk with parallel_bulk."""
        with override_env_var(
            ELASTICSEARCH="False",
            OPENSEARCH="True",
            ELASTICSEARCH_STREAMING_BULK="False",
        ):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    client.parallel_bulk = MagicMock(
                        return_value=[(True, {}), (True, {})]
                    )

                    actions = [
                        {"_id": "1", "_source": {"field": "value"}},
                        {"_id": "2", "_source": {"field": "value2"}},
                    ]
                    client.bulk("test_index", actions)

                    assert client.doc_count == 2

    @mock.patch("pgsync.search_client.logger")
    def test_bulk_failed_docs_logged(self, mock_logger):
        """Test that failed documents are logged."""
        with override_env_var(
            ELASTICSEARCH="False",
            OPENSEARCH="True",
            ELASTICSEARCH_STREAMING_BULK="True",
        ):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    client.streaming_bulk = MagicMock(
                        return_value=[(False, {"error": "Failed"})]
                    )

                    actions = [{"_id": "1", "_source": {"field": "value"}}]
                    client.bulk("test_index", actions)

                    mock_logger.error.assert_called_once()
                    assert client.doc_count == 0

    def test_refresh(self):
        """Test SearchClient refresh method."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_indices = MagicMock()
                    client._SearchClient__client.indices = mock_indices

                    client.refresh(["index1", "index2"])
                    mock_indices.refresh.assert_called_once_with(
                        index=["index1", "index2"]
                    )

    def test_search(self):
        """Test SearchClient search method."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_client = MagicMock()
                    mock_client.search.return_value = {"hits": {"total": 10}}
                    client._SearchClient__client = mock_client

                    body = {"query": {"match_all": {}}}
                    result = client.search("test_index", body)

                    mock_client.search.assert_called_once_with(
                        index="test_index", body=body
                    )
                    assert result["hits"]["total"] == 10

    def test_create_setting_when_index_exists(self):
        """Test _create_setting does nothing when index exists."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_indices = MagicMock()
                    mock_indices.exists.return_value = True
                    client._SearchClient__client.indices = mock_indices

                    # Create mock tree
                    mock_tree = MagicMock()

                    client._create_setting("test_index", mock_tree)

                    # indices.create should NOT be called since index exists
                    mock_indices.create.assert_not_called()

    def test_create_setting_with_custom_setting(self):
        """Test _create_setting with custom settings."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    mock_indices = MagicMock()
                    mock_indices.exists.return_value = False
                    mock_indices.create.return_value = {"acknowledged": True}
                    mock_indices.get_mapping.return_value = {}
                    mock_indices.get_settings.return_value = {}
                    client._SearchClient__client.indices = mock_indices

                    mock_tree = MagicMock()
                    mock_tree.traverse_post_order.return_value = []
                    mock_tree.root = MagicMock()
                    mock_tree.root._mapping = {}

                    custom_setting = {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                    }
                    client._create_setting(
                        "test_index", mock_tree, setting=custom_setting
                    )

                    mock_indices.create.assert_called_once()

    def test_build_mapping_invalid_type_raises_error(self):
        """Test _build_mapping raises error for invalid ES type."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()

                    # Create mock tree with invalid mapping type
                    mock_node = MagicMock()
                    mock_node.transform = {
                        "mapping": {"field1": {"type": "invalid_type"}}
                    }
                    mock_node._mapping = {}
                    mock_node.parent = None

                    mock_tree = MagicMock()
                    mock_tree.traverse_post_order.return_value = [mock_node]
                    mock_tree.root = mock_node

                    with pytest.raises(RuntimeError) as excinfo:
                        client._build_mapping(mock_tree)
                    assert "Invalid Elasticsearch type" in str(excinfo.value)

    def test_build_mapping_invalid_parameter_raises_error(self):
        """Test _build_mapping raises error for invalid mapping parameter."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()

                    # Create mock tree with invalid mapping parameter
                    mock_node = MagicMock()
                    mock_node.transform = {
                        "mapping": {
                            "field1": {
                                "type": "text",
                                "invalid_param": "value",
                            }
                        }
                    }
                    mock_node._mapping = {}
                    mock_node.parent = None

                    mock_tree = MagicMock()
                    mock_tree.traverse_post_order.return_value = [mock_node]
                    mock_tree.root = mock_node

                    with pytest.raises(RuntimeError) as excinfo:
                        client._build_mapping(mock_tree)
                    assert "Invalid Elasticsearch mapping parameter" in str(
                        excinfo.value
                    )

    def test_build_mapping_with_routing(self):
        """Test _build_mapping adds routing when specified."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()

                    mock_node = MagicMock()
                    mock_node.transform = {
                        "mapping": {"field1": {"type": "text"}}
                    }
                    mock_node._mapping = {}
                    mock_node.parent = None

                    mock_tree = MagicMock()
                    mock_tree.traverse_post_order.return_value = [mock_node]
                    mock_tree.root = mock_node

                    result = client._build_mapping(
                        mock_tree, routing="user_id"
                    )

                    assert mock_node._mapping.get("_routing") == {
                        "required": True
                    }

    def test_doc_count_property(self):
        """Test doc_count is properly initialized."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    assert client.doc_count == 0

    def test_major_version_default(self):
        """Test major_version defaults to 0."""
        with override_env_var(ELASTICSEARCH="False", OPENSEARCH="True"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=MagicMock(),
                ):
                    client = SearchClient()
                    assert client.major_version == 0


# ============================================================================
# PHASE 5 EXTENDED TESTS - Search Client Comprehensive Coverage
# ============================================================================


class TestSearchClientBulkOperations:
    """Extended tests for bulk operations."""

    def test_bulk_with_custom_chunk_size(self):
        """Test bulk operations with custom chunk size."""
        with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                mock_client = MagicMock()
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=mock_client,
                ):
                    client = SearchClient()
                    actions = [
                        {
                            "_index": "test",
                            "_id": "1",
                            "_source": {"field": "value"},
                        }
                    ]

                    with mock.patch.object(client, "_bulk") as mock_bulk:
                        client.bulk("test", actions, chunk_size=100)

                        # Should pass custom chunk_size
                        call_kwargs = mock_bulk.call_args[1]
                        assert call_kwargs["chunk_size"] == 100

    def test_bulk_with_retry_parameters(self):
        """Test bulk operations with retry configuration."""
        with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                mock_client = MagicMock()
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=mock_client,
                ):
                    client = SearchClient()
                    actions = [{"_index": "test", "_id": "1"}]

                    with mock.patch.object(client, "_bulk") as mock_bulk:
                        client.bulk(
                            "test",
                            actions,
                            max_retries=5,
                            initial_backoff=2.0,
                            max_backoff=60.0,
                        )

                        # Should pass retry parameters
                        call_kwargs = mock_bulk.call_args[1]
                        assert call_kwargs["max_retries"] == 5
                        assert call_kwargs["initial_backoff"] == 2.0
                        assert call_kwargs["max_backoff"] == 60.0

    def test_bulk_exception_handling_with_raise_on_exception(self):
        """Test bulk raises exception when raise_on_exception is True."""
        with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                mock_client = MagicMock()
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=mock_client,
                ):
                    client = SearchClient()
                    actions = [{"_index": "test", "_id": "1"}]

                    with mock.patch.object(
                        client, "_bulk", side_effect=Exception("Bulk failed")
                    ):
                        with pytest.raises(Exception) as excinfo:
                            client.bulk(
                                "test", actions, raise_on_exception=True
                            )
                        assert "Bulk failed" in str(excinfo.value)

    def test_bulk_exception_handling_with_raise_on_error(self):
        """Test bulk raises exception when raise_on_error is True."""
        with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                mock_client = MagicMock()
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=mock_client,
                ):
                    client = SearchClient()
                    actions = [{"_index": "test", "_id": "1"}]

                    with mock.patch.object(
                        client,
                        "_bulk",
                        side_effect=Exception("Error occurred"),
                    ):
                        with pytest.raises(Exception) as excinfo:
                            client.bulk("test", actions, raise_on_error=True)
                        assert "Error occurred" in str(excinfo.value)

    def test_bulk_suppresses_exception_when_raise_flags_false(self):
        """Test bulk suppresses exception when settings have raise flags as False."""
        with override_env_var(
            ELASTICSEARCH="True",
            OPENSEARCH="False",
            ELASTICSEARCH_RAISE_ON_EXCEPTION="False",
            ELASTICSEARCH_RAISE_ON_ERROR="False",
        ):
            importlib.reload(settings)
            with mock.patch(
                "pgsync.search_client.get_search_url",
                return_value="http://localhost:9200",
            ):
                mock_client = MagicMock()
                with mock.patch(
                    "pgsync.search_client.get_search_client",
                    return_value=mock_client,
                ):
                    client = SearchClient()
                    actions = [{"_index": "test", "_id": "1"}]

                    with mock.patch.object(
                        client, "_bulk", side_effect=Exception("Bulk failed")
                    ):
                        # Should not raise when settings are False
                        client.bulk("test", actions)


