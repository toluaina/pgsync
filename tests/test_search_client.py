"""SearchClient tests."""

import importlib

import elastic_transport
import mock
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
                            )
