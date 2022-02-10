"""Elasticsearch helper tests."""

import mock
from elasticsearch import RequestsHttpConnection
from mock import ANY, MagicMock

from pgsync.elastichelper import ElasticHelper, get_elasticsearch_client
from pgsync.settings import (
    ELASTICSEARCH_CA_CERTS,
    ELASTICSEARCH_CLIENT_CERT,
    ELASTICSEARCH_CLIENT_KEY,
    ELASTICSEARCH_SSL_SHOW_WARN,
    ELASTICSEARCH_TIMEOUT,
    ELASTICSEARCH_USE_SSL,
    ELASTICSEARCH_VERIFY_CERTS,
)


class TestElasticsearchHelper(object):
    """Elasticsearch helper tests."""

    def test_get_elasticsearch_init(self, mocker):
        url = "http://some-domain:33"
        with mock.patch(
            "pgsync.elastichelper.get_elasticsearch_url",
            return_value=url,
        ) as mock_get_elasticsearch_url:
            with mock.patch(
                "pgsync.elastichelper.get_elasticsearch_client",
                return_value=MagicMock(),
            ) as mock_get_elasticsearch_client:
                ElasticHelper()
                mock_get_elasticsearch_url.assert_called_once()
                mock_get_elasticsearch_client.assert_called_once_with(url)

    def test_get_elasticsearch_client(self, mocker):
        url = "http://some-domain:33"

        with mock.patch(
            "pgsync.elastichelper.Elasticsearch",
            return_value=MagicMock(),
        ) as mocker_elasticsearch:
            get_elasticsearch_client(url)
            mocker_elasticsearch.assert_called_once_with(
                hosts=[url],
                timeout=ELASTICSEARCH_TIMEOUT,
                verify_certs=ELASTICSEARCH_VERIFY_CERTS,
                use_ssl=ELASTICSEARCH_USE_SSL,
                ssl_show_warn=ELASTICSEARCH_SSL_SHOW_WARN,
                ca_certs=ELASTICSEARCH_CA_CERTS,
                client_cert=ELASTICSEARCH_CLIENT_CERT,
                client_key=ELASTICSEARCH_CLIENT_KEY,
                api_key=None,
            )

        with mock.patch(
            "pgsync.elastichelper.ELASTICSEARCH_AWS_HOSTED",
            returns=True,
        ):
            with mock.patch(
                "pgsync.elastichelper.Elasticsearch",
                return_value=MagicMock(),
            ) as mocker_elasticsearch:
                with mock.patch(
                    "pgsync.elastichelper.AWS4Auth",
                    return_value="foo",
                ):
                    with mock.patch(
                        "pgsync.elastichelper.boto3",
                        return_value=MagicMock(),
                    ):
                        get_elasticsearch_client(url)
                        mocker_elasticsearch.assert_called_once_with(
                            hosts=[url],
                            http_auth=ANY,
                            use_ssl=True,
                            verify_certs=True,
                            connection_class=RequestsHttpConnection,
                        )
