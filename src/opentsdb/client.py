import logging
import requests

from utils import Endpoints, RequestParameters, Filters
import request

logger = logging.getLogger(__name__)


class Client:
    """OpenTSDB client class."""

    def __init__(
        self, url: str, port: int, session: requests.Session = requests.Session()
    ):
        """Initializes the OpenTSDB class.

        Args:
            url (str): OpenTSDB server url.
            port (int): OpenTSDB server port.
            session (requests.Session, optional): Requests session. Defaults to requests.Session().
        """
        self._url = url
        self._port = port
        self._complete_url = f"{self._url}:{self._port}"
        self._server_config = {}
        self._server_version = {}
        self._server_filters = {}
        self._server_aggregators = []
        self._session = session

        logger.info(f"Initialized OpenTSDB client with url: {self._complete_url}")

    @property
    def url(self) -> str:
        """Returns the OpenTSDB server url.

        Returns:
            str: OpenTSDB server url.
        """
        return self._url

    @property
    def port(self) -> int:
        """Returns the OpenTSDB server port.

        Returns:
            int: OpenTSDB server port.
        """
        return self._port

    @property
    def complete_url(self) -> str:
        """Returns the complete OpenTSDB server url.

        Returns:
            str: Complete OpenTSDB server url.
        """
        return self._complete_url

    def update_filters(self) -> None:
        """Updates the locally stored dict of available filters on the OpenTSDB server."""
        url = f"{self._complete_url}{Endpoints.FILTERS}"
        self._server_filters = requests.get(url=url).json()

    @property
    def filters(self) -> dict:
        """Returns the filters that are available on the OpenTSDB server.

        Returns:
            dict: Filters that are available on the OpenTSDB server.
        """
        if not self._server_filters:
            self.update_filters()
        return self._server_filters

    def update_aggregators(self) -> None:
        """Updates the locally stored list of available aggregators on the OpenTSDB server."""
        url = f"{self._complete_url}{Endpoints.AGGREGATORS}"
        self._server_aggregators = requests.get(url=url).json()

    @property
    def aggregators(self) -> list:
        """Returns the aggregators that are available on the OpenTSDB server.

        Returns:
            list: Aggregators that are available on the OpenTSDB server.
        """
        if not self._server_aggregators:
            self.update_aggregators()
        return self._server_aggregators

    def update_version(self) -> None:
        """Updates the locally stored information on the servers OpenTSDB version.

        URL: http://opentsdb.net/docs/build/html/api_http/version.html

        Example response:
        `{
            "timestamp": "1362712695",
            "host": "localhost",
            "repo": "/opt/opentsdb/build",
            "full_revision": "11c5eefd79f0c800b703ebd29c10e7f924c01572",
            "short_revision": "11c5eef",
            "user": "localuser",
            "repo_status": "MODIFIED",
            "version": "2.0.0"
        }`

        Returns:
            dict: Version of the set OpenTSDB server.
        """
        url = f"{self._complete_url}{Endpoints.VERSION}"
        self._server_version = requests.get(url=url).json()

    @property
    def version(self) -> dict:
        """Returns the version information of the set OpenTSDB server.

        Returns:
            dict: Version information of the set OpenTSDB server.
        """
        if not self._server_version:
            self.update_version()
        return self._server_version

    def update_config(self) -> None:
        """Updates the locally stored information on the servers OpenTSDB config.

        URL: http://opentsdb.net/docs/build/html/api_http/config.html

        Example response:
        `{
            "tsd.search.elasticsearch.tsmeta_type": "tsmetadata",
            "tsd.storage.flush_interval": "1000",
            "tsd.network.tcp_no_delay": "true",
            "tsd.search.tree.indexer.enable": "true",
            "tsd.http.staticroot": "/usr/local/opentsdb/staticroot/",
            "tsd.network.bind": "0.0.0.0",
            "tsd.network.worker_threads": "",
            "tsd.storage.hbase.zk_quorum": "localhost",
            "tsd.network.port": "4242",
            "tsd.rpcplugin.DummyRPCPlugin.port": "42",
            "tsd.search.elasticsearch.hosts": "localhost",
            "tsd.network.async_io": "true",
            "tsd.rtpublisher.plugin": "net.opentsdb.tsd.RabbitMQPublisher",
            "tsd.search.enableindexer": "false",
            "tsd.rtpublisher.rabbitmq.user": "guest",
            "tsd.search.enable": "false",
            "tsd.search.plugin": "net.opentsdb.search.ElasticSearch",
            "tsd.rtpublisher.rabbitmq.hosts": "localhost",
            "tsd.core.tree.enable_processing": "false",
            "tsd.stats.canonical": "true",
            "tsd.http.cachedir": "/tmp/opentsdb/",
            "tsd.http.request.max_chunk": "16384",
            "tsd.http.show_stack_trace": "true",
            "tsd.core.auto_create_metrics": "true",
            "tsd.storage.enable_compaction": "true",
            "tsd.rtpublisher.rabbitmq.pass": "guest",
            "tsd.core.meta.enable_tracking": "true",
            "tsd.mq.enable": "true",
            "tsd.rtpublisher.rabbitmq.vhost": "/",
            "tsd.storage.hbase.data_table": "tsdb",
            "tsd.storage.hbase.uid_table": "tsdb-uid",
            "tsd.http.request.enable_chunked": "true",
            "tsd.core.plugin_path": "/usr/local/opentsdb/plugins",
            "tsd.storage.hbase.zk_basedir": "/hbase",
            "tsd.rtpublisher.enable": "false",
            "tsd.rpcplugin.DummyRPCPlugin.hosts": "localhost",
            "tsd.storage.hbase.tree_table": "tsdb-tree",
            "tsd.network.keep_alive": "true",
            "tsd.network.reuse_address": "true",
            "tsd.rpc.plugins": "net.opentsdb.tsd.DummyRpcPlugin"
        }`
        """
        url = f"{self._complete_url}{Endpoints.CONFIG}"
        self._server_config = requests.get(url=url).json()

    @property
    def config(self) -> dict:
        """Returns the config information of the set OpenTSDB server.

        Returns:
            dict: Config information of the set OpenTSDB server.
        """
        if not self._server_config:
            self.update_config()
        return self._server_config

    def get(self) -> request.GetRequestBuilder:
        """Creates and returns a new GetRequestBuilder object.

        Returns:
            GetRequestBuilder: GetRequestBuilder object.
        """
        return request.GetRequestBuilder(self)

    def post(self) -> request.PostRequestBuilder:
        """Creates and returns a new PostRequestBuilder object.

        Returns:
            PostRequestBuilder: PostRequestBuilder object.
        """
        return request.PostRequestBuilder(self)

    def delete(self) -> request.DeleteRequestBuilder:
        """Creates and returns a new DeleteRequestBuilder object.

        Returns:
            DeleteRequestBuilder: DeleteRequestBuilder object.
        """
        return request.DeleteRequestBuilder(self)


if __name__ == "__main__":
    print("Hello OpenTSDB!")






