import requests


class Client():
    def __init__(self, url: str, port: int):
        """Initializes the OpenTSDB class.

        Args:
            url (str): OpenTSDB server url.
            port (int): OpenTSDB server port.
        """
        self.__url = url
        self.__port = port
        self.__complete_url = f'{self.__url}:{self.__port}'

    def aggregators(self) -> list:
        """Returns a list of all available aggregators.
        The list is requested from the set OpenTSDB server.

        URL: http://opentsdb.net/docs/build/html/api_http/aggregators.html

        Example response:
        `[
            "min",
            "sum",
            "max",
            "avg",
            "dev"
        ]`

        Returns:
            list: List of all available aggregators.
        """
        url = f'{self.__complete_url}/api/aggregators'
        return requests.get(url=url).json()

    def version(self) -> dict:
        """Returns the version of the set OpenTSDB server.

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
        url = f'{self.__complete_url}/api/version'
        return requests.get(url=url).json()

    def config(self) -> dict:
        """Returns the configuration of the set OpenTSDB server.

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
        Returns:
            dict: Configuration of the set OpenTSDB server.
        """
        url = f'{self.__complete_url}/api/config'
        return requests.get(url=url).json()

    def query(self) -> Query:
        """Returns a Query object.

        Returns:
            Query: Query object.
        """
        return Query(self)


if __name__ == '__main__':
    client = Client(url='http://localhost', port=4242)
    client.query().set_verb('GET').set_request()
    pass
