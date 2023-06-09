import requests
import logging

from typing import Any, List, Tuple

from .utils import Endpoints
from opentsdb2py.query import (
    Filter,
    MetricQueryBuilder,
    QueryBuilder,
    TSUIDQueryBuilder,
)
from opentsdb2py.utils import Endpoints, Verbs, _builder

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

    def get(self) -> "GetRequestBuilder":
        """Creates and returns a new GetRequestBuilder object.

        Returns:
            GetRequestBuilder: GetRequestBuilder object.
        """
        return GetRequestBuilder(self)

    def post(self) -> "PostRequestBuilder":
        """Creates and returns a new PostRequestBuilder object.

        Returns:
            PostRequestBuilder: PostRequestBuilder object.
        """
        return PostRequestBuilder(self)

    def delete(self) -> "DeleteRequestBuilder":
        """Creates and returns a new DeleteRequestBuilder object.

        Returns:
            DeleteRequestBuilder: DeleteRequestBuilder object.
        """
        return DeleteRequestBuilder(self)


class RequestBuilder:
    def __init__(self, client: Client, verb: str, **parameters):
        """Base object for all request builders. This object should not be used directly.
        Args:
            client (Client): Client object.
            verb (str): HTTP verb to use for the request. Either of "GET", "POST" or "DELETE".
            **parameters (dict): Additional parameters to pass to the request. Parameters must be supported by the given OpenTSDB server.
        """
        self._client = client
        self._verb = verb
        self._parameters = parameters if parameters is not None else {}
        self._parameters.setdefault("queries", [])

    @property
    def client(self) -> Client:
        """Returns the Client object.
        Returns:
            Client: Client object.
        """
        return self._client

    @_builder
    def start(self, start: str) -> "RequestBuilder":
        """Sets the start parameter for the request.
        Immutable functiuon.
        Args:
            start (str): Start time of the request. Must be an epoch timestamp.
        Returns:
            RequestBuilder: New RequestBuilder object.
        """
        self._parameters["start"] = start

    @_builder
    def end(self, end: str) -> "RequestBuilder":
        """end parameter for the request.
        Immutable functiuon.
        Args:
            end (str): End time of the request. Must be an epoch timestamp.
        Returns:
            RequestBuilder: New RequestBuilder object with the end parameter set.
        """
        self._parameters["end"] = end

    @_builder
    def query(self, query: "QueryBuilder") -> "RequestBuilder":
        """query parameter for the request.
        Immutable functiuon.
        Args:
            query (QueryBuilder): QueryBuilder object.
        Returns:
            RequestBuilder: New RequestBuilder object with the query parameter set.
        """
        self._parameters["queries"].append(query)

    @_builder
    def no_annotations(self, no_annotations: bool) -> "RequestBuilder":
        """no_annotations parameter for the request.
        Immutable functiuon.
        Args:
            no_annotations (bool): If set to True, annotations will not be returned.
        Returns:
            RequestBuilder: New RequestBuilder object with the no_annotations parameter set.
        """
        self._parameters["no_annotations"] = no_annotations

    @_builder
    def global_annotations(self, global_annotations: bool) -> "RequestBuilder":
        self._parameters["global_annotations"] = global_annotations

    @_builder
    def ms_resolution(self, ms_resolution: bool) -> "RequestBuilder":
        self._parameters["ms_resolution"] = ms_resolution

    @_builder
    def show_tsuids(self, show_tsuids: bool) -> "RequestBuilder":
        self._parameters["show_tsuids"] = show_tsuids

    @_builder
    def show_summary(self, show_summary: bool) -> "RequestBuilder":
        self._parameters["show_summary"] = show_summary

    @_builder
    def show_stats(self, show_stats: bool) -> "RequestBuilder":
        self._parameters["show_stats"] = show_stats

    @_builder
    def show_query(self, show_query: bool) -> "RequestBuilder":
        self._parameters["show_query"] = show_query

    @_builder
    def delete(self, delete: bool) -> "RequestBuilder":
        self._parameters["delete"] = delete

    @_builder
    def time_zone(self, time_zone: str) -> "RequestBuilder":
        self._parameters["time_zone"] = time_zone

    @_builder
    def use_calendar(self, use_calendar: bool) -> "RequestBuilder":
        self._parameters["use_calendar"] = use_calendar

    @_builder
    def add_queries(self, queries: List["QueryBuilder"]) -> "RequestBuilder":
        self._parameters["queries"].extend(queries)

    @_builder
    def add_metric_query(
        self,
        metric: str,
        aggregator: str,
        downsample: str = None,
        rate: bool = None,
        rate_options: str = None,
        explicit_tags: bool = None,
        filters: List["Filter"] = None,
        percentiles: List[float] = None,
        rollup_usage: str = None,
    ) -> "RequestBuilder":
        self._parameters["queries"].append(
            MetricQueryBuilder(
                metric=metric,
                aggregator=aggregator,
                downsample=downsample,
                rate=rate,
                rate_options=rate_options,
                explicit_tags=explicit_tags,
                filters=filters,
                percentiles=percentiles,
                rollup_usage=rollup_usage,
            )
        )

    @_builder
    def add_tsuids_query(
        self,
        metric: str,
        aggregator: str,
        downsample: str = None,
        rate: bool = None,
        rate_options: str = None,
        explicit_tags: bool = None,
        filters: List["Filter"] = None,
        percentiles: List[float] = None,
        rollup_usage: str = None,
    ) -> "RequestBuilder":
        self._parameters["queries"].append(
            TSUIDQueryBuilder(
                metric=metric,
                aggregator=aggregator,
                downsample=downsample,
                rate=rate,
                rate_options=rate_options,
                explicit_tags=explicit_tags,
                filters=filters,
                percentiles=percentiles,
                rollup_usage=rollup_usage,
            )
        )

    def _build_parameter_list(self) -> List[Tuple[str, Any]]:
        """Returns a list of tuples containing the parameters for the request.
        Returns:
            List[Tuple[str, Any]]: A list of tuples containing the parameters for the request.
        """
        params = []
        for k, v in self._parameters.items():
            if k == "queries" and len(v) > 0:
                params.extend(q.build() for q in v)
            else:
                params.append((k, v))
        logging.debug(f"Built parameter list: {params}")
        return params

    def build(self) -> requests.Request:
        """Builds a requests.Request object.
        Is done automatically when calling run.
        Might be used to create the request without sending it.
        Returns:
            requests.Request: The request object.
        """
        request = requests.Request(
            self._verb,
            self.client._complete_url + Endpoints.QUERY,
            params=self._build_parameter_list(),
        )
        logging.debug(f"Built request: {request}")
        return request

    def run(self) -> requests.Response:
        """Builds and runs a requests.Request object.
        Returns:
            requests.Response: The response object.
        """
        with self.client._session as s:
            pr = self.build().prepare()
            logging.debug(f"Sending request: {pr}")
            return s.send(pr)


class GetRequestBuilder(
    RequestBuilder
):  # TODO: make start an explicite required parameter
    def __init__(self, client: Client, **parameters: Any):
        """RequestBuilder for GET requests.
        Used to specifically build GET requests.
        Args:
            client (Client): The client to use.
        """
        super().__init__(client, Verbs.GET, **parameters)


class PostRequestBuilder(RequestBuilder):  # FIXME: See GetRequestBuilder
    def __init__(
        self,
        client: Client,
        start: str = None,
        end: str = None,
        queries: List["QueryBuilder"] = None,
        no_annotations: bool = None,
        global_annotations: bool = None,
        ms_resolution: bool = None,
        show_tsuids: bool = None,
        show_summary: bool = None,
        show_stats: bool = None,
        show_query: bool = None,
        delete: bool = None,
        time_zone: str = None,
        use_calendar: bool = None,
    ):
        super().__init__(verb=RequestBuilder.Verb.POST)


class DeleteRequestBuilder(RequestBuilder):  # FIXME: See GetRequestBuilder
    def __init__(
        self,
        client: Client,
        start: str = None,
        end: str = None,
        queries: List["QueryBuilder"] = None,
        no_annotations: bool = None,
        global_annotations: bool = None,
        ms_resolution: bool = None,
        show_tsuids: bool = None,
        show_summary: bool = None,
        show_stats: bool = None,
        show_query: bool = None,
        delete: bool = None,
        time_zone: str = None,
        use_calendar: bool = None,
    ):
        super().__init__(verb=RequestBuilder.Verb.DELETE)




if __name__ == "__main__":
    print("Hello OpenTSDB!")
