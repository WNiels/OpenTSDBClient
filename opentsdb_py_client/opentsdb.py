import requests
from typing import Any, Callable, Dict, List, Tuple, Union


class Client():
    ENDPOINT_AGGREGATORS = '/api/aggregators'
    ENDPOINT_VERSION = '/api/version'
    ENDPOINT_CONFIG = '/api/config'
    ENDPOINT_FILTERS = '/api/config/filters'

    def __init__(self, url: str, port: int):
        """Initializes the OpenTSDB class.

        Args:
            url (str): OpenTSDB server url.
            port (int): OpenTSDB server port.
        """
        self._url = url
        self._port = port
        self._complete_url = f'{self._url}:{self._port}'
        self._server_config = {}
        self._server_version = {}
        self._server_filters = {}
        self._server_aggregators = []
        self._session = requests.Session()

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
        url = f'{self._complete_url}{self.ENDPOINT_FILTERS}'
        self._server_filters = requests.get(url=url).json()

    @property
    def filters(self) -> dict:
        if not self._server_filters:
            self.update_filters()
        return self._server_filters

    def update_aggregators(self) -> None:
        url = f'{self._complete_url}{self.ENDPOINT_AGGREGATORS}'
        self._server_aggregators = requests.get(url=url).json()

    @property
    def aggregators(self) -> list:
        if not self._server_aggregators:
            self.update_aggregators()
        return self._server_aggregators
    
    def update_version(self) -> None:
        """Updates the servers version information.

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
        url = f'{self._complete_url}{self.ENDPOINT_VERSION}'
        self._server_version = requests.get(url=url).json()

    @property
    def version(self) -> dict:
        if not self._server_version:
            self.update_version()
        return self._server_version
    
    def update_config(self) -> None:
        """Updates the locally stored server configuration.

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
        url = f'{self._complete_url}{self.ENDPOINT_CONFIG}'
        self._server_config = requests.get(url=url).json()

    @property
    def config(self) -> dict:
        if not self._server_config:
            self.update_config()
        return self._server_config

    def request(self) -> 'Request':
        """Returns a new Request object.

        Returns:
            Request: Request object.
        """
        return Request(self)

    def get(self) -> 'GetRequestBuilder':
        """Returns a new GetRequestBuilder object.

        Returns:
            GetRequestBuilder: GetRequestBuilder object.
        """
        return GetRequestBuilder(self)


def builder(func: Callable) -> Callable:
    """
    Decorator for wrapper "builder" functions.  These are functions on the Query class or other classes used for
    building queries which mutate the query and return self.  To make the build functions immutable, this decorator is
    used which will deepcopy the current instance.  This decorator will return the return value of the inner function
    or the new copy of the instance.  The inner function does not need to return self.
    """
    import copy

    def _copy(self, *args, **kwargs):
        self_copy = copy.copy(self)
        result = func(self_copy, *args, **kwargs)

        # Return self if the inner function returns None.  This way the inner function can return something
        # different (for example when creating joins, a different builder is returned).
        if result is None:
            return self_copy

        return result

    return _copy


class Request:
    def __init__(self, client: Client):
        self._client = client

    def get(self) -> "GetRequestBuilder":
        return GetRequestBuilder(client=self._client)

    def post(self) -> "PostRequestBuilder":
        return PostRequestBuilder(client=self._client)

    def delete(self) -> "DeleteRequestBuilder":
        return DeleteRequestBuilder(client=self._client)


class RequestBuilder:
    _BASE_QUERY_URL = "/api/query"

    def __init__(self, client: Client, verb: str, **parameters):
        self._client = client
        self._verb = verb
        self._parameters = parameters if parameters is not None else {}
        self._parameters.setdefault("queries", [])

    class Verb:
        GET = "GET"
        POST = "POST"
        DELETE = "DELETE"

    class Parameter:
        START = "start"
        END = "end"
        QUERIES = "queries"
        NO_ANNOTATIONS = "no_annotations"
        GLOBAL_ANNOTATIONS = "global_annotations"
        MS_RESOLUTION = "ms_resolution"
        SHOW_TSUIDS = "show_tsuids"
        SHOW_SUMMARY = "show_summary"
        SHOW_STATS = "show_stats"
        SHOW_QUERY = "show_query"
        DELETE = "delete"
        TIME_ZONE = "time_zone"
        USE_CALENDAR = "use_calendar"

    @property
    def client(self) -> Client:
        return self._client

    @builder
    def start(self, start: str) -> "RequestBuilder":
        self._parameters["start"] = start

    @builder
    def end(self, end: str) -> "RequestBuilder":
        self._parameters["end"] = end

    @builder
    def query(self, query: "QueryBuilder") -> "RequestBuilder":
        self._parameters["queries"].append(query)

    @builder
    def no_annotations(self, no_annotations: bool) -> "RequestBuilder":
        self._parameters["no_annotations"] = no_annotations

    @builder
    def global_annotations(self, global_annotations: bool) -> "RequestBuilder":
        self._parameters["global_annotations"] = global_annotations

    @builder
    def ms_resolution(self, ms_resolution: bool) -> "RequestBuilder":
        self._parameters["ms_resolution"] = ms_resolution

    @builder
    def show_tsuids(self, show_tsuids: bool) -> "RequestBuilder":
        self._parameters["show_tsuids"] = show_tsuids

    @builder
    def show_summary(self, show_summary: bool) -> "RequestBuilder":
        self._parameters["show_summary"] = show_summary

    @builder
    def show_stats(self, show_stats: bool) -> "RequestBuilder":
        self._parameters["show_stats"] = show_stats

    @builder
    def show_query(self, show_query: bool) -> "RequestBuilder":
        self._parameters["show_query"] = show_query

    @builder
    def delete(self, delete: bool) -> "RequestBuilder":
        self._parameters["delete"] = delete

    @builder
    def time_zone(self, time_zone: str) -> "RequestBuilder":
        self._parameters["time_zone"] = time_zone

    @builder
    def use_calendar(self, use_calendar: bool) -> "RequestBuilder":
        self._parameters["use_calendar"] = use_calendar

    @builder
    def add_queries(self, queries: List["QueryBuilder"]) -> "RequestBuilder":
        self._parameters["queries"].extend(queries)

    @builder
    def add_metric_query(self, metric: str, aggregator: str, downsample: str = None, rate: bool = None, rate_options: str = None,
                         explicit_tags: bool = None, filters: List["Filter"] = None, percentiles: List[float] = None, rollup_usage: str = None) -> "RequestBuilder":
        self._parameters["queries"].append(MetricQueryBuilder(metric=metric, aggregator=aggregator, downsample=downsample, rate=rate, rate_options=rate_options,
                                                              explicit_tags=explicit_tags, filters=filters, percentiles=percentiles, rollup_usage=rollup_usage))

    @builder
    def add_tsuids_query(self, metric: str, aggregator: str, downsample: str = None, rate: bool = None, rate_options: str = None,
                         explicit_tags: bool = None, filters: List["Filter"] = None, percentiles: List[float] = None, rollup_usage: str = None) -> "RequestBuilder":
        self._parameters["queries"].append(TSUIDQueryBuilder(
            metric=metric, aggregator=aggregator, downsample=downsample, rate=rate, rate_options=rate_options,
            explicit_tags=explicit_tags, filters=filters, percentiles=percentiles, rollup_usage=rollup_usage))

    def parameters(self) -> List[Tuple[str, Any]]:
        """Returns a list of tuples containing the parameters for the request.

        Returns:
            List[Tuple[str, Any]]: A list of tuples containing the parameters for the request.
        """
        params = []
        for k, v in self._parameters.items():
            if k == "queries" and len(v) > 0:
                params.extend(q.as_param() for q in v)
            else:
                params.append((k, v))
        return params

    def validate(self) -> None:
        # TODO: Impleement validation
        pass

    def build_request(self) -> requests.Request:
        return requests.Request(self._verb, self.client._complete_url + self._BASE_QUERY_URL, params=self.parameters())

    def run(self) -> requests.Response:
        self.validate()
        return self.build_request().prepare().send(self.client.session)

    def _parameter_string(self) -> str:
        param = "&".join(f'{p[0]}={p[1]}' for p in self.parameters())
        return param

    def __str__(self) -> str:
        return f"{self._verb} {self.client.complete_url}{self._BASE_QUERY_URL}?{self._parameter_string()}"

    def __repr__(self) -> str:
        pass


class GetRequestBuilder(RequestBuilder): # TODO: make start an explicite required parameter
    def __init__(self, client: Client, **parameters: Any):
        super().__init__(client, RequestBuilder.Verb.GET, **parameters)

    def validate(self) -> None:
        return super().validate()

    def __str__(self) -> str:
        return super().__str__()


class PostRequestBuilder(RequestBuilder): #FIXME: See GetRequestBuilder
    def __init__(self, client: Client, start: str = None, end: str = None, queries: List["QueryBuilder"] = None, no_annotations: bool = None, global_annotations: bool = None, ms_resolution: bool = None, show_tsuids: bool = None, show_summary: bool = None, show_stats: bool = None, show_query: bool = None, delete: bool = None, time_zone: str = None, use_calendar: bool = None):
        super().__init__(verb=RequestBuilder.Verb.POST)


class DeleteRequestBuilder(RequestBuilder): #FIXME: See GetRequestBuilder
    def __init__(self, client: Client, start: str = None, end: str = None, queries: List["QueryBuilder"] = None, no_annotations: bool = None, global_annotations: bool = None, ms_resolution: bool = None, show_tsuids: bool = None, show_summary: bool = None, show_stats: bool = None, show_query: bool = None, delete: bool = None, time_zone: str = None, use_calendar: bool = None):
        super().__init__(verb=RequestBuilder.Verb.DELETE)


class QueryBuilder:
    def __init__(self, aggregator: str = None, metric: str = None, rate: bool = None, rate_options: "RateOptions" = None, downsample: str = None, filters: List["Filter"] = None, explicit_tags: bool = None, percentiles: List[float] = None, rollup_usage: str = None):
        self._aggregator = aggregator
        self._metric = metric
        self._rate = rate
        self._rate_options = rate_options
        self._downsample = downsample
        self._filters = filters if filters is not None else []
        self._explicit_tags = explicit_tags
        self._percentiles = percentiles if percentiles is not None else []
        self._rollup_usage = rollup_usage

    def as_param(self) -> str:
        return (self.TYPE, str(self))

    def __str__(self) -> str:
        return "This method should be overridden by the subclass. Please use either MetricQueryBuilder or TSUIDQueryBuilder."


class MetricQueryBuilder(QueryBuilder):
    TYPE = "m"

    def __str__(self) -> str:
        """Gnerates the query string for the metric query

        m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]}]:][<down_sampler>:][percentiles\[<p1>, <pn>\]:][explicit_tags:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]

        Returns:
            str: The query string
        """
        string = f'{self._aggregator}:'

        # [rate[{counter[,<counter_max>[,<reset_value>]]}]:]
        if self._rate is not None:
            string += str(self._rate)
            if self._rate_options is not None:
                string += str(self._rate_options)
            string += ':'

        # [<down_sampler>:]
        if self._downsample is not None:
            string += f'{self._downsample}:'

        # [percentiles\[<p1>, <pn>\]:]
        if len(self._percentiles) > 0:
            string += f'percentiles[{",".join([str(p) for p in self._percentiles])}]:'

        # [explicit_tags:]
        if self._explicit_tags is not None:
            string += str(self._explicit_tags)

        # <metric_name>
        string += self._metric

        # [{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}]
        grouping_filters = [f for f in self._filters if f._group_by is True]
        if len(grouping_filters) > 0:
            string += '{'
            string += ','.join([str(f) for f in grouping_filters])
            string += '}'

        # [{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
        non_grouping_filters = [
            f for f in self._filters if f._group_by is False]
        if len(non_grouping_filters) > 0:
            string += '{'
            string += ','.join([str(f) for f in non_grouping_filters])
            string += '}'

        return string


class TSUIDQueryBuilder(QueryBuilder):
    TYPE = "tsuid"
    pass


class RateOptions:
    def __init__(self, counter: bool = None, counter_max: int = None, reset_value: int = None, drop_resets: bool = None):
        self._counter = counter
        self._counter_max = counter_max
        self._reset_value = reset_value
        self._drop_resets = drop_resets

    # [{counter[,<counter_max>[,<reset_value>]]}]
    def __str__(self) -> str:
        return f'\u007b{self._counter if self._counter is not None else ""},{self._counter_max if self._counter_max is not None else ""},{self._reset_value if self._reset_value is not None else ""}\u007d'


class Filter:
    def __init__(self, type: str = None, tagk: str = None, filter: str = None, group_by: bool = None):
        self._type = type
        self._tagk = tagk
        self._filter = filter
        self._group_by = group_by

    # <tag_name1>=<non grouping filter>
    def __str__(self) -> str:
        return f'{self._tagk}={self._type}({self._filter})'


if __name__ == "__main__":
    #http://lsx-kubemaster-1.informatik.uni-wuerzburg.de:31617/api/query?start=1546297200&end=1548975600&m=avg:1d-avg:temperature{app=we4bee,name=*,beehive=f9311d93-afae-48a2-afa1-371df04b66ec}
    client = Client(
        url='http://lsx-kubemaster-1.informatik.uni-wuerzburg.de', port=31617)
    q = MetricQueryBuilder(
        aggregator="avg",
        metric="temperature",
        downsample="1d-avg",
        filters=[Filter(type="wildcard", tagk="app", filter="*", group_by=True),
                 Filter(type="literal_or", tagk="dc", filter="lga,ord", group_by=False)],
    )
    rb = client.get().add_queries([q]).start(1546297200).end(1548975600)

    print(str(rb.client))

    request = rb.build_request()
    print(request.prepare().url)

    #print(RateOptions(counter=False, drop_resets=True, counter_max=100, reset_value=1000))