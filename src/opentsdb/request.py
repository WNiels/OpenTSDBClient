from opentsdb.client import Client
from opentsdb.query import Filter, MetricQueryBuilder, QueryBuilder, TSUIDQueryBuilder
from opentsdb.utils import Endpoints, Verbs, _builder


import requests


import logging
from typing import Any, List, Tuple


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