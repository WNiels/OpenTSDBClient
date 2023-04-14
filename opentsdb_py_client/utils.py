from opentsdb_py_client.opentsdb import Client
from typing import Callable, List, Union


def builder(func: Callable) -> Callable:
    """
    Decorator for wrapper "builder" functions.  These are functions on the Query class or other classes used for
    building queries which mutate the query and return self.  To make the build functions immutable, this decorator is
    used which will deepcopy the current instance.  This decorator will return the return value of the inner function
    or the new copy of the instance.  The inner function does not need to return self.
    """
    import copy

    def _copy(self, *args, **kwargs):
        self_copy = copy.copy(self) if getattr(
            self, "immutable", True) else self
        result = func(self_copy, *args, **kwargs)

        # Return self if the inner function returns None.  This way the inner function can return something
        # different (for example when creating joins, a different builder is returned).
        if result is None:
            return self_copy

        return result

    return _copy


class Query:
    def __init__(self, client: Client):
        self.__client = client

    def get(self) -> "GetQueryBuilder":
        return GetQueryBuilder(client=self.__client)

    def post(self) -> "PostQueryBuilder":
        return PostQueryBuilder(client=self.__client)

    def delete(self) -> "DeleteQueryBuilder":
        return DeleteQueryBuilder(client=self.__client)


class QueryBuilder:
    def __init__(self, client: Client, verb: str, start: str = None, end: str = None, queries: List["SubQueryBuilder"] = None, no_annotations: bool = None,
                 global_annotations: bool = None, ms_resolution: bool = None, show_tsuids: bool = None, show_summary: bool = None,
                 show_stats: bool = None, show_query: bool = None, delete: bool = None, time_zone: str = None, use_calendar: bool = None
                 ):
        self.__client = client
        self.__verb = verb
        self.__start = start
        self.__end = end
        self.__queries = queries if queries is not None else []
        self.__no_annotations = no_annotations
        self.__global_annotations = global_annotations
        self.__ms_resolution = ms_resolution
        self.__show_tsuids = show_tsuids
        self.__show_summary = show_summary
        self.__show_stats = show_stats
        self.__show_query = show_query
        self.__delete = delete
        self.__time_zone = time_zone
        self.__use_calendar = use_calendar

    # create properties for all the parameters
    @property
    def client(self) -> Client:
        return self.__client

    @property
    def verb(self) -> str:
        return self.__verb

    @property
    def start(self) -> str:
        return self.__start

    @property
    def end(self) -> str:
        return self.__end

    @property
    def queries(self) -> List["SubQueryBuilder"]:
        return self.__queries

    @property
    def no_annotations(self) -> bool:
        return self.__no_annotations

    @property
    def global_annotations(self) -> bool:
        return self.__global_annotations

    @property
    def ms_resolution(self) -> bool:
        return self.__ms_resolution

    @property
    def show_tsuids(self) -> bool:
        return self.__show_tsuids

    @property
    def show_summary(self) -> bool:
        return self.__show_summary

    @property
    def show_stats(self) -> bool:
        return self.__show_stats

    @property
    def show_query(self) -> bool:
        return self.__show_query

    @property
    def delete(self) -> bool:
        return self.__delete

    @property
    def time_zone(self) -> str:
        return self.__time_zone

    @property
    def use_calendar(self) -> bool:
        return self.__use_calendar

    def update_request(self, **kwargs):
        """Updates the request parameters of the query with the given parameters.

        Link: http://opentsdb.net/docs/build/html/api_http/query/index.html#requests

        Args:
            **kwargs: Request parameters.

        Returns:
            Query: self
        """
        self.__request.update(kwargs)
        return self

    def add_metric_query(self, metric_query: Metric_Query):
        """Adds a metric subquery to the query.

        Args:
            metric_query (Metric_Query): Metric subquery.

        Returns:
            Query: self
        """
        self.__metric_queries.append(metric_query)
        return self

    def add_tsuid_query(self, tsuid_query: TSUID_Query):
        """Adds a TSUID subquery to the query.

        Args:
            tsuid_query (TSUID_Query): TSUID subquery.

        Returns:
            Query: self
        """
        self.__tsuid_queries.append(tsuid_query)
        return self

    def run(self) -> dict:
        """Runs the query.

        Raises:
            Exception: Verb not set or invalid. Must be either "GET", "POST" or "DELETE".
            Exception: Start time not set. "start" is a required parameter.
            Exception: No queries added. At least one query is required.

        Returns:
            dict: Response of the query.
        """
        if self.__verb not in ['GET', 'POST', 'DELETE']:
            raise Exception(
                'Verb not set or invalid. Must be either "GET", "POST" or "DELETE".')
        if not 'start' in self.__request:
            raise Exception(
                'Start time not set. "start" is a required parameter.')
        if len(self.__metric_queries) == 0 and len(self.__tsuid_queries) == 0:
            raise Exception(
                'No queries added. At least one query is required.')
        pass


class GetQueryBuilder(QueryBuilder):
    pass


class PostQueryBuilder(QueryBuilder):
    pass


class DeleteQueryBuilder(QueryBuilder):
    pass


class SubQueryBuilder:
    pass


class MetricQueryBuilder(SubQueryBuilder):
    pass


class TSUIDQueryBuilder(SubQueryBuilder):
    pass
