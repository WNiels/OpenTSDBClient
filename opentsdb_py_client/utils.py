from opentsdb_py_client.opentsdb import Client


class Metric_Query():
    pass


class TSUID_Query():
    pass


class Query():
    def __init__(self, client: Client):
        self.__client = client
        self.__verb = None
        self.__request = {}
        self.__metric_queries = []
        self.__tsuid_queries = []

    def set_verb(self, verb: str):
        """Sets the query method.

        Link: http://opentsdb.net/docs/build/html/api_http/query/index.html#verbs

        Args:
            verb (str): Either 'GET', 'POST' or 'DELETE'.

        Returns:
            Query: self
        """
        self.__verb = verb
        return self

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
