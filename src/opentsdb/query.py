import logging


from typing import List, Tuple
class RateOptions:
    def __init__(
        self,
        counter: bool = None,
        counter_max: int = None,
        reset_value: int = None,
        drop_resets: bool = None,
    ):
        self._counter = counter
        self._counter_max = counter_max
        self._reset_value = reset_value
        self._drop_resets = drop_resets

    # [{counter[,<counter_max>[,<reset_value>]]}]
    def build(self) -> str:
        ro = f'\u007b{self._counter if self._counter is not None else ""},{self._counter_max if self._counter_max is not None else ""},{self._reset_value if self._reset_value is not None else ""}\u007d'
        logging.debug(f"RateOptions: {ro}")
        return ro


class Filter:
    def __init__(
        self,
        type: str = None,
        tagk: str = None,
        filter: str = None,
        group_by: bool = None,
    ):
        self._type = type
        self._tagk = tagk
        self._filter = filter
        self._group_by = group_by

    # <tag_name1>=<non grouping filter>
    def build(self) -> str:
        f = f"{self._tagk}={self._type}({self._filter})"
        logging.debug(f"Filter: {f}")
        return f


class QueryBuilder:
    def __init__(
        self,
        aggregator: str = None,
        metric: str = None,
        rate: bool = None,
        rate_options: "RateOptions" = None,
        downsample: str = None,
        filters: List["Filter"] = None,
        explicit_tags: bool = None,
        percentiles: List[float] = None,
        rollup_usage: str = None,
    ):
        """Base QueryBuilder.
        Specific QueryBuilders should inherit from this class.
        Should not be used directly. Use MetricQueryBuilder or TSUIDQueryBuilder instead.
        Parameters set to None will not be included in the request. Thus invoking the default behaviour of the OpenTSDB Instance.
        Args:
            aggregator (str, optional): The name of an aggregation function to use. Defaults to None.
            metric (str, optional): The name of a metric stored in the system. Defaults to None.
            rate (bool, optional): Whether or not the data should be converted into deltas before returning. Defaults to None.
            rate_options (RateOptions, optional): Monotonically increasing counter handling options. Defaults to None.
            downsample (str, optional): An optional downsampling function to reduce the amount of data returned. Defaults to None.
            filters (List[&quot;Filter&quot;], optional): Filters the time series emitted in the results. Defaults to None.
            explicit_tags (bool, optional): Returns the series that include only the tag keys provided in the filters. Defaults to None.
            percentiles (List[float], optional): Fetches histogram data for the metric and computes the given list of percentiles on the data. Percentiles are floating point values from 0 to 100.. Defaults to None.
            rollup_usage (str, optional): An optional fallback mode when fetching rollup data. Defaults to None.
        """
        self._aggregator = aggregator
        self._metric = metric
        self._rate = rate
        self._rate_options = rate_options
        self._downsample = downsample
        self._filters = filters if filters is not None else []
        self._explicit_tags = explicit_tags
        self._percentiles = percentiles if percentiles is not None else []
        self._rollup_usage = rollup_usage

    def build(self) -> str:
        raise NotImplementedError("This method should be implemented by the subclass. And not be used directly.")


class MetricQueryBuilder(QueryBuilder):
    TYPE = "m"

    def build(self) -> Tuple[str, str]:
        """Gnerates the query string for the metric query
        m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]}]:][<down_sampler>:][percentiles\[<p1>, <pn>\]:][explicit_tags:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
        Returns:
            str: The query string
        """
        string = f"{self._aggregator}:"

        # [rate[{counter[,<counter_max>[,<reset_value>]]}]:]
        if self._rate is not None:
            string += str(self._rate)
            if self._rate_options is not None:
                string += self._rate_options.build()
            string += ":"

        # [<down_sampler>:]
        if self._downsample is not None:
            string += f"{self._downsample}:"

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
            string += "{"
            string += ",".join([f.build() for f in grouping_filters])
            string += "}"

        # [{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
        non_grouping_filters = [f for f in self._filters if f._group_by is False]
        if len(non_grouping_filters) > 0:
            string += "{"
            string += ",".join([f.build() for f in non_grouping_filters])
            string += "}"

        query = (self.TYPE, string)
        logging.debug(f"Metric Query: {query}")
        return query


class TSUIDQueryBuilder(QueryBuilder):
    TYPE = "tsuid"
    pass