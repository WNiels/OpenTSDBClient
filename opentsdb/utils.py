import copy
from typing import Callable


class RequestParameters:
    """Request parameters for the OpenTSDB API."""
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


class Verbs:
    """HTTP verbs for use in requests."""
    GET = "GET" # Used to retrieve data from OpenTSDB. Overrides can be provided to modify content. Note: Requests via GET can only use query string parameters; see the note below.
    POST = "POST" # Used to update or create an object in OpenTSDB using the content body from the request. Will use a formatter to parse the content body
    DELETE = "DELETE" # Used to delete data from the system
    PUT = "PUT" # Replace an entire object in the system with the provided content


class Endpoints:
    """Endpoints for the OpenTSDB API."""
    QUERY = "/api/query"
    AGGREGATORS = "/api/aggregators"
    VERSION = "/api/version"
    CONFIG = "/api/config"
    FILTERS = "/api/config/filters"


class Filters:
    """Some common filters for use in queries."""
    # REGEXP
    # Examples:
    #   host=regexp(.*)  {"type":"regexp","tagk":"host","filter":".*","groupBy":false}
    # Description:
    #   Provides full, POSIX compliant regular expression using the built in Java Pattern class.
    #   Note that an expression containing curly braces {} will not parse properly in URLs.
    #   If the pattern is not a valid regular expression then an exception will be raised.
    REGEXP = "regexp"

    # IWILDCARD
    # Examples:
    #   host=iwildcard(web*),  host=iwildcard(web*.tsdb.net)  {"type":"iwildcard","tagk":"host","filter":"web*.tsdb.net","groupBy":false}
    # Description:
    #   Performs pre, post and in-fix glob matching of values.
    #   The globs are case insensitive and multiple wildcards can be used. The wildcard character is the * (asterisk).
    #   Case insensitivity is achieved by dropping all values to lower case. At least one wildcard must be present in the filter value.
    #   A wildcard by itself can be used as well to match on any value for the tag key.
    IWILDCARD = "iwildcard"

    # ILITERAL_OR
    # Examples:
    #   host=iliteral_or(web01),  host=iliteral_or(web01|web02|web03)  {"type":"iliteral_or","tagk":"host","filter":"web01|web02|web03","groupBy":false}
    # Description:
    #   Accepts one or more exact values and matches if the series contains any of them. 
    #   Multiple values can be included and must be separated by the | (pipe) character.
    #   The filter is case insensitive and will not allow characters that TSDB does not allow at write time.
    ILITERAL_OR = "iliteral_or"

    # NOT_ILITERAL_OR
    # Examples:
    #   host=not_iliteral_or(web01),  host=not_iliteral_or(web01|web02|web03)  {"type":"not_iliteral_or","tagk":"host","filter":"web01|web02|web03","groupBy":false}
    # Description:
    #   Accepts one or more exact values and matches if the series does NOT contain any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case insensitive and will not allow characters that TSDB does not allow at write time.
    NOT_ILITERAL_OR = "not_iliteral_or"

    # NOT_LITERAL_OR
    # Examples:
    #   host=not_literal_or(web01),  host=not_literal_or(web01|web02|web03)  {"type":"not_literal_or","tagk":"host","filter":"web01|web02|web03","groupBy":false}
    # Description:
    #   Accepts one or more exact values and matches if the series does NOT contain any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case sensitive and will not allow characters that TSDB does not allow at write time.
    NOT_LITERAL_OR = "not_literal_or"

    # LITERAL_OR
    # Examples:
    #   host=literal_or(web01),  host=literal_or(web01|web02|web03)  {"type":"literal_or","tagk":"host","filter":"web01|web02|web03","groupBy":false}
    # Description:
    #   Accepts one or more exact values and matches if the series contains any of them. 
    #   Multiple values can be included and must be separated by the | (pipe) character. 
    #   The filter is case sensitive and will not allow characters that TSDB does not allow at write time.
    LITERAL_OR = "literal_or"

    # WILDCARD
    # Examples:
    #   host=host=wildcard(web*),  host=wildcard(web*.tsdb.net)  {"type":"wildcard","tagk":"host","filter":"web*.tsdb.net","groupBy":false}
    # Description:
    #   Performs pre, post and in-fix glob matching of values. The globs are case sensitive and multiple wildcards can be used. The wildcard character is the * (asterisk). At least one wildcard must be present in the filter value. A wildcard by itself can be used as well to match on any value for the tag key.
    WILDCARD = "wildcard"


def _builder(func: Callable) -> Callable:
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


class Response:
    pass


if __name__ == "__main__":
    print("Hello OpenTSDB!")
