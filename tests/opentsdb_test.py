from opentsdb_py_client import opentsdb
import unittest

class TestRequestBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.request_builder_valid = opentsdb.RequestBuilder()

class TestQueryBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.query_builder_valid = opentsdb.QueryBuilder()

class TestRateOptions(unittest.TestCase):
    def setUp(self) -> None:
        self.rate_options_valid = opentsdb.RateOptions(counter=False, drop_resets=True, counter_max=100, reset_value=1000)
        self.rate_options_defaults_valid = opentsdb.RateOptions(drop_resets=True, reset_value=1000)

    def test_all_values_given(self):
        self.assertEqual(self.rate_options_valid.__str__(), "{False,100,1000}")
        self.assertEqual(self.rate_options_defaults_valid.__str__(), "{,,1000}")