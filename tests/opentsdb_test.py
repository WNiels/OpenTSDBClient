import pytest
from ..opentsdb.request import RequestBuilder


def f():
    raise SystemExit(1)

def test_mytest():
    with pytest.raises(SystemExit):
        f()