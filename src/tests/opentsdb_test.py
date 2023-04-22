import pytest
import requests


def f():
    raise SystemExit(1)

def test_mytest():
    with pytest.raises(SystemExit):
        f()