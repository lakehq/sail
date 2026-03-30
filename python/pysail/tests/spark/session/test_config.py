import os

import pytest

from pysail.spark import SparkConnectServer


def test_warning_on_static_config_addition(monkeypatch):
    key = "SAIL_TELEMETRY__OTLP_ENDPOINT"
    assert key not in os.environ
    monkeypatch.setenv(key, "http://localhost:4317")
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_static_config_deletion(monkeypatch):
    key = "SAIL_RUNTIME__STACK_SIZE"
    monkeypatch.delenv(key, raising=True)
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_static_config_modification(monkeypatch):
    key = "SAIL_RUNTIME__STACK_SIZE"
    monkeypatch.setenv(key, str(int(os.environ[key]) * 2))
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()
