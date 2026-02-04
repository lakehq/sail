import os

import pytest

from pysail.spark import SparkConnectServer


def test_warning_on_config_addition(monkeypatch):
    # The application configuration will refuse to load if there are unknown
    # environment variables. But since the application configuration is already
    # loaded globally, the configuration loader would not run inside this test,
    # so this unknown environment variable is good enough for testing purposes here.
    key = "SAIL_UNKNOWN_KEY"
    assert key not in os.environ
    monkeypatch.setenv(key, "42")
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_config_deletion(monkeypatch):
    key = "SAIL_EXECUTION__DEFAULT_PARALLELISM"
    monkeypatch.delenv(key, raising=True)
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_config_modification(monkeypatch):
    key = "SAIL_EXECUTION__DEFAULT_PARALLELISM"
    monkeypatch.setenv(key, str(int(os.environ[key]) * 2))
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()
