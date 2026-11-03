import pytest

from pysail.testing.spark.fixtures import (
    local_timezone,
    remote,
    session_timezone,
    spark,
    spark_doctest,
)

__all__ = ["local_timezone", "remote", "session_timezone", "spark", "spark_doctest"]


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.import_plugin("pysail.testing.fixtures")
    config.pluginmanager.import_plugin("pysail.testing.spark.doctest")
