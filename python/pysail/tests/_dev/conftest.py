import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.import_plugin("pysail.testing.fixtures")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.celeborn")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.glue")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.hms")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.iceberg_rest")
    config.pluginmanager.import_plugin("pysail.testing._dev.containers.unity")
