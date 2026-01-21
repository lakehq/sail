from __future__ import annotations

import doctest
import os
import time
from typing import TYPE_CHECKING

import pytest
from _pytest.doctest import DoctestItem
from pyspark.sql import SparkSession

from pysail.tests.spark.server_manager import SERVER_MANAGER, collect_sail_env
from pysail.tests.spark.utils import SAIL_ONLY, is_jvm_spark


def pytest_configure(config):
    # Load all pytest-bdd step modules.
    config.pluginmanager.import_plugin("pysail.tests.spark.steps.file_tree")
    config.pluginmanager.import_plugin("pysail.tests.spark.steps.sql")
    config.pluginmanager.import_plugin("pysail.tests.spark.steps.plan")
    config.pluginmanager.import_plugin("pysail.tests.spark.steps.delta_log")
    config.addinivalue_line(
        "markers",
        "sail_env(**kwargs): configure environment variables for the Sail server (may trigger restart)",
    )
    # pytest-bdd turns feature tags into markers with the same name. Register the tags
    # we currently use to avoid PytestUnknownMarkWarning noise.
    config.addinivalue_line(
        "markers",
        "sail_env_SAIL_OPTIMIZER__ENABLE_JOIN_REORDER__true: enable join reorder for the Sail server",
    )
    config.addinivalue_line(
        "markers",
        "sail_env_SAIL_MODE__local-cluster: run the Sail server in local-cluster mode",
    )


if TYPE_CHECKING:
    import pyspark.sql.connect.session


@pytest.fixture(scope="session", autouse=True)
def sail_default_parallelism():
    """Sets the default parallelism to a fixed value regardless of the
    number of CPU cores to ensure deterministic test results, especially for
    snapshot tests involving execution plans.
    """
    os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"


@pytest.fixture(scope="session", autouse=True)
def shutdown_sail_server():
    yield
    SERVER_MANAGER.shutdown()


def _find_module_node(node):
    cur = node
    while cur is not None and cur.__class__.__name__ != "Module":
        cur = getattr(cur, "parent", None)
    return cur


def _get_module_sail_env_config(request) -> dict[str, str]:
    # 1) explicit module-level markers (e.g. pytestmark / module decorators)
    config = collect_sail_env(request.node)
    # 2) collection-time aggregated config (e.g. from pytest-bdd feature tags)
    aggregated = getattr(request.node, "sail_env_config", None)
    if aggregated:
        config.update(aggregated)
    return config


@pytest.fixture(scope="module")
def remote(request):
    """Creates a Spark Connect server if there is not one already running
    whose address is set in the `SPARK_REMOTE` environment variable.

    :yields: The remote address of the Spark Connect server to connect to.
    """
    if r := os.environ.get("SPARK_REMOTE"):
        yield r
    else:
        desired_env = _get_module_sail_env_config(request)
        yield SERVER_MANAGER.get_server_address(desired_env)


@pytest.fixture(scope="module")
def spark(remote):
    """Create and configure a Spark Session to be used in the tests.
    After the tests are finished, the Spark Session is stopped.

    :param remote: The remote address of the Spark Connect server to connect to.
    :yields: A Spark Session configured for the tests.
    """
    spark = SparkSession.builder.remote(remote).getOrCreate()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    yield spark
    spark.stop()


def configure_spark_session(session):
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    # Enable Arrow to avoid data type errors when creating Spark DataFrame from Pandas.
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def patch_spark_connect_session(session: pyspark.sql.connect.session.SparkSession):
    """
    Patch the Spark Connect session to avoid deadlock when closing the session.
    """
    f = session._client.close  # noqa: SLF001

    def close():
        if session._client._closed:  # noqa: SLF001
            return
        return f()

    session._client.close = close  # noqa: SLF001


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace, spark):
    # The Spark session is scoped to each module, so that the registered
    # temporary views and UDFs do not interfere with each other.
    doctest_namespace["spark"] = spark


@pytest.fixture
def session_timezone(spark, request):
    tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", request.param)
    yield
    spark.conf.set("spark.sql.session.timeZone", tz)


@pytest.fixture
def local_timezone(request):
    tz = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if tz is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = tz
    time.tzset()


def pytest_collection_modifyitems(session, config, items):  # noqa: ARG001
    # Aggregate per-module sail env configs from item-level markers/tags.
    # This enables using pytest-bdd feature tags (which attach to scenario items)
    # while keeping module-scoped fixtures (required by other test modules).
    per_module: dict[object, dict[str, str]] = {}

    for item in items:
        module_node = _find_module_node(item)
        if module_node is None:
            continue

        item_cfg = collect_sail_env(item)
        if not item_cfg:
            continue

        existing = per_module.get(module_node)
        if existing is None:
            per_module[module_node] = item_cfg
        elif existing != item_cfg:
            module_id = getattr(module_node, "nodeid", str(module_node))
            message = (
                "Multiple different `sail_env` configurations were found within the same test module.\n"
                "Module: {module}\n"
                "Config A: {a}\n"
                "Config B: {b}\n"
                "Please split these tests into separate modules, or make their `sail_env` consistent."
            ).format(module=module_id, a=existing, b=item_cfg)
            raise pytest.UsageError(message)  # noqa: TRY003

    for module_node, cfg in per_module.items():
        module_node.sail_env_config = cfg

    if is_jvm_spark():
        for item in items:
            if isinstance(item, DoctestItem):
                for example in item.dtest.examples:
                    if example.options.get(SAIL_ONLY):
                        example.options[doctest.SKIP] = True
