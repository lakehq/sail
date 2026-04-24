"""Shared fixtures for catalog integration tests.

Each sub-directory (glue/, iceberg_rest/, unity/) provides fixtures that
spin up the relevant infrastructure containers and a dedicated Sail server.

These tests are **deselected by default**. To run them, pass
``--run-catalog-integration`` explicitly::

    hatch run pytest --run-catalog-integration
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from pysail.spark import SparkConnectServer


def start_sail_server(
    catalog_list: str,
    extra_env: dict[str, str] | None = None,
) -> tuple[SparkConnectServer, str, dict[str, str | None]]:
    """Start a Sail Spark Connect server with the given catalog configuration.

    Returns ``(server, remote_url, saved_env)`` where *saved_env* contains
    the original environment values so the caller can restore them later.
    """
    # Deferred import so pysail._native is not loaded at module collection time.
    from pysail.spark import SparkConnectServer

    env_vars: dict[str, str] = {
        "SAIL_CATALOG__LIST": catalog_list,
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
        **(extra_env or {}),
    }

    # Save and override
    saved: dict[str, str | None] = {}
    for key, value in env_vars.items():
        saved[key] = os.environ.get(key)
        os.environ[key] = value

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    _, port = server.listening_address
    return server, f"sc://localhost:{port}", saved


def stop_sail_server(
    server: SparkConnectServer,
    saved_env: dict[str, str | None],
) -> None:
    """Stop a Sail server and restore the environment."""
    server.stop()
    for key, old_value in saved_env.items():
        if old_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old_value


def create_spark_session(remote: str, app_name: str = "catalog_test") -> SparkSession:
    """Create a Spark session connected to the given remote."""
    # Deferred imports so pysail._native is not loaded at module collection time.
    from pyspark.sql import SparkSession

    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    spark = SparkSession.builder.remote(remote).appName(app_name).getOrCreate()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    return spark


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the CLI flag that opts in to catalog integration tests."""
    parser.addoption(
        "--run-catalog-integration",
        action="store_true",
        default=False,
        help="Run catalog integration tests (deselected by default).",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-mark catalog_integration tests and deselect them unless explicitly opted in.

    Catalog integration tests are skipped by default. They are only collected when the
    user passes ``--run-catalog-integration`` or an explicit marker expression that
    positively selects the ``catalog_integration`` marker (e.g. ``-m catalog_integration``).
    Expressions such as ``-m 'not catalog_integration'`` or ``-m 'not slow'`` will
    deselect catalog integration tests.
    """
    this_dir = os.path.dirname(os.path.abspath(__file__))
    opted_in = bool(config.getoption("--run-catalog-integration", default=False))

    remaining: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        if str(item.fspath).startswith(this_dir):
            item.add_marker(pytest.mark.catalog_integration)
            if not opted_in:
                deselected.append(item)
                continue
        remaining.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = remaining
