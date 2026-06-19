"""Shared fixtures for catalog integration tests.

Each sub-directory (glue/, iceberg_rest/, unity/) provides fixtures that
spin up the relevant infrastructure containers and a dedicated Sail server.

These tests are marked with ``@pytest.mark.integration`` and are
**deselected by default**. To run them, pass ``-m integration``::

    hatch run pytest -m integration
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from pysail.spark import SparkConnectServer

CATALOG_SPARK_FIXTURES = {
    "glue": "glue_spark",
    "iceberg_rest": "iceberg_spark",
    "unity": "unity_spark",
}


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


def create_spark_session(
    remote: str,
    app_name: str = "catalog_test",
    *,
    new_session: bool = False,
) -> SparkSession:
    """Create a Spark session connected to the given remote."""
    # Deferred imports so pysail._native is not loaded at module collection time.
    from pyspark.sql import SparkSession

    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    builder = SparkSession.builder.remote(remote).appName(app_name)
    spark = builder.create() if new_session else builder.getOrCreate()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    return spark


@pytest.fixture(scope="module")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    """Dispatch the shared BDD ``spark`` fixture to the matching catalog backend.

    When tests are collected from an installed package outside pytest's rootdir,
    nested conftest fixtures can become visible globally. In that case this
    fixture may be used by non-catalog tests too, so it must fall back to the
    default Spark session unless the requesting test is under catalog_integration.
    """
    test_path = Path(str(request.node.fspath)).resolve()
    this_dir = Path(__file__).parent.resolve()
    for name, fixture_name in CATALOG_SPARK_FIXTURES.items():
        if test_path.is_relative_to(this_dir / name):
            return request.getfixturevalue(fixture_name)
    return request.getfixturevalue("default_spark")
