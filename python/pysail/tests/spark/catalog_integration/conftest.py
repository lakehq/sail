"""Shared fixtures for catalog integration tests.

These tests validate catalog providers by running SQL through Sail's
Spark Connect interface. Each sub-directory (glue/, iceberg_rest/, unity/)
provides fixtures that spin up the relevant infrastructure containers and
a dedicated Sail server.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

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
