"""Pytest fixtures for Glue catalog integration tests using Moto."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def remote(moto_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server with Glue catalog."""
    catalog_config = (
        f'[{{name="sail", type="glue", catalog_id="123456789012", region="us-east-1", endpoint_url="{moto_endpoint}"}}]'
    )
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": catalog_config,
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
        },
    ) as server:
        yield server.remote


@pytest.fixture(scope="module", autouse=True)
def _glue_test_database(spark: SparkSession) -> Generator[None, None, None]:
    """Create the default database used by Glue catalog tests."""
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    yield
    with contextlib.suppress(Exception):
        spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
