"""Pytest fixtures for Glue catalog integration tests using Moto."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def moto_container() -> Generator[DockerContainer, None, None]:
    """Start a Moto container for mocking AWS Glue."""
    container = DockerContainer("motoserver/moto:5.1.22")
    container.with_exposed_ports(5000)
    container.start()
    wait_for_logs(container, "Running on", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def moto_endpoint(moto_container: DockerContainer) -> str:
    """Get the Moto endpoint URL."""
    host = moto_container.get_container_host_ip()
    port = moto_container.get_exposed_port(5000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def remote(moto_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server with Glue catalog."""
    catalog_config = f'[{{name="sail", type="glue", region="us-east-1", endpoint_url="{moto_endpoint}"}}]'
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
