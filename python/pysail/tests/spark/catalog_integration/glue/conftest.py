"""Pytest fixtures for Glue catalog integration tests using Moto."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

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
def glue_spark(moto_endpoint: str) -> Generator[SparkSession, None, None]:
    """Start Sail server with Glue catalog and create a Spark session."""
    catalog_config = f'[{{name="sail", type="glue", region="us-east-1", endpoint_url="{moto_endpoint}"}}]'
    server, remote, saved_env = start_sail_server(
        catalog_list=catalog_config,
        extra_env={
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
        },
    )
    spark = create_spark_session(remote, "glue_catalog_test")
    yield spark
    with contextlib.suppress(Exception):
        spark.stop()
    stop_sail_server(server, saved_env)


@pytest.fixture(scope="module")
def spark(glue_spark: SparkSession) -> SparkSession:
    """Alias for glue_spark, used by BDD step definitions."""
    return glue_spark
