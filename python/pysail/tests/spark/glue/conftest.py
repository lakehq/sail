"""Pytest fixtures for Glue catalog tests with moto."""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.spark import SparkConnectServer
from pysail.tests.spark.conftest import configure_spark_session, patch_spark_connect_session

if TYPE_CHECKING:
    from collections.abc import Generator


# Override the spark_doctest autouse fixture from parent conftest
# to prevent it from starting the default memory catalog server
@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace, glue_spark):
    doctest_namespace["spark"] = glue_spark


@pytest.fixture(scope="module")
def moto_container() -> Generator[DockerContainer, None, None]:
    """Start a moto container for mocking AWS Glue."""
    container = DockerContainer("motoserver/moto:latest")
    container.with_exposed_ports(5000)
    container.start()
    wait_for_logs(container, "Running on", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def moto_endpoint(moto_container: DockerContainer) -> str:
    """Get the moto endpoint URL."""
    host = moto_container.get_container_host_ip()
    port = moto_container.get_exposed_port(5000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def glue_remote(moto_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server configured with Glue catalog as the default."""
    # Configure Sail with Glue as the default catalog (TOML inline table format)
    # Using Glue as the sole catalog to work around Figment env var parsing limitations
    catalogs_config = f'[{{name="sail", type="glue", region="us-east-1", endpoint_url="{moto_endpoint}"}}]'

    # Set environment variables for Sail configuration
    old_catalogs = os.environ.get("SAIL_CATALOG__LIST")
    old_aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    old_aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    old_parallelism = os.environ.get("SAIL_EXECUTION__DEFAULT_PARALLELISM")

    os.environ["SAIL_CATALOG__LIST"] = catalogs_config
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    _, port = server.listening_address
    remote_url = f"sc://localhost:{port}"
    yield remote_url
    server.stop()

    # Restore environment
    if old_catalogs is None:
        os.environ.pop("SAIL_CATALOG__LIST", None)
    else:
        os.environ["SAIL_CATALOG__LIST"] = old_catalogs
    if old_aws_key is None:
        os.environ.pop("AWS_ACCESS_KEY_ID", None)
    else:
        os.environ["AWS_ACCESS_KEY_ID"] = old_aws_key
    if old_aws_secret is None:
        os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
    else:
        os.environ["AWS_SECRET_ACCESS_KEY"] = old_aws_secret
    if old_parallelism is None:
        os.environ.pop("SAIL_EXECUTION__DEFAULT_PARALLELISM", None)
    else:
        os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = old_parallelism


@pytest.fixture(scope="module")
def glue_spark(glue_remote: str) -> Generator[SparkSession, None, None]:
    """Create a Spark session connected to Sail with Glue catalog as default."""
    spark = SparkSession.builder.remote(glue_remote).appName("glue_test").getOrCreate()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)

    # Create test database in the default Glue catalog
    spark.sql("CREATE DATABASE test_db")

    yield spark

    # Cleanup - ignore errors during teardown
    with contextlib.suppress(Exception):
        spark.sql("DROP DATABASE test_db CASCADE")
    spark.stop()
