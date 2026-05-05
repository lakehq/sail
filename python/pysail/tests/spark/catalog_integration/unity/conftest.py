"""Pytest fixtures for Unity Catalog integration tests.

Uses PostgreSQL as the backend database and the Unity Catalog OSS Docker image.
"""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

DEFAULT_CATALOG = "sail_test_catalog"


@pytest.fixture(scope="module")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for inter-container communication."""
    network = Network()
    network.create()
    yield network
    network.remove()


@pytest.fixture(scope="module")
def postgres_container(docker_network: Network) -> Generator[DockerContainer, None, None]:
    """Start a PostgreSQL container for Unity Catalog backend."""
    container = (
        DockerContainer("postgres:18.3")
        .with_env("POSTGRES_USER", "test")
        .with_env("POSTGRES_PASSWORD", "test")
        .with_env("POSTGRES_DB", "test")
        .with_network(docker_network)
        .with_network_aliases("postgres")
    )
    container.start()
    wait_for_logs(container, "database system is ready to accept connections", timeout=120)
    # Wait a bit for PostgreSQL to be fully ready
    time.sleep(2)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def unity_container(
    docker_network: Network,
    postgres_container: DockerContainer,  # noqa: ARG001
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[DockerContainer, None, None]:
    """Start a Unity Catalog container with PostgreSQL backend."""
    # Write hibernate config that points to the internal PostgreSQL hostname
    hibernate_config = (
        "hibernate.connection.driver_class=org.postgresql.Driver\n"
        "hibernate.connection.url=jdbc:postgresql://postgres:5432/test\n"
        "hibernate.connection.username=test\n"
        "hibernate.connection.password=test\n"
        "hibernate.hbm2ddl.auto=update\n"
    )
    tmp_dir = tmp_path_factory.mktemp("unity")
    hibernate_path = tmp_dir / "hibernate.properties"
    hibernate_path.write_text(hibernate_config)

    container = (
        DockerContainer("unitycatalog/unitycatalog:v0.3.0")
        .with_exposed_ports(8080)
        .with_volume_mapping(str(hibernate_path), "/home/unitycatalog/etc/conf/hibernate.properties", "ro")
        .with_network(docker_network)
        .with_network_aliases("unity-catalog")
    )
    container.start()
    wait_for_logs(
        container,
        "###################################################################",
        timeout=120,
    )
    yield container
    container.stop()


@pytest.fixture(scope="module")
def unity_rest_url(unity_container: DockerContainer) -> str:
    """Host-accessible Unity Catalog REST API URL."""
    host = unity_container.get_container_host_ip()
    port = unity_container.get_exposed_port(8080)
    return f"http://{host}:{port}/api/2.1/unity-catalog"


@pytest.fixture(scope="module")
def _create_unity_catalog(unity_rest_url: str) -> None:
    """Create the test catalog in Unity Catalog via REST API."""
    url = f"{unity_rest_url}/catalogs"
    payload = {"name": DEFAULT_CATALOG, "comment": "Main catalog for testing"}
    max_retries = 10
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code in (200, 201, 409):
                # 409 = already exists, that's fine
                return
            resp.raise_for_status()
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
        else:
            return


@pytest.fixture(scope="module")
def unity_spark(
    unity_rest_url: str,
    _create_unity_catalog: None,
) -> Generator[SparkSession, None, None]:
    """Start Sail server with Unity catalog and create a Spark session."""
    catalog_config = f'[{{name="sail", type="unity", uri="{unity_rest_url}", default_catalog="{DEFAULT_CATALOG}"}}]'
    server, remote, saved_env = start_sail_server(
        catalog_list=catalog_config,
        extra_env={"UNITY_ALLOW_HTTP_URL": "true"},
    )
    spark = create_spark_session(remote, "unity_catalog_test")
    yield spark
    with contextlib.suppress(Exception):
        spark.stop()
    stop_sail_server(server, saved_env)


@pytest.fixture(scope="module")
def spark(unity_spark: SparkSession) -> SparkSession:
    """Alias for unity_spark, used by BDD step definitions."""
    return unity_spark
