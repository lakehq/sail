"""Pytest fixtures for Iceberg REST catalog integration tests.

Uses SeaweedFS for S3-compatible storage and the Apache Iceberg REST fixture
as the catalog server.
"""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING

import pytest
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


@pytest.fixture(scope="module")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for inter-container communication."""
    network = Network()
    network.create()
    yield network
    network.remove()


@pytest.fixture(scope="module")
def seaweedfs_container(
    docker_network: Network,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[DockerContainer, None, None]:
    """Start a SeaweedFS container with S3 API enabled."""
    # Write S3 IAM config so signed S3 requests with admin/password are accepted.
    s3_config = (
        '{"identities":[{"name":"admin","credentials":[{"accessKey":"admin","secretKey":"password"}]'
        ',"actions":["Admin","Read","Write"]}]}'
    )
    tmp_dir = tmp_path_factory.mktemp("seaweedfs")
    config_path = tmp_dir / "s3_config.json"
    config_path.write_text(s3_config)

    container = (
        DockerContainer("chrislusf/seaweedfs:4.21")
        .with_command("server -s3 -s3.port=8333 -master.volumeSizeLimitMB=64 -s3.config=/etc/seaweedfs/s3_config.json")
        .with_volume_mapping(str(config_path), "/etc/seaweedfs/s3_config.json", "ro")
        .with_exposed_ports(8333)
        .with_network(docker_network)
        .with_network_aliases("seaweedfs")
    )
    container.start()
    wait_for_logs(container, "Start Seaweed S3 API", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def seaweedfs_internal_endpoint() -> str:
    """Internal S3 endpoint (within Docker network)."""
    return "http://seaweedfs:8333"


@pytest.fixture(scope="module")
def seaweedfs_host_endpoint(seaweedfs_container: DockerContainer) -> str:
    """Host-accessible S3 endpoint."""
    host = seaweedfs_container.get_container_host_ip()
    port = seaweedfs_container.get_exposed_port(8333)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def _create_s3_bucket(seaweedfs_host_endpoint: str) -> None:
    """Create the icebergdata bucket on SeaweedFS using boto3."""
    import boto3
    from botocore.config import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=seaweedfs_host_endpoint,
        aws_access_key_id="admin",
        aws_secret_access_key="password",  # noqa: S106
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    # Retry bucket creation a few times to allow SeaweedFS to fully start
    max_retries = 10
    for attempt in range(max_retries):
        try:
            s3.create_bucket(Bucket="icebergdata")
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)
        else:
            return


@pytest.fixture(scope="module")
def iceberg_rest_container(
    docker_network: Network,
    seaweedfs_container: DockerContainer,  # noqa: ARG001
    seaweedfs_internal_endpoint: str,
    _create_s3_bucket: None,
) -> Generator[DockerContainer, None, None]:
    """Start an Apache Iceberg REST catalog fixture."""
    container = (
        DockerContainer("apache/iceberg-rest-fixture:1.10.1")
        .with_exposed_ports(8181)
        .with_env("AWS_ACCESS_KEY_ID", "admin")
        .with_env("AWS_SECRET_ACCESS_KEY", "password")
        .with_env("AWS_REGION", "us-east-1")
        .with_env("CATALOG_CATALOG__IMPL", "org.apache.iceberg.jdbc.JdbcCatalog")
        .with_env("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory")
        .with_env("CATALOG_WAREHOUSE", "s3://icebergdata/demo")
        .with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env("CATALOG_S3_ENDPOINT", seaweedfs_internal_endpoint)
        .with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_network(docker_network)
        .with_network_aliases("iceberg-rest")
    )
    container.start()
    wait_for_logs(container, "INFO org.eclipse.jetty.server.Server - Started ", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def iceberg_rest_endpoint(iceberg_rest_container: DockerContainer) -> str:
    """Host-accessible Iceberg REST catalog endpoint."""
    host = iceberg_rest_container.get_container_host_ip()
    port = iceberg_rest_container.get_exposed_port(8181)
    return f"http://{host}:{port}"


@pytest.fixture(scope="module")
def iceberg_spark(iceberg_rest_endpoint: str) -> Generator[SparkSession, None, None]:
    """Start Sail server with Iceberg REST catalog and create a Spark session."""
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{iceberg_rest_endpoint}"}}]'
    server, remote, saved_env = start_sail_server(catalog_list=catalog_config)
    spark = create_spark_session(remote, "iceberg_rest_catalog_test")
    yield spark
    with contextlib.suppress(Exception):
        spark.stop()
    stop_sail_server(server, saved_env)


@pytest.fixture(scope="module")
def spark(iceberg_spark: SparkSession) -> SparkSession:
    """Alias for iceberg_spark, used by BDD step definitions."""
    return iceberg_spark
