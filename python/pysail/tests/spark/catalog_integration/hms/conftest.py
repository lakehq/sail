"""Pytest fixtures for Hive Metastore (HMS) catalog integration tests.

Uses the Apache Hive standalone metastore container with a MinIO-backed S3
warehouse, so that database and table locations are ``s3://`` URIs resolved
through Sail's object store, mirroring HMS deployments in the wild.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import socket
import time
from pathlib import Path
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

# The default image is multi-arch; `apache/hive:3.1.3` (amd64-only) can be
# selected on amd64 hosts to exercise an HMS 3.x server end-to-end, including
# pyiceberg's legacy Thrift interop. (Sail itself uses `get_table_req` against
# both versions; only a pre-2.1 metastore would exercise its legacy fallback.)
_HMS_IMAGE = os.environ.get("SAIL_TEST_HMS_IMAGE") or "apache/hive:4.0.1"
_HMS_METASTORE_PORT = 9083
_HMS_STARTUP_TIMEOUT = 180  # seconds
# Use 127.0.0.1 explicitly instead of 'localhost' to avoid IPv6 resolution
# on macOS where Docker only binds exposed ports on IPv4 (0.0.0.0).
_HMS_HOST = "127.0.0.1"
_MINIO_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z"
_MINIO_PORT = 9000
MINIO_USER = "admin"
MINIO_PASSWORD = "password"  # noqa: S105
HMS_S3_BUCKET = "hms-warehouse"


def hms_test_database_name(request: pytest.FixtureRequest, prefix: str = "hms_", max_name_len: int = 100) -> str:
    """Build a unique, HMS-safe database name derived from the test node ID."""
    module = Path(str(request.node.fspath)).stem
    test = getattr(request.node, "originalname", None) or request.node.name
    safe_name = re.sub(r"[^0-9A-Za-z_]+", "_", f"{module}_{test}").strip("_").lower()
    digest = hashlib.sha1(request.node.nodeid.encode(), usedforsecurity=False).hexdigest()[:12]
    suffix = f"_{digest}"
    available = max_name_len - len(prefix) - len(suffix)
    return f"{prefix}{safe_name[:available]}{suffix}"


def _hms_core_site_xml(endpoint: str) -> str:
    """Hadoop core-site.xml mapping both s3:// and s3a:// URIs to S3A/MinIO.

    The ``fs.s3.impl`` mapping is load-bearing: it lets the metastore create
    database and table directories for locations given as ``s3://...``.
    """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3A</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>{endpoint}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>{MINIO_USER}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>{MINIO_PASSWORD}</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  </property>
</configuration>
"""


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    """Block until ``host:port`` accepts a TCP connection."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(1)
    msg = f"HMS did not start accepting connections on {host}:{port} within {timeout}s"
    raise TimeoutError(msg)


def _wait_for_hms_catalog(remote: str, timeout: float) -> None:
    """Block until Sail can successfully list HMS databases.

    We intentionally probe with ``SHOW DATABASES`` instead of an HMS-only
    ping. This verifies the full harness path (Sail Spark Connect server,
    catalog wiring, and HMS) rather than just metastore socket readiness.
    The metastore Thrift port opens before its embedded Derby schema is
    initialized, so this probe absorbs the remaining startup window. The
    window is generous because the metastore image is amd64-only and Derby
    schema initialization is slow under emulation on arm64 hosts.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        spark = None
        try:
            spark = create_spark_session(remote, "hms_readiness", new_session=True)
            spark.sql("SHOW DATABASES").collect()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1)
        else:
            return
        finally:
            if spark is not None:
                with contextlib.suppress(Exception):
                    spark.stop()
    msg = f"Sail HMS catalog did not become queryable within {timeout}s; last error: {last_error}"
    raise TimeoutError(msg)


@pytest.fixture(scope="session")
def hms_docker_network() -> Generator[Network, None, None]:
    """Create a Docker network shared by MinIO and the metastore."""
    network = Network()
    network.create()
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="session")
def minio_container(hms_docker_network: Network) -> Generator[DockerContainer, None, None]:
    """Start MinIO for the S3-backed HMS warehouse."""
    container = (
        DockerContainer(_MINIO_IMAGE)
        .with_exposed_ports(_MINIO_PORT)
        .with_env("MINIO_ROOT_USER", MINIO_USER)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
        .with_network(hms_docker_network)
        .with_network_aliases("minio")
        .with_command(["server", "/data", "--console-address", ":9001"])
    )
    container.start()
    wait_for_logs(container, "MinIO Object Storage Server", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="session")
def minio_host_endpoint(minio_container: DockerContainer) -> str:
    """Host-accessible MinIO endpoint used by Sail and pyiceberg."""
    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(_MINIO_PORT)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def minio_internal_endpoint() -> str:
    """MinIO endpoint visible to containers on the shared network."""
    return f"http://minio:{_MINIO_PORT}"


@pytest.fixture(scope="session")
def minio_s3_client(minio_host_endpoint: str):
    """A boto3 S3 client for the MinIO warehouse."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=minio_host_endpoint,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture(scope="session")
def pyiceberg_s3_properties(minio_host_endpoint: str) -> dict[str, str]:
    """pyiceberg FileIO properties for the MinIO warehouse."""
    return {
        "s3.endpoint": minio_host_endpoint,
        "s3.access-key-id": MINIO_USER,
        "s3.secret-access-key": MINIO_PASSWORD,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }


@pytest.fixture(scope="session")
def _create_warehouse_bucket(minio_s3_client) -> None:
    """Create the warehouse bucket on MinIO."""
    s3 = minio_s3_client
    # Retry bucket creation a few times to allow MinIO to fully start
    max_retries = 10
    for attempt in range(max_retries):
        try:
            s3.create_bucket(Bucket=HMS_S3_BUCKET)
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)
        else:
            return


@pytest.fixture(scope="session")
def hms_core_site_path(
    tmp_path_factory: pytest.TempPathFactory,
    minio_internal_endpoint: str,
) -> Path:
    """Write a Hadoop core-site.xml that maps s3:// URIs to S3A/MinIO."""
    path = tmp_path_factory.mktemp("hms_conf") / "core-site.xml"
    path.write_text(_hms_core_site_xml(minio_internal_endpoint), encoding="utf-8")
    return path


@pytest.fixture(scope="session")
def hms_container(
    hms_docker_network: Network,
    minio_container: DockerContainer,  # noqa: ARG001
    _create_warehouse_bucket: None,
    hms_core_site_path: Path,
) -> Generator[DockerContainer, None, None]:
    """Start a Hive Metastore container with S3A wiring for s3:// locations."""
    # Use a wildcard so the S3A jars are found regardless of the Hadoop
    # version bundled in the image.
    s3a_classpath = "/opt/hadoop/share/hadoop/tools/lib/*"
    container = (
        DockerContainer(_HMS_IMAGE)
        .with_exposed_ports(_HMS_METASTORE_PORT)
        .with_env("SERVICE_NAME", "metastore")
        .with_env("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
        .with_env("HADOOP_CLASSPATH", s3a_classpath)
        .with_env("HIVE_AUX_JARS_PATH", s3a_classpath)
        .with_network(hms_docker_network)
        .with_network_aliases("hms")
        .with_volume_mapping(
            str(hms_core_site_path),
            "/opt/hadoop/etc/hadoop/core-site.xml",
            mode="ro",
        )
    )
    container.start()
    port = int(container.get_exposed_port(_HMS_METASTORE_PORT))
    _wait_for_port(_HMS_HOST, port, _HMS_STARTUP_TIMEOUT)
    yield container
    container.stop()


@pytest.fixture(scope="session")
def hms_metastore_endpoint(hms_container: DockerContainer) -> str:
    """Host-accessible ``host:port`` for the metastore Thrift endpoint."""
    port = hms_container.get_exposed_port(_HMS_METASTORE_PORT)
    return f"{_HMS_HOST}:{port}"


@pytest.fixture(scope="session")
def hms_s3_env(minio_host_endpoint: str) -> dict[str, str]:
    """AWS-compatible environment for Sail's S3 object-store client."""
    return {
        "AWS_ACCESS_KEY_ID": MINIO_USER,
        "AWS_SECRET_ACCESS_KEY": MINIO_PASSWORD,
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT": minio_host_endpoint,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "AWS_ALLOW_HTTP": "true",
    }


@pytest.fixture(scope="module")
def hms_spark(
    hms_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
) -> Generator[SparkSession, None, None]:
    """Start a Sail server with an HMS catalog and create a Spark session.

    The catalog is named ``spark_catalog`` and set as the default catalog to
    mirror the configuration reported in
    https://github.com/lakehq/sail/issues/2055.
    """
    catalog_config = f'[{{name="spark_catalog", type="hive_metastore", uris=["{hms_metastore_endpoint}"]}}]'
    server, remote, saved_env = start_sail_server(
        catalog_list=catalog_config,
        extra_env={
            "SAIL_CATALOG__DEFAULT_CATALOG": "spark_catalog",
            **hms_s3_env,
        },
    )
    try:
        _wait_for_hms_catalog(remote, 300)
        spark = create_spark_session(remote, "hms_catalog_test")
    except Exception:
        # Stop the server and restore the environment even when readiness
        # fails, so the mutated environment does not leak into other suites.
        stop_sail_server(server, saved_env)
        raise
    yield spark
    with contextlib.suppress(Exception):
        spark.stop()
    stop_sail_server(server, saved_env)


@pytest.fixture
def hms_database(request: pytest.FixtureRequest, hms_spark: SparkSession) -> Generator[str, None, None]:
    """Create a unique HMS database whose managed table root is on S3."""
    database = hms_test_database_name(request)
    location = f"s3://{HMS_S3_BUCKET}/{database}"
    hms_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    hms_spark.sql(f"CREATE DATABASE {database} LOCATION '{location}'")
    yield database
    with contextlib.suppress(Exception):
        hms_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@pytest.fixture(scope="module")
def pyiceberg_hive_catalog(
    hms_container: DockerContainer,  # noqa: ARG001
    hms_metastore_endpoint: str,
    pyiceberg_s3_properties: dict[str, str],
):
    """A pyiceberg Hive catalog acting as a foreign engine against the same HMS.

    This mimics external tooling (e.g. pyiceberg-backed dataset libraries)
    that creates Iceberg tables in HMS outside of Sail.
    """
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "hms_test",
        **{
            "type": "hive",
            "uri": f"thrift://{hms_metastore_endpoint}",
            **pyiceberg_s3_properties,
        },
    )
