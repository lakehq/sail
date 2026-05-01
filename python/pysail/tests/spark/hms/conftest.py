# ruff: noqa: ARG001, EM102, FLY002, S105, TC003, TRY003, TRY300
"""Pytest fixtures for HMS catalog interop tests."""

from __future__ import annotations

import contextlib
import os
import re
import socket
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

if TYPE_CHECKING:
    from collections.abc import Generator


# ---------------------------------------------------------------------------
# Override the parent's autouse spark_doctest fixture so it does not
# start a default in-memory-catalog Sail server.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(
    request: pytest.FixtureRequest,
    doctest_namespace: dict[str, object],
) -> None:
    spark_fixture = "hms_s3_spark" if request.module.__name__.endswith("test_hms_s3_roundtrip") else "hms_spark"
    doctest_namespace["spark"] = request.getfixturevalue(spark_fixture)


# ---------------------------------------------------------------------------
# HMS container
# ---------------------------------------------------------------------------
#
_HMS_IMAGE = "apache/hive:3.1.3"
_HMS_METASTORE_PORT = 9083
_HMS_STARTUP_TIMEOUT = 180  # seconds
# Use 127.0.0.1 explicitly instead of 'localhost' to avoid IPv6 resolution
# on macOS where Docker only binds exposed ports on IPv4 (0.0.0.0).
_HMS_HOST = "127.0.0.1"
_MINIO_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z"
_MINIO_MC_IMAGE = "minio/mc:RELEASE.2025-05-21T01-59-54Z"
_MINIO_PORT = 9000
_MINIO_USER = "admin"
_MINIO_PASSWORD = "password"
_HMS_S3_BUCKET = "hms-warehouse"


def _hms_s3_core_site_xml(endpoint: str) -> str:
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
    <value>{_MINIO_USER}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>{_MINIO_PASSWORD}</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  </property>
</configuration>
"""


def _spark_s3_options(endpoint: str) -> dict[str, str]:
    return {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.2",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": _MINIO_USER,
        "spark.hadoop.fs.s3a.secret.key": _MINIO_PASSWORD,
        "spark.hadoop.fs.s3a.aws.credentials.provider": ("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
    }


@contextlib.contextmanager
def _classic_spark_mode() -> Generator[None, None, None]:
    old_api_mode = os.environ.get("SPARK_API_MODE")
    old_remote = os.environ.pop("SPARK_REMOTE", None)
    old_connect_mode = os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    os.environ["SPARK_API_MODE"] = "classic"
    try:
        yield
    finally:
        if old_api_mode is None:
            os.environ.pop("SPARK_API_MODE", None)
        else:
            os.environ["SPARK_API_MODE"] = old_api_mode
        if old_remote is not None:
            os.environ["SPARK_REMOTE"] = old_remote
        if old_connect_mode is not None:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = old_connect_mode


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


def _wait_for_hms_catalog(remote_url: str, timeout: float) -> None:
    """Block until Sail can successfully list HMS databases.

    We intentionally probe with ``SHOW DATABASES`` instead of an HMS-only
    ping. This verifies the full harness path (Sail Spark Connect server,
    catalog wiring, and HMS) rather than just metastore socket readiness.
    """
    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        spark = None
        try:
            spark = SparkSession.builder.remote(remote_url).appName("hms_smoke_readiness").create()
            configure_spark_session(spark)
            patch_spark_connect_session(spark)
            spark.sql("SHOW DATABASES").collect()
            return
        except AnalysisException as exc:
            last_error = exc
            time.sleep(1)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1)
        finally:
            if spark is not None:
                spark.stop()

    raise TimeoutError(f"Sail HMS catalog did not become queryable within {timeout}s; last error: {last_error}")


def _run_sail_hms_server(
    hms_endpoint: str,
    extra_env: dict[str, str] | None = None,
) -> Generator[str, None, None]:
    # Defer imports that pull in pysail._native until fixture execution time,
    # after the root conftest's pytest_configure has set up the environment.
    from pysail.spark import SparkConnectServer

    catalogs_config = f'[{{name="sail", type="hms", uris=["{hms_endpoint}"]}}]'
    env = {
        "SAIL_CATALOG__LIST": catalogs_config,
        **(extra_env or {}),
    }
    old_env = {key: os.environ.get(key) for key in env}
    os.environ.update(env)

    server: SparkConnectServer | None = None
    try:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        remote_url = f"sc://localhost:{port}"
        _wait_for_hms_catalog(remote_url, 60)
        yield remote_url
    finally:
        if server is not None:
            with contextlib.suppress(Exception):
                server.stop()
        for key, old_value in old_env.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


@pytest.fixture(scope="session")
def hms_warehouse_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Return a host-visible warehouse path shared with HMS and Spark.

    This local-file harness needs host JVM Spark and the HMS container to see
    the same absolute ``file:`` path for managed-table directories.
    """
    return tmp_path_factory.mktemp("hms_warehouse")


@pytest.fixture(scope="session")
def hms_container(hms_warehouse_dir: Path) -> Generator[DockerContainer, None, None]:
    """Start a Hive Metastore container (apache/hive:3.1.3, service=metastore)."""
    container = DockerContainer(_HMS_IMAGE)
    container.with_exposed_ports(_HMS_METASTORE_PORT)
    container.with_env("SERVICE_NAME", "metastore")
    container.with_env("VERBOSE", "true")
    container.with_env(
        "SERVICE_OPTS",
        f"-Dhive.metastore.warehouse.dir={hms_warehouse_dir.as_uri()}",
    )
    container.with_volume_mapping(hms_warehouse_dir, str(hms_warehouse_dir), mode="rw")
    container.start()

    # Wait for the Thrift metastore port to be reachable on IPv4.
    port = int(container.get_exposed_port(_HMS_METASTORE_PORT))
    _wait_for_port(_HMS_HOST, port, _HMS_STARTUP_TIMEOUT)

    # Extra grace period: the Thrift service may accept TCP connections
    # before it is fully initialized.  The Rust tests also retry with
    # ``list_databases`` polling; here we simply sleep a few extra seconds.
    time.sleep(10)

    yield container
    container.stop()


@pytest.fixture(scope="session")
def hms_endpoint(hms_container: DockerContainer) -> str:
    """Return ``host:port`` for the Hive Metastore Thrift endpoint."""
    port = hms_container.get_exposed_port(_HMS_METASTORE_PORT)
    return f"{_HMS_HOST}:{port}"


# ---------------------------------------------------------------------------
# MinIO-backed S3 warehouse
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def hms_s3_network() -> Generator[Network, None, None]:
    """Return a Docker network shared by MinIO and the short-lived mc setup."""
    network = Network()
    network.create()
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="session")
def hms_s3_minio_container(
    hms_s3_network: Network,
) -> Generator[DockerContainer, None, None]:
    """Start MinIO for S3-backed HMS warehouse tests."""
    container = DockerContainer(_MINIO_IMAGE)
    container.with_exposed_ports(_MINIO_PORT)
    container.with_env("MINIO_ROOT_USER", _MINIO_USER)
    container.with_env("MINIO_ROOT_PASSWORD", _MINIO_PASSWORD)
    container.with_network(hms_s3_network)
    container.with_network_aliases("minio")
    container.with_command(["server", "/data", "--console-address", ":9001"])
    container.start()

    wait_for_logs(container, "MinIO Object Storage Server", timeout=120)

    yield container
    container.stop()


@pytest.fixture(scope="session")
def hms_s3_endpoint(hms_s3_minio_container: DockerContainer) -> str:
    """Return the host-visible MinIO endpoint used by Sail and Spark."""
    host = hms_s3_minio_container.get_container_host_ip()
    port = hms_s3_minio_container.get_exposed_port(_MINIO_PORT)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def hms_s3_internal_endpoint(_hms_s3_bucket: None) -> str:
    """Return the MinIO endpoint visible to containers on the shared network."""
    del _hms_s3_bucket
    return f"http://minio:{_MINIO_PORT}"


@pytest.fixture(scope="session")
def _hms_s3_bucket(
    hms_s3_network: Network,
    hms_s3_minio_container: DockerContainer,
) -> Generator[None, None, None]:
    """Create the S3 warehouse bucket with a short-lived MinIO mc container."""
    del hms_s3_minio_container
    command = (
        "until mc alias set "
        f"minio http://minio:{_MINIO_PORT} {_MINIO_USER} {_MINIO_PASSWORD}; "
        "do sleep 1; done; "
        f"mc rm -r --force minio/{_HMS_S3_BUCKET} || true; "
        f"mc mb minio/{_HMS_S3_BUCKET}; "
        "echo hms-s3-bucket-ready; "
        "tail -f /dev/null"
    )
    container = DockerContainer(_MINIO_MC_IMAGE)
    container.with_network(hms_s3_network)
    container.with_kwargs(entrypoint="/bin/sh")
    container.with_command(["-c", command])
    container.start()

    wait_for_logs(container, "hms-s3-bucket-ready", timeout=120)

    yield
    container.stop()


@pytest.fixture(scope="session")
def hms_s3_env(hms_s3_endpoint: str, _hms_s3_bucket: None) -> dict[str, str]:
    """Return AWS-compatible environment for Sail's S3 object-store client."""
    del _hms_s3_bucket
    return {
        "AWS_ACCESS_KEY_ID": _MINIO_USER,
        "AWS_SECRET_ACCESS_KEY": _MINIO_PASSWORD,
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT": hms_s3_endpoint,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "AWS_ALLOW_HTTP": "true",
    }


@pytest.fixture(scope="session")
def hms_s3_core_site_path(
    tmp_path_factory: pytest.TempPathFactory,
    hms_s3_internal_endpoint: str,
) -> Path:
    """Write a Hadoop core-site.xml that maps s3:// URIs to S3A/MinIO."""
    path = tmp_path_factory.mktemp("hms_s3_conf") / "core-site.xml"
    path.write_text(_hms_s3_core_site_xml(hms_s3_internal_endpoint), encoding="utf-8")
    return path


@pytest.fixture(scope="session")
def hms_s3_container(
    hms_warehouse_dir: Path,
    hms_s3_network: Network,
    hms_s3_core_site_path: Path,
) -> Generator[DockerContainer, None, None]:
    """Start a Hive Metastore container with S3A wiring for s3:// locations."""
    s3a_classpath = ":".join(
        [
            "/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar",
            "/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar",
        ]
    )
    container = DockerContainer(_HMS_IMAGE)
    container.with_exposed_ports(_HMS_METASTORE_PORT)
    container.with_env("SERVICE_NAME", "metastore")
    container.with_env("VERBOSE", "true")
    container.with_env("HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop")
    container.with_env("HADOOP_CLASSPATH", s3a_classpath)
    container.with_env("HIVE_AUX_JARS_PATH", s3a_classpath)
    container.with_env(
        "SERVICE_OPTS",
        f"-Dhive.metastore.warehouse.dir={hms_warehouse_dir.as_uri()}",
    )
    container.with_network(hms_s3_network)
    container.with_volume_mapping(hms_warehouse_dir, str(hms_warehouse_dir), mode="rw")
    container.with_volume_mapping(
        hms_s3_core_site_path,
        "/opt/hadoop/etc/hadoop/core-site.xml",
        mode="ro",
    )
    container.start()

    port = int(container.get_exposed_port(_HMS_METASTORE_PORT))
    _wait_for_port(_HMS_HOST, port, _HMS_STARTUP_TIMEOUT)
    time.sleep(10)

    yield container
    container.stop()


@pytest.fixture(scope="session")
def hms_s3_metastore_endpoint(hms_s3_container: DockerContainer) -> str:
    """Return host:port for the S3-aware Hive Metastore Thrift endpoint."""
    port = hms_s3_container.get_exposed_port(_HMS_METASTORE_PORT)
    return f"{_HMS_HOST}:{port}"


# ---------------------------------------------------------------------------
# Sail server configured with HMS catalog
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def hms_remote(hms_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server configured with HMS as the sole catalog.

    Kept session-scoped to amortize Spark Connect server startup cost across
    all HMS interop tests in the run.
    """
    yield from _run_sail_hms_server(hms_endpoint)


@pytest.fixture(scope="session")
def hms_spark(hms_remote: str) -> Generator[SparkSession, None, None]:
    """Create a Spark session connected to Sail with HMS catalog.

    Kept session-scoped to avoid repeatedly creating remote Spark sessions.
    """
    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    spark = SparkSession.builder.remote(hms_remote).appName("hms_smoke_test").create()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def hms_s3_remote(
    hms_remote: str,
    hms_spark: SparkSession,
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
) -> Generator[str, None, None]:
    """Start a separate Sail server configured for HMS plus MinIO-backed S3."""
    del hms_remote
    del hms_spark
    yield from _run_sail_hms_server(hms_s3_metastore_endpoint, hms_s3_env)


@pytest.fixture(scope="module")
def hms_s3_spark(hms_s3_remote: str) -> Generator[SparkSession, None, None]:
    """Create a Spark Connect session connected to Sail's S3 HMS lane."""
    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    spark = SparkSession.builder.remote(hms_s3_remote).appName("hms_s3_interop_test").create()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# Reference (vanilla JVM) Spark session against the same HMS endpoint
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def reference_spark(
    hms_spark: SparkSession,
    hms_endpoint: str,
    hms_warehouse_dir: Path,
) -> Generator[SparkSession, None, None]:
    """Start a local JVM Spark session with Hive support, pointed at the same HMS.

    This is the *reference* Spark used to create tables that Sail must later
    read back.  It uses ``enableHiveSupport()`` and configures
    ``hive.metastore.uris`` to point at the shared HMS container.

    Right now the HMS harness mixes two Spark modes in one Python process:
    the Sail side uses a Spark Connect session
    (``SparkSession.builder.remote(...)``), while the reference side uses a
    classic JVM Spark session (``enableHiveSupport()``).

    PySpark 4.x has mode/global-state interactions, so creating these in the
    wrong order can break startup or session behavior. The ``hms_spark``
    dependency is intentional: it enforces remote-first, then classic.

    "Spark Connect through and through" would mean using Spark Connect for both
    sides. That removes the mixed-mode conflict, but it also requires standing
    up a second reference Spark Connect server with HMS wired, and replacing
    tests that currently inspect JVM internals
    (``_jsparkSession...externalCatalog()``), since that path is not directly
    available the same way in pure Connect flows.

    PySpark 4.x defaults to Connect mode.  We force classic (JVM) mode by
    setting ``SPARK_API_MODE=classic`` for the duration of this fixture.
    """
    warehouse_uri = hms_warehouse_dir.as_uri()

    with _classic_spark_mode():
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("hms_reference_spark")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.hadoop.hive.metastore.uris", f"thrift://{hms_endpoint}")
            .config("spark.sql.warehouse.dir", warehouse_uri)
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={hms_warehouse_dir}/metastore_db;create=true",
            )
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sql(f"ALTER DATABASE default SET LOCATION '{warehouse_uri}'")

    yield spark

    spark.stop()


@pytest.fixture(scope="module")
def reference_spark_s3(
    hms_s3_spark: SparkSession,
    hms_s3_metastore_endpoint: str,
    hms_warehouse_dir: Path,
    hms_s3_endpoint: str,
) -> Generator[SparkSession, None, None]:
    """Start classic reference Spark with Hive support and MinIO S3A wiring."""
    del hms_s3_spark
    warehouse_uri = hms_warehouse_dir.as_uri()

    with _classic_spark_mode():
        builder = (
            SparkSession.builder.master("local[1]")
            .appName("hms_reference_spark_s3")
            .config("spark.sql.catalogImplementation", "hive")
            .config(
                "spark.hadoop.hive.metastore.uris",
                f"thrift://{hms_s3_metastore_endpoint}",
            )
            .config("spark.sql.warehouse.dir", warehouse_uri)
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={hms_warehouse_dir}/metastore_s3_db;create=true",
            )
        )
        for key, value in _spark_s3_options(hms_s3_endpoint).items():
            builder = builder.config(key, value)
        spark = builder.enableHiveSupport().getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    yield spark

    spark.stop()


@pytest.fixture
def hms_database(
    request: pytest.FixtureRequest,
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_warehouse_dir: Path,
) -> Generator[str, None, None]:
    """Create a unique HMS database for one test under the shared warehouse.

    Function scope keeps table names simple while still isolating test state.
    """
    safe_name = re.sub(r"[^0-9A-Za-z_]+", "_", request.node.nodeid).strip("_").lower()
    database = f"hms_{safe_name[:96]}"
    location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{database}"

    reference_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    reference_spark.sql(f"CREATE DATABASE {database} LOCATION '{location}'")

    yield database

    try:
        reference_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    except Exception:  # noqa: BLE001
        hms_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@pytest.fixture
def hms_s3_database(
    request: pytest.FixtureRequest,
    reference_spark_s3: SparkSession,
    hms_s3_spark: SparkSession,
) -> Generator[str, None, None]:
    """Create a unique HMS database whose managed table root is on S3."""
    safe_name = re.sub(r"[^0-9A-Za-z_]+", "_", request.node.nodeid).strip("_").lower()
    database = f"hms_s3_{safe_name[:88]}"
    location = f"s3://{_HMS_S3_BUCKET}/{database}"

    reference_spark_s3.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    reference_spark_s3.sql(f"CREATE DATABASE {database} LOCATION '{location}'")

    yield database

    try:
        reference_spark_s3.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    except Exception:  # noqa: BLE001
        hms_s3_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
