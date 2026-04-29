"""Pytest fixtures for HMS catalog interop tests."""

from __future__ import annotations

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

if TYPE_CHECKING:
    from collections.abc import Generator


# ---------------------------------------------------------------------------
# Override the parent's autouse spark_doctest fixture so it does not
# start a default in-memory-catalog Sail server.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace, hms_spark):
    doctest_namespace["spark"] = hms_spark


# ---------------------------------------------------------------------------
# HMS container
# ---------------------------------------------------------------------------

_HMS_IMAGE = "apache/hive:3.1.3"
_HMS_METASTORE_PORT = 9083
_HMS_STARTUP_TIMEOUT = 180  # seconds
# Use 127.0.0.1 explicitly instead of 'localhost' to avoid IPv6 resolution
# on macOS where Docker only binds exposed ports on IPv4 (0.0.0.0).
_HMS_HOST = "127.0.0.1"


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
    """Block until Sail can successfully list HMS databases."""
    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        spark = None
        try:
            spark = (
                SparkSession.builder.remote(remote_url)
                .appName("hms_smoke_readiness")
                .getOrCreate()
            )
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

    raise TimeoutError(
        "Sail HMS catalog did not become queryable within "
        f"{timeout}s; last error: {last_error}"
    )


@pytest.fixture(scope="session")
def hms_warehouse_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Return a host-visible warehouse path shared with HMS and Spark."""
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
# Sail server configured with HMS catalog
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def hms_remote(hms_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server configured with HMS as the sole catalog."""
    # Defer imports that pull in pysail._native until fixture execution time,
    # after the root conftest's pytest_configure has set up the environment.
    from pysail.spark import SparkConnectServer

    catalogs_config = (
        f'[{{name="sail", type="hms", uris=["{hms_endpoint}"]}}]'
    )

    # Snapshot and set environment variables
    _env_keys = [
        "SAIL_CATALOG__LIST",
    ]
    old_env = {k: os.environ.get(k) for k in _env_keys}

    os.environ["SAIL_CATALOG__LIST"] = catalogs_config

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    _, port = server.listening_address
    remote_url = f"sc://localhost:{port}"
    _wait_for_hms_catalog(remote_url, 60)
    yield remote_url
    server.stop()

    # Restore environment
    for k in _env_keys:
        if old_env[k] is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = old_env[k]


@pytest.fixture(scope="session")
def hms_spark(hms_remote: str) -> Generator[SparkSession, None, None]:
    """Create a Spark session connected to Sail with HMS catalog."""
    from pysail.tests.spark.conftest import (
        configure_spark_session,
        patch_spark_connect_session,
    )

    spark = (
        SparkSession.builder.remote(hms_remote)
        .appName("hms_smoke_test")
        .getOrCreate()
    )
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

    The ``hms_spark`` dependency is intentional: it forces the Sail Spark
    Connect session to exist before a classic JVM Spark session is created
    in-process, avoiding Spark's mixed-session startup conflict.

    PySpark 4.x defaults to Connect mode.  We force classic (JVM) mode by
    setting ``SPARK_API_MODE=classic`` for the duration of this fixture.
    """
    warehouse_uri = hms_warehouse_dir.as_uri()

    # Force classic JVM mode for PySpark 4.x
    old_api_mode = os.environ.get("SPARK_API_MODE")
    os.environ["SPARK_API_MODE"] = "classic"

    # Temporarily clear SPARK_REMOTE / SPARK_CONNECT_MODE_ENABLED so the
    # classic builder path is taken.
    old_spark_remote = os.environ.pop("SPARK_REMOTE", None)
    old_connect_mode = os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)

    try:
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
    finally:
        # Restore environment regardless of session creation outcome.
        if old_api_mode is not None:
            os.environ["SPARK_API_MODE"] = old_api_mode
        else:
            os.environ.pop("SPARK_API_MODE", None)
        if old_spark_remote is not None:
            os.environ["SPARK_REMOTE"] = old_spark_remote
        if old_connect_mode is not None:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = old_connect_mode

    yield spark

    spark.stop()


@pytest.fixture
def hms_database(
    request: pytest.FixtureRequest,
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_warehouse_dir: Path,
) -> Generator[str, None, None]:
    """Create a unique HMS database for one test under the shared warehouse."""
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
