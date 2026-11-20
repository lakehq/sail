# SPDX-License-Identifier: Apache-2.0

"""Pytest fixtures for Iceberg REST catalog integration tests.

Uses SeaweedFS for S3-compatible storage and Iceberg REST-compatible catalog
servers.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, NamedTuple

import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

LAKEKEEPER_IMAGE = "quay.io/lakekeeper/catalog:v0.12.1"
LAKEKEEPER_DATABASE_URL = "postgresql://postgres:postgres@lakekeeper-db:5432/postgres"
LAKEKEEPER_PROJECT_ID = "00000000-0000-0000-0000-000000000000"
NESSIE_NAMESPACE_SEPARATOR = "-"
ACCESS_DELEGATION = "X-Iceberg-Access-Delegation"


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
        .waiting_for(LogMessageWaitStrategy("Start Seaweed S3 API").with_startup_timeout(120))
    )
    container.start()
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


def lakekeeper_command_container(
    docker_network: Network,
    command: str,
) -> DockerContainer:
    """Configure a Lakekeeper container for the test PostgreSQL database."""
    return (
        DockerContainer(LAKEKEEPER_IMAGE)
        .with_command([command])
        .with_env("LAKEKEEPER__PG_ENCRYPTION_KEY", "This-is-NOT-Secure!")
        .with_env("LAKEKEEPER__PG_DATABASE_URL_READ", LAKEKEEPER_DATABASE_URL)
        .with_env("LAKEKEEPER__PG_DATABASE_URL_WRITE", LAKEKEEPER_DATABASE_URL)
        .with_network(docker_network)
    )


@pytest.fixture(scope="module")
def lakekeeper_database_container(
    docker_network: Network,
) -> Generator[DockerContainer, None, None]:
    """Start the PostgreSQL database used by Lakekeeper."""
    container = (
        DockerContainer("postgres:17")
        .with_env("POSTGRES_PASSWORD", "postgres")
        .with_network(docker_network)
        .with_network_aliases("lakekeeper-db")
        .waiting_for(LogMessageWaitStrategy("database system is ready to accept connections").with_startup_timeout(120))
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def _lakekeeper_migration(
    docker_network: Network,
    lakekeeper_database_container: DockerContainer,  # noqa: ARG001
) -> None:
    """Apply Lakekeeper database migrations before starting the server."""
    container = lakekeeper_command_container(docker_network, "migrate")
    container.start()
    try:
        result = container.get_wrapped_container().wait(timeout=120)
        if result["StatusCode"] != 0:
            stdout, stderr = container.get_logs()
            message = (stdout + stderr).decode(errors="replace")
            msg = f"Lakekeeper database migration failed:\n{message}"
            raise RuntimeError(msg)
    finally:
        container.stop()


@pytest.fixture(scope="module")
def lakekeeper_container(
    docker_network: Network,
    seaweedfs_container: DockerContainer,  # noqa: ARG001
    _lakekeeper_migration: None,
) -> Generator[DockerContainer, None, None]:
    """Start Lakekeeper after its database has been migrated."""
    container = (
        lakekeeper_command_container(docker_network, "serve")
        .with_exposed_ports(8181)
        .with_network_aliases("lakekeeper")
        .waiting_for(LogMessageWaitStrategy("Starting server on 0.0.0.0:8181").with_startup_timeout(120))
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def lakekeeper_endpoint(lakekeeper_container: DockerContainer) -> str:
    """Return a healthy host-accessible Lakekeeper endpoint."""
    host = lakekeeper_container.get_container_host_ip()
    port = lakekeeper_container.get_exposed_port(8181)
    endpoint = f"http://{host}:{port}"

    for attempt in range(30):
        try:
            response = requests.get(f"{endpoint}/health", timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            if attempt == 29:
                raise
            time.sleep(1)
        else:
            return endpoint
    msg = "unreachable"
    raise AssertionError(msg)


@pytest.fixture(scope="module")
def lakekeeper_warehouse_id(
    lakekeeper_endpoint: str,
    seaweedfs_internal_endpoint: str,
    _create_s3_bucket: None,
) -> str:
    """Bootstrap Lakekeeper and create the S3-compatible test warehouse."""
    bootstrap = requests.post(
        f"{lakekeeper_endpoint}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
        timeout=30,
    )
    bootstrap.raise_for_status()

    warehouse = requests.post(
        f"{lakekeeper_endpoint}/management/v1/warehouse",
        json={
            "warehouse-name": "demo",
            "project-id": LAKEKEEPER_PROJECT_ID,
            "storage-profile": {
                "type": "s3",
                "bucket": "icebergdata",
                "key-prefix": "lakekeeper",
                "endpoint": seaweedfs_internal_endpoint,
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
                "remote-signing-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": "admin",
                "secret-access-key": "password",
            },
        },
        timeout=30,
    )
    warehouse.raise_for_status()
    return warehouse.json()["warehouse-id"]


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
        .waiting_for(
            LogMessageWaitStrategy("INFO org.eclipse.jetty.server.Server - Started ").with_startup_timeout(120)
        )
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def iceberg_rest_endpoint(iceberg_rest_container: DockerContainer) -> str:
    """Host-accessible Iceberg REST catalog endpoint."""
    host = iceberg_rest_container.get_container_host_ip()
    port = iceberg_rest_container.get_exposed_port(8181)
    return f"http://{host}:{port}"


class RecordedRequest(NamedTuple):
    """A request forwarded by the recording catalog proxy."""

    method: str
    path: str
    body: Any
    status: int
    access_delegation: str | None


@pytest.fixture(scope="module")
def catalog_config_overrides() -> dict[str, str]:
    """Catalog properties that the recording catalog proxy adds as config overrides."""
    return {}


@pytest.fixture(scope="module")
def recording_catalog(
    iceberg_rest_endpoint: str, catalog_config_overrides: dict[str, str]
) -> Generator[tuple[str, list[RecordedRequest]], None, None]:
    """Forward requests to the REST catalog and record each of them."""
    records = []

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("content-length", "0")))
            headers = {"Content-Type": "application/json"}
            if ACCESS_DELEGATION in self.headers:
                headers[ACCESS_DELEGATION] = self.headers[ACCESS_DELEGATION]
            response = requests.request(
                self.command,
                iceberg_rest_endpoint + self.path,
                data=body or None,
                headers=headers,
                timeout=30,
            )
            records.append(
                RecordedRequest(
                    self.command,
                    urllib.parse.unquote(self.path),
                    json.loads(body) if body else None,
                    response.status_code,
                    self.headers.get(ACCESS_DELEGATION),
                )
            )
            content = response.content
            if catalog_config_overrides and urllib.parse.urlsplit(self.path).path == "/v1/config":
                config = response.json()
                config.setdefault("overrides", {}).update(catalog_config_overrides)
                content = json.dumps(config).encode()
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(content)

        do_GET = forward  # noqa: N815
        do_POST = forward  # noqa: N815
        do_DELETE = forward  # noqa: N815
        do_HEAD = forward  # noqa: N815

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", records
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.fixture(scope="module")
def remote(
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
) -> Generator[str, None, None]:
    """Start Sail server with Iceberg REST catalog."""
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{iceberg_rest_endpoint}"}}]'
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": catalog_config,
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        },
    ) as server:
        yield server.remote


def make_nessie_container(
    docker_network: Network,
    seaweedfs_internal_endpoint: str,
    *,
    config_path: Path | None = None,
) -> DockerContainer:
    """Build a Nessie server container with Iceberg REST enabled."""
    container = (
        DockerContainer("ghcr.io/projectnessie/nessie:0.107.5")
        .with_exposed_ports(19120)
        .with_env("NESSIE_CATALOG_DEFAULT_WAREHOUSE", "warehouse")
        .with_env("NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION", "s3://icebergdata/nessie")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT", seaweedfs_internal_endpoint)
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION", "us-east-1")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS", "true")
        .with_env("NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_AUTH_TYPE", "STATIC")
        .with_env(
            "NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY",
            "urn:nessie-secret:quarkus:nessie.catalog.secrets.s3default",
        )
        .with_env("NESSIE_CATALOG_SECRETS_S3DEFAULT_NAME", "admin")
        .with_env("NESSIE_CATALOG_SECRETS_S3DEFAULT_SECRET", "password")
        .with_network(docker_network)
        .with_network_aliases("nessie")
    )
    if config_path is not None:
        container = container.with_volume_mapping(
            str(config_path),
            "/tmp/nessie-application.properties",  # noqa: S108
            "ro",
        ).with_env("QUARKUS_CONFIG_LOCATIONS", "file:/tmp/nessie-application.properties")
    return container


@pytest.fixture(scope="module")
def nessie_container(
    docker_network: Network,
    seaweedfs_container: DockerContainer,  # noqa: ARG001
    seaweedfs_internal_endpoint: str,
    _create_s3_bucket: None,
) -> Generator[DockerContainer, None, None]:
    """Start a Nessie server with Iceberg REST enabled."""
    container = make_nessie_container(
        docker_network,
        seaweedfs_internal_endpoint,
    )
    container.start()
    wait_for_logs(container, "Nessie 0.107.5", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def nessie_iceberg_rest_endpoint(nessie_container: DockerContainer) -> str:
    """Host-accessible Nessie Iceberg REST catalog endpoint."""
    host = nessie_container.get_container_host_ip()
    port = nessie_container.get_exposed_port(19120)
    return f"http://{host}:{port}/iceberg"


@pytest.fixture(scope="module")
def nessie_container_custom_separator(
    docker_network: Network,
    seaweedfs_container: DockerContainer,  # noqa: ARG001
    seaweedfs_internal_endpoint: str,
    _create_s3_bucket: None,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[DockerContainer, None, None]:
    """Start a Nessie server whose Iceberg REST config uses a custom namespace separator."""
    tmp_dir = tmp_path_factory.mktemp("nessie-custom-separator")
    config_path = tmp_dir / "nessie-application.properties"
    config_path.write_text(f"nessie.catalog.iceberg-config-defaults.namespace-separator={NESSIE_NAMESPACE_SEPARATOR}\n")
    container = make_nessie_container(
        docker_network,
        seaweedfs_internal_endpoint,
        config_path=config_path,
    )
    container.start()
    wait_for_logs(container, "Nessie 0.107.5", timeout=120)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def nessie_custom_separator_iceberg_rest_endpoint(nessie_container_custom_separator: DockerContainer) -> str:
    """Host-accessible custom-separator Nessie Iceberg REST catalog endpoint."""
    host = nessie_container_custom_separator.get_container_host_ip()
    port = nessie_container_custom_separator.get_exposed_port(19120)
    return f"http://{host}:{port}/iceberg"
