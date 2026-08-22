"""Iceberg REST catalog container fixtures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

if TYPE_CHECKING:
    from collections.abc import Generator

_MINIO_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z"
_MINIO_CLIENT_IMAGE = "minio/mc:RELEASE.2025-05-21T01-59-54Z"
_ICEBERG_REST_IMAGE = "apache/iceberg-rest-fixture:1.10.1"
_MINIO_ALIAS = "minio"
_MINIO_ENDPOINT = f"http://{_MINIO_ALIAS}:9000"
_ICEBERG_REST_PORT = 8181


@dataclass(frozen=True)
class IcebergRestService:
    """Host-visible Iceberg REST catalog endpoint."""

    host: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}"


def _published_host(container: DockerContainer) -> str:
    host = container.get_container_host_ip()
    return "127.0.0.1" if host in {"localhost", "::1"} else host


@pytest.fixture(scope="session")
def iceberg_rest_service() -> Generator[IcebergRestService, None, None]:
    """Start MinIO and the Apache Iceberg REST catalog fixture."""
    network = Network()
    network.create()

    minio = (
        DockerContainer(_MINIO_IMAGE)
        .with_env("MINIO_ROOT_USER", "admin")
        .with_env("MINIO_ROOT_PASSWORD", "password")
        .with_command("server /data --console-address :9001")
        .with_network(network)
        .with_network_aliases(_MINIO_ALIAS)
        .waiting_for(LogMessageWaitStrategy("MinIO Object Storage Server").with_startup_timeout(120))
    )
    create_bucket = " ".join(
        [
            f"until /usr/bin/mc alias set minio {_MINIO_ENDPOINT} admin password; do sleep 1; done;",
            "/usr/bin/mc mb --ignore-existing minio/icebergdata;",
            "/usr/bin/mc anonymous set public minio/icebergdata;",
            "tail -f /dev/null",
        ]
    )
    minio_client = (
        DockerContainer(_MINIO_CLIENT_IMAGE)
        .with_kwargs(entrypoint="/bin/sh")
        .with_command(["-c", create_bucket])
        .with_network(network)
        .waiting_for(LogMessageWaitStrategy("Bucket created successfully").with_startup_timeout(120))
    )
    rest_catalog = (
        DockerContainer(_ICEBERG_REST_IMAGE)
        .with_exposed_ports(_ICEBERG_REST_PORT)
        .with_env("AWS_ACCESS_KEY_ID", "admin")
        .with_env("AWS_SECRET_ACCESS_KEY", "password")
        .with_env("AWS_REGION", "us-east-1")
        .with_env("CATALOG_CATALOG__IMPL", "org.apache.iceberg.jdbc.JdbcCatalog")
        .with_env("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory")
        .with_env("CATALOG_WAREHOUSE", "s3://icebergdata/demo")
        .with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env("CATALOG_S3_ENDPOINT", _MINIO_ENDPOINT)
        .with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_network(network)
        .waiting_for(
            LogMessageWaitStrategy("INFO org.eclipse.jetty.server.Server - Started ").with_startup_timeout(120)
        )
    )

    try:
        with minio, minio_client, rest_catalog:
            yield IcebergRestService(
                host=_published_host(rest_catalog),
                port=int(rest_catalog.get_exposed_port(_ICEBERG_REST_PORT)),
            )
    finally:
        network.remove()
