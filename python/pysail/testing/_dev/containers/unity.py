# SPDX-License-Identifier: Apache-2.0

"""Shared Unity Catalog container fixtures."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pysail.testing._dev.containers.unity_defaults import (
    DEFAULT_CATALOG,
    UNITY_CATALOG_SERVERS,
    UnityCatalogServer,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "unity_container" in getattr(item, "fixturenames", ()) and item.get_closest_marker("timeout") is None:
            # The pytest timeout includes pulling the image, creating the container and waiting for Unity.
            item.add_marker(pytest.mark.timeout(300))


@pytest.fixture(scope="session", params=UNITY_CATALOG_SERVERS, ids=lambda server: server.id)
def unity_server(request: pytest.FixtureRequest) -> UnityCatalogServer:
    """Return the Unity Catalog server image, so that every test runs against each image."""
    return request.param


@pytest.fixture(scope="session")
def unity_storage_root(tmp_path_factory: pytest.TempPathFactory, unity_server: UnityCatalogServer) -> Path:
    """Return the host path mounted for managed Unity Catalog table storage."""
    return tmp_path_factory.mktemp(f"unity_storage_root_{unity_server.id}")


@pytest.fixture(scope="session")
def unity_container(
    tmp_path_factory: pytest.TempPathFactory,
    unity_server: UnityCatalogServer,
    unity_storage_root: Path,
) -> Generator[DockerContainer, None, None]:
    """Start a Unity Catalog container with its embedded H2 backend."""
    tmp_dir = tmp_path_factory.mktemp(f"unity_{unity_server.id}")
    server_config = "server.env=dev\nserver.authorization=disable\nserver.managed-table.enabled=true\n"
    server_path = tmp_dir / "server.properties"
    server_path.write_text(server_config)

    container = (
        DockerContainer(unity_server.image)
        .with_exposed_ports(8080)
        .with_volume_mapping(str(server_path), "/home/unitycatalog/etc/conf/server.properties", "ro")
        .with_volume_mapping(str(unity_storage_root), str(unity_storage_root), "rw")
        .waiting_for(
            LogMessageWaitStrategy(
                "###################################################################"
            ).with_startup_timeout(120)
        )
    )
    with container:
        yield container


@pytest.fixture(scope="session")
def unity_rest_url(unity_container: DockerContainer) -> str:
    """Return the host-accessible Unity Catalog REST API URL."""
    host = unity_container.get_container_host_ip()
    port = unity_container.get_exposed_port(8080)
    return f"http://{host}:{port}/api/2.1/unity-catalog"


@pytest.fixture(scope="session")
def unity_catalog_initialized(unity_rest_url: str, unity_storage_root: Path) -> None:
    """Create the test catalog in Unity Catalog via its REST API."""
    url = f"{unity_rest_url}/catalogs"
    payload = {
        "name": DEFAULT_CATALOG,
        "comment": "Main catalog for testing",
        "storage_root": str(unity_storage_root),
    }
    max_retries = 10
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code in (200, 201, 409):
                return
            response.raise_for_status()
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
        else:
            return
