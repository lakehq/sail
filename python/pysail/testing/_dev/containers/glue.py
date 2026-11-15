"""AWS Glue catalog container fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

if TYPE_CHECKING:
    from collections.abc import Generator

_MOTO_IMAGE = "motoserver/moto:5.1.22"
_MOTO_PORT = 5000


@pytest.fixture(scope="module")
def glue_moto_container() -> Generator[DockerContainer, None, None]:
    """Start Moto with its AWS Glue API enabled."""
    container = (
        DockerContainer(_MOTO_IMAGE)
        .with_exposed_ports(_MOTO_PORT)
        .waiting_for(LogMessageWaitStrategy("Running on").with_startup_timeout(120))
    )
    with container:
        yield container


@pytest.fixture(scope="module")
def moto_endpoint(glue_moto_container: DockerContainer) -> str:
    """Return the host-visible Moto endpoint."""
    host = glue_moto_container.get_container_host_ip()
    port = glue_moto_container.get_exposed_port(_MOTO_PORT)
    return f"http://{host}:{port}"
