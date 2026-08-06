"""Celeborn container fixtures."""

from __future__ import annotations

import socket
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

if TYPE_CHECKING:
    from collections.abc import Generator

_IMAGE = "apache/celeborn:0.6.3"
_MASTER_PORT = 12097
_CONFIG_PATH = Path(__file__).with_name("celeborn-defaults.conf")


def _wait_for_port(host: str, port: int, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    msg = f"Celeborn master did not accept connections on {host}:{port}"
    raise TimeoutError(msg)


@pytest.fixture(scope="package")
def celeborn_master() -> Generator[tuple[str, int], None, None]:
    network = Network()
    network.create()
    master = (
        DockerContainer(_IMAGE)
        .with_kwargs(hostname="celeborn-master")
        .with_env("CELEBORN_LOCAL_HOSTNAME", "celeborn-master")
        .with_env("CELEBORN_MASTER_MEMORY", "512m")
        .with_env("CELEBORN_NO_DAEMONIZE", "1")
        .with_command(["start-master.sh", "--host", "celeborn-master", "--port", str(_MASTER_PORT)])
        .with_volume_mapping(str(_CONFIG_PATH), "/opt/celeborn/conf/celeborn-defaults.conf", "ro")
        .with_exposed_ports(_MASTER_PORT)
        .with_network(network)
        .with_network_aliases("celeborn-master")
    )
    worker = (
        DockerContainer(_IMAGE)
        .with_kwargs(hostname="celeborn-worker")
        .with_env("CELEBORN_LOCAL_HOSTNAME", "celeborn-worker")
        .with_env("CELEBORN_WORKER_MEMORY", "512m")
        .with_env("CELEBORN_WORKER_OFFHEAP_MEMORY", "512m")
        .with_env("CELEBORN_NO_DAEMONIZE", "1")
        .with_command(
            [
                "start-worker.sh",
                "--host",
                "celeborn-worker",
                "--port",
                "12000",
                "celeborn://celeborn-master:12097",
            ]
        )
        .with_volume_mapping(str(_CONFIG_PATH), "/opt/celeborn/conf/celeborn-defaults.conf", "ro")
        .with_network(network)
        .with_network_aliases("celeborn-worker")
        .waiting_for(LogMessageWaitStrategy("Register worker successfully.").with_startup_timeout(90))
    )
    try:
        master.start()
        host = master.get_container_host_ip()
        port = int(master.get_exposed_port(_MASTER_PORT))
        _wait_for_port(host, port)
        worker.start()
        yield host, port
    finally:
        worker.stop()
        master.stop()
        network.remove()
