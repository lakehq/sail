"""Celeborn container fixtures."""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from collections.abc import Generator

_IMAGE = "apache/celeborn:0.6.3"
_MASTER_PORT = 12097
_MASTER_HTTP_PORT = 12098
_CONFIG_PATH = Path(__file__).with_name("celeborn-defaults.conf")

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@dataclass(frozen=True)
class MasterService:
    host: str
    port: int
    http_port: int


@dataclass(frozen=True)
class WorkerService:
    host: str
    rpc_port: int
    push_port: int
    fetch_port: int
    replicate_port: int


@pytest.fixture(scope="package")
def remote(celeborn_master: MasterService, celeborn_worker: WorkerService) -> Generator[str, None, None]:
    """Run Spark Connect with a Celeborn shuffle backend."""
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ advertised_host = "celeborn-worker", advertised_port = {port}, '
            f'host = "{celeborn_worker.host}", port = {mapped_port} }}'
            for port, mapped_port in [
                (12000, celeborn_worker.rpc_port),
                (12001, celeborn_worker.push_port),
                (12002, celeborn_worker.fetch_port),
                (12003, celeborn_worker.replicate_port),
            ]
        )
    )
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_HOST": celeborn_master.host,
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_PORT": str(celeborn_master.port),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


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
def celeborn_network() -> Generator[Network, None, None]:
    network = Network()
    network.create()
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="package")
def celeborn_master(celeborn_network: Network) -> Generator[MasterService, None, None]:
    master = (
        DockerContainer(_IMAGE)
        .with_kwargs(hostname="celeborn-master")
        .with_env("CELEBORN_LOCAL_HOSTNAME", "celeborn-master")
        .with_env("CELEBORN_MASTER_MEMORY", "512m")
        .with_env("CELEBORN_NO_DAEMONIZE", "1")
        .with_command(["start-master.sh", "--host", "celeborn-master", "--port", str(_MASTER_PORT)])
        .with_volume_mapping(str(_CONFIG_PATH), "/opt/celeborn/conf/celeborn-defaults.conf", "ro")
        .with_exposed_ports(_MASTER_PORT, _MASTER_HTTP_PORT)
        .with_network(celeborn_network)
        .with_network_aliases("celeborn-master")
    )
    try:
        master.start()
        host = master.get_container_host_ip()
        port = int(master.get_exposed_port(_MASTER_PORT))
        http_port = int(master.get_exposed_port(_MASTER_HTTP_PORT))
        _wait_for_port(host, port)
        _wait_for_port(host, http_port)
        yield MasterService(host, port, http_port)
    finally:
        master.stop()


@pytest.fixture(scope="package")
def celeborn_worker(
    celeborn_network: Network,
    celeborn_master: MasterService,
) -> Generator[WorkerService, None, None]:
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
        .with_exposed_ports(12000, 12001, 12002, 12003)
        .with_network(celeborn_network)
        .with_network_aliases("celeborn-worker")
        .waiting_for(LogMessageWaitStrategy("Register worker successfully.").with_startup_timeout(90))
    )
    try:
        worker.start()
        host = celeborn_master.host
        rpc_port = int(worker.get_exposed_port(12000))
        push_port = int(worker.get_exposed_port(12001))
        fetch_port = int(worker.get_exposed_port(12002))
        replicate_port = int(worker.get_exposed_port(12003))
        yield WorkerService(host, rpc_port, push_port, fetch_port, replicate_port)
    finally:
        worker.stop()


@pytest.fixture(scope="package")
def celeborn_replica_worker(
    celeborn_network: Network,
    celeborn_master: MasterService,
    celeborn_worker: WorkerService,
) -> Generator[WorkerService, None, None]:
    assert celeborn_worker.rpc_port > 0
    worker = (
        DockerContainer(_IMAGE)
        .with_kwargs(hostname="celeborn-replica-worker")
        .with_env("CELEBORN_LOCAL_HOSTNAME", "celeborn-replica-worker")
        .with_env("CELEBORN_WORKER_MEMORY", "512m")
        .with_env("CELEBORN_WORKER_OFFHEAP_MEMORY", "512m")
        .with_env("CELEBORN_NO_DAEMONIZE", "1")
        .with_command(
            [
                "start-worker.sh",
                "--host",
                "celeborn-replica-worker",
                "--port",
                "12000",
                "celeborn://celeborn-master:12097",
            ]
        )
        .with_volume_mapping(str(_CONFIG_PATH), "/opt/celeborn/conf/celeborn-defaults.conf", "ro")
        .with_exposed_ports(12000, 12001, 12002, 12003)
        .with_network(celeborn_network)
        .with_network_aliases("celeborn-replica-worker")
        .waiting_for(LogMessageWaitStrategy("Register worker successfully.").with_startup_timeout(90))
    )
    try:
        worker.start()
        host = celeborn_master.host
        rpc_port = int(worker.get_exposed_port(12000))
        push_port = int(worker.get_exposed_port(12001))
        fetch_port = int(worker.get_exposed_port(12002))
        replicate_port = int(worker.get_exposed_port(12003))
        yield WorkerService(host, rpc_port, push_port, fetch_port, replicate_port)
    finally:
        worker.stop()


def _endpoint_resolver(overrides: dict[tuple[str, int], tuple[str, int]]) -> object:
    from pysail import _native

    return _native._celeborn.StaticEndpointResolver(overrides)  # noqa: SLF001


@pytest.fixture(scope="package")
def endpoint_resolver(celeborn_worker: WorkerService) -> object:
    """Map Docker-network worker endpoints to the host-published ports."""
    return _endpoint_resolver(
        {
            ("celeborn-worker", 12000): (celeborn_worker.host, celeborn_worker.rpc_port),
            ("celeborn-worker", 12001): (celeborn_worker.host, celeborn_worker.push_port),
            ("celeborn-worker", 12002): (celeborn_worker.host, celeborn_worker.fetch_port),
        }
    )


@pytest.fixture(scope="package")
def replication_endpoint_resolver(
    celeborn_worker: WorkerService,
    celeborn_replica_worker: WorkerService,
) -> object:
    """Resolve each worker advertised on the Docker network to its published ports."""
    return _endpoint_resolver(
        {
            ("celeborn-worker", 12000): (celeborn_worker.host, celeborn_worker.rpc_port),
            ("celeborn-worker", 12001): (celeborn_worker.host, celeborn_worker.push_port),
            ("celeborn-worker", 12002): (celeborn_worker.host, celeborn_worker.fetch_port),
            ("celeborn-replica-worker", 12000): (
                celeborn_replica_worker.host,
                celeborn_replica_worker.rpc_port,
            ),
            ("celeborn-replica-worker", 12001): (
                celeborn_replica_worker.host,
                celeborn_replica_worker.push_port,
            ),
            ("celeborn-replica-worker", 12002): (
                celeborn_replica_worker.host,
                celeborn_replica_worker.fetch_port,
            ),
        }
    )
