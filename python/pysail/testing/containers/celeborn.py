"""Celeborn container fixtures."""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
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
_MASTER_HTTP_PORT = 12098
_CONFIG_PATH = Path(__file__).with_name("celeborn-defaults.conf")
_WORKER_NAMES = ("celeborn-worker-1", "celeborn-worker-2")


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


@dataclass(frozen=True)
class PushFaultSnapshot:
    dropped_workers: tuple[str, ...]
    forwarded_workers: tuple[str, ...]
    disconnected_workers: tuple[str, ...]


class PushFaultController:
    """Coordinates one injected failure across all worker push proxies."""

    def __init__(self) -> None:
        self._drop_next_connection = False
        self._lock = threading.Lock()
        self._dropped_workers: list[str] = []
        self._forwarded_workers: list[str] = []
        self._disconnected_workers: list[str] = []

    def reset(self) -> None:
        with self._lock:
            self._drop_next_connection = False
            self._dropped_workers.clear()
            self._forwarded_workers.clear()
            self._disconnected_workers.clear()

    def drop_next_connection(self) -> None:
        with self._lock:
            self._drop_next_connection = True

    def consume_drop_next_connection(self, worker: str) -> bool:
        with self._lock:
            drop_connection = self._drop_next_connection
            self._drop_next_connection = False
            if drop_connection:
                self._dropped_workers.append(worker)
            return drop_connection

    def record_forward(self, worker: str) -> None:
        with self._lock:
            self._forwarded_workers.append(worker)

    def record_disconnect(self, worker: str) -> None:
        with self._lock:
            self._disconnected_workers.append(worker)

    def snapshot(self) -> PushFaultSnapshot:
        with self._lock:
            return PushFaultSnapshot(
                dropped_workers=tuple(self._dropped_workers),
                forwarded_workers=tuple(self._forwarded_workers),
                disconnected_workers=tuple(self._disconnected_workers),
            )


class FaultInjectingTcpProxy:
    """A local TCP proxy that can reject one subsequent client connection.

    The proxy lets integration tests simulate a failed push connection without
    stopping a worker or waiting for Celeborn's worker-timeout detection.
    """

    def __init__(
        self,
        worker: str,
        target_host: str,
        target_port: int,
        fault_controller: PushFaultController,
    ) -> None:
        self._worker = worker
        self._target = (target_host, target_port)
        self._fault_controller = fault_controller
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self._listener.settimeout(0.1)
        self._lock = threading.Lock()
        self._clients: set[socket.socket] = set()
        self._closed = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        return int(self._listener.getsockname()[1])

    def start(self) -> None:
        self._thread.start()

    def disconnect_clients(self) -> None:
        """Close active client connections so the next push creates a new one."""
        with self._lock:
            clients = tuple(self._clients)
        for client in clients:
            self._fault_controller.record_disconnect(self._worker)
            with suppress(OSError):
                client.shutdown(socket.SHUT_RDWR)
            client.close()

    def close(self) -> None:
        self._closed.set()
        self._listener.close()
        self._thread.join(timeout=1)

    def _serve(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            if self._fault_controller.consume_drop_next_connection(self._worker):
                client.close()
                continue
            try:
                upstream = socket.create_connection(self._target, timeout=1)
            except OSError:
                client.close()
                continue
            self._fault_controller.record_forward(self._worker)
            with self._lock:
                self._clients.add(client)
            threading.Thread(target=self._relay, args=(client, upstream), daemon=True).start()

    def _relay(self, client: socket.socket, upstream: socket.socket) -> None:
        def copy(source: socket.socket, destination: socket.socket) -> None:
            try:
                while data := source.recv(64 * 1024):
                    destination.sendall(data)
            except OSError:
                pass
            finally:
                with suppress(OSError):
                    destination.shutdown(socket.SHUT_WR)

        client_to_upstream = threading.Thread(target=copy, args=(client, upstream), daemon=True)
        upstream_to_client = threading.Thread(target=copy, args=(upstream, client), daemon=True)
        client_to_upstream.start()
        upstream_to_client.start()
        client_to_upstream.join()
        upstream_to_client.join()
        with self._lock:
            self._clients.discard(client)
        client.close()
        upstream.close()


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


@pytest.fixture(scope="session")
def celeborn_network() -> Generator[Network, None, None]:
    network = Network()
    network.create()
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="session")
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


def _create_worker(celeborn_network: Network, worker_name: str) -> DockerContainer:
    return (
        DockerContainer(_IMAGE)
        .with_kwargs(hostname=worker_name)
        .with_env("CELEBORN_LOCAL_HOSTNAME", worker_name)
        .with_env("CELEBORN_WORKER_MEMORY", "512m")
        .with_env("CELEBORN_WORKER_OFFHEAP_MEMORY", "512m")
        .with_env("CELEBORN_NO_DAEMONIZE", "1")
        .with_command(
            [
                "start-worker.sh",
                "--host",
                worker_name,
                "--port",
                "12000",
                "celeborn://celeborn-master:12097",
            ]
        )
        .with_volume_mapping(str(_CONFIG_PATH), "/opt/celeborn/conf/celeborn-defaults.conf", "ro")
        .with_exposed_ports(12000, 12001, 12002, 12003)
        .with_network(celeborn_network)
        .with_network_aliases(worker_name)
        .waiting_for(LogMessageWaitStrategy("Register worker successfully.").with_startup_timeout(90))
    )


@pytest.fixture(scope="session")
def celeborn_workers(
    celeborn_network: Network,
    celeborn_master: MasterService,
) -> Generator[dict[str, WorkerService], None, None]:
    """Start the independent Celeborn workers concurrently after the master is ready."""
    containers = {name: _create_worker(celeborn_network, name) for name in _WORKER_NAMES}
    try:
        with ThreadPoolExecutor(max_workers=len(containers)) as executor:
            futures = {name: executor.submit(worker.start) for name, worker in containers.items()}
            for future in futures.values():
                future.result()
        yield {
            name: WorkerService(
                host=celeborn_master.host,
                rpc_port=int(worker.get_exposed_port(12000)),
                push_port=int(worker.get_exposed_port(12001)),
                fetch_port=int(worker.get_exposed_port(12002)),
                replicate_port=int(worker.get_exposed_port(12003)),
            )
            for name, worker in containers.items()
        }
    finally:
        with ThreadPoolExecutor(max_workers=len(containers)) as executor:
            futures = [executor.submit(worker.stop) for worker in containers.values()]
            for future in futures:
                future.result()


def _celeborn_endpoint_resolver(overrides: dict[tuple[str, int], tuple[str, int]]) -> object:
    from pysail import _native  # noqa: PLC0415

    return _native._celeborn.StaticEndpointResolver(overrides)  # noqa: SLF001


@pytest.fixture(scope="session")
def celeborn_endpoint_resolver(celeborn_workers: dict[str, WorkerService]) -> object:
    """Map Docker-network worker endpoints to the host-published ports."""
    return _celeborn_endpoint_resolver(
        {
            (name, port): (worker.host, mapped_port)
            for name, worker in celeborn_workers.items()
            for port, mapped_port in [
                (12000, worker.rpc_port),
                (12001, worker.push_port),
                (12002, worker.fetch_port),
            ]
        }
    )


@pytest.fixture
def celeborn_push_fault_controller() -> PushFaultController:
    return PushFaultController()


@pytest.fixture
def celeborn_push_fault_proxies(
    celeborn_workers: dict[str, WorkerService],
    celeborn_push_fault_controller: PushFaultController,
) -> Generator[dict[str, FaultInjectingTcpProxy], None, None]:
    """Forward worker push traffic while allowing a test to drop one connection."""
    proxies = {
        name: FaultInjectingTcpProxy(name, worker.host, worker.push_port, celeborn_push_fault_controller)
        for name, worker in celeborn_workers.items()
    }
    for proxy in proxies.values():
        proxy.start()
    try:
        yield proxies
    finally:
        for proxy in proxies.values():
            proxy.close()


@pytest.fixture
def celeborn_push_fault_endpoint_resolver(
    celeborn_workers: dict[str, WorkerService],
    celeborn_push_fault_proxies: dict[str, FaultInjectingTcpProxy],
) -> object:
    """Resolve worker endpoints through fault-injectable push proxies."""
    return _celeborn_endpoint_resolver(
        {(name, 12000): (worker.host, worker.rpc_port) for name, worker in celeborn_workers.items()}
        | {
            (name, 12001): (
                celeborn_push_fault_proxies[name].host,
                celeborn_push_fault_proxies[name].port,
            )
            for name in celeborn_workers
        }
        | {(name, 12002): (worker.host, worker.fetch_port) for name, worker in celeborn_workers.items()}
    )
