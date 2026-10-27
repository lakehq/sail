"""Celeborn container fixtures."""

from __future__ import annotations

import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pysail.testing.utils.proxy import EndpointProxy, FrameDecoder, ProxyCodec

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


class CelebornMessageType:
    """Celeborn transport message type identifiers used by the Netty codec."""

    CHUNK_FETCH_REQUEST = 0
    CHUNK_FETCH_SUCCESS = 1
    CHUNK_FETCH_FAILURE = 2
    RPC_REQUEST = 3
    RPC_RESPONSE = 4
    RPC_FAILURE = 5
    OPEN_STREAM = 6
    STREAM_HANDLE = 7
    ONE_WAY_MESSAGE = 9
    PUSH_DATA = 11
    PUSH_MERGED_DATA = 12
    REGION_START = 13
    REGION_FINISH = 14
    PUSH_DATA_HAND_SHAKE = 15
    READ_ADD_CREDIT = 16
    READ_DATA = 17
    OPEN_STREAM_WITH_CREDIT = 18
    BACKLOG_ANNOUNCEMENT = 19
    TRANSPORTABLE_ERROR = 20
    BUFFER_STREAM_END = 21
    HEARTBEAT = 22
    SEGMENT_START = 23
    NOTIFY_REQUIRED_SEGMENT = 24
    SUBPARTITION_READ_DATA = 25


class CelebornStatus:
    """Celeborn worker response status codes used by split tests."""

    HARD_SPLIT = 21
    SOFT_SPLIT = 22


@dataclass(frozen=True)
class CelebornFrame:
    """One complete Celeborn Netty transport frame.

    ``metadata`` is the encoded ``Message`` header and ``body`` is the optional
    managed-buffer payload.  They are intentionally left opaque so a test can
    inspect, replace, or mutate them without reimplementing every Celeborn
    transport message.
    """

    message_type: int
    metadata: bytes
    body: bytes


class CelebornFrameDecoder(FrameDecoder[CelebornFrame]):
    """Decode Celeborn transport frames."""

    _HEADER_SIZE = 9
    _MAX_FRAME_SIZE = 2**31 - 1

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[CelebornFrame]:
        self._buffer.extend(data)
        frames: list[CelebornFrame] = []
        while len(self._buffer) >= self._HEADER_SIZE:
            metadata_size, message_type, body_size = struct.unpack_from(">iBi", self._buffer)
            frame_size = metadata_size + body_size
            if metadata_size < 0 or body_size < 0 or not 0 < frame_size < self._MAX_FRAME_SIZE:
                msg = f"invalid Celeborn frame size: metadata={metadata_size}, body={body_size}"
                raise ValueError(msg)
            total_size = self._HEADER_SIZE + frame_size
            if len(self._buffer) < total_size:
                break
            metadata_end = self._HEADER_SIZE + metadata_size
            frames.append(
                CelebornFrame(
                    message_type=message_type,
                    metadata=bytes(self._buffer[self._HEADER_SIZE : metadata_end]),
                    body=bytes(self._buffer[metadata_end:total_size]),
                )
            )
            del self._buffer[:total_size]
        return frames


class CelebornCodec(ProxyCodec[CelebornFrame]):
    """Codec for Celeborn's framed Netty transport protocol."""

    def decoder(self, direction: str) -> CelebornFrameDecoder:
        del direction
        return CelebornFrameDecoder()

    def encode(self, frame: CelebornFrame) -> bytes:
        return (
            struct.pack(
                ">iBi",
                len(frame.metadata),
                frame.message_type,
                len(frame.body),
            )
            + frame.metadata
            + frame.body
        )


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
def celeborn_frame_codec() -> CelebornCodec:
    """Provide the codec used to inspect Celeborn transport frames."""
    return CelebornCodec()


@pytest.fixture
def celeborn_master_proxy(
    celeborn_master: MasterService,
    celeborn_frame_codec: CelebornCodec,
) -> Generator[EndpointProxy, None, None]:
    """Forward master traffic through a proxy so application heartbeats can be observed."""
    proxy = EndpointProxy(
        name="celeborn-master",
        target_host=celeborn_master.host,
        target_port=celeborn_master.port,
        codec=celeborn_frame_codec,
    )
    proxy.start()
    try:
        yield proxy
    finally:
        proxy.close()


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
def celeborn_push_proxies(
    celeborn_workers: dict[str, WorkerService],
    celeborn_frame_codec: CelebornCodec,
) -> Generator[dict[str, EndpointProxy], None, None]:
    """Forward worker push traffic through general purpose endpoint proxies."""
    proxies = {
        name: EndpointProxy(
            name=f"{name}:push",
            target_host=worker.host,
            target_port=worker.push_port,
            codec=celeborn_frame_codec,
        )
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
def celeborn_push_endpoint_resolver(
    celeborn_workers: dict[str, WorkerService],
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> object:
    """Resolve worker endpoints through the general purpose push proxies."""
    return _celeborn_endpoint_resolver(
        {(name, 12000): (worker.host, worker.rpc_port) for name, worker in celeborn_workers.items()}
        | {
            (name, 12001): (
                celeborn_push_proxies[name].host,
                celeborn_push_proxies[name].port,
            )
            for name in celeborn_workers
        }
        | {(name, 12002): (worker.host, worker.fetch_port) for name, worker in celeborn_workers.items()}
    )
