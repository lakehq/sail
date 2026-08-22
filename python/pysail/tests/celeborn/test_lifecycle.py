"""Integration tests for the Celeborn lifecycle actor."""

from __future__ import annotations

import struct
import time
from typing import TYPE_CHECKING

import pytest

from pysail import _native
from pysail.testing.containers.celeborn import CelebornFrame, CelebornMessageType
from pysail.testing.utils.proxy import FrameReceived

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import MasterService, WorkerService
    from pysail.testing.utils.proxy import EndpointProxy


LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001
_HEARTBEAT_FROM_APPLICATION_MESSAGE_TYPE = 20
_HEARTBEAT_REQUEST_COUNT = 2
_NATIVE_TRANSPORT_MARKER = 0xFF


def _rpc_message_type(frame: CelebornFrame) -> int:
    """Extract the inner Celeborn control-message type from a native RPC envelope."""
    offset = 1  # protocol version
    sender_host_length = struct.unpack_from(">H", frame.body, offset)[0]
    offset += 2 + sender_host_length + 4  # host and port
    offset += 1  # receiver protocol version
    receiver_host_length = struct.unpack_from(">H", frame.body, offset)[0]
    offset += 2 + receiver_host_length + 4  # host and port
    endpoint_length = struct.unpack_from(">H", frame.body, offset)[0]
    offset += 2 + endpoint_length
    assert frame.body[offset] == _NATIVE_TRANSPORT_MARKER
    return struct.unpack_from(">i", frame.body, offset + 1)[0]


def _heartbeat_requests(proxy: EndpointProxy) -> list[FrameReceived]:
    return [
        event
        for event in proxy.events.snapshot()
        if isinstance(event, FrameReceived)
        and event.direction == "client_to_server"
        and isinstance(event.frame, CelebornFrame)
        and event.frame.message_type == CelebornMessageType.RPC_REQUEST
        and _rpc_message_type(event.frame) == _HEARTBEAT_FROM_APPLICATION_MESSAGE_TYPE
    ]


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_workers["celeborn-worker-1"].rpc_port > 0
    with LifecycleManager(
        celeborn_master.host,
        celeborn_master.port,
        "sail-celeborn-integration",
        endpoint_resolver=celeborn_endpoint_resolver,
    ) as manager:
        yield manager


def test_lifecycle_manager_registers_shuffles_and_unregisters(
    lifecycle_manager: LifecycleManager,
) -> None:
    assert lifecycle_manager.running
    workers = lifecycle_manager.register_shuffle(1, [0, 1], False, 1)
    assert sorted(workers) == [
        "celeborn-worker-1:12000:12001:12002:12003",
        "celeborn-worker-2:12000:12001:12002:12003",
    ]
    lifecycle_manager.unregister_shuffle(1)


def test_lifecycle_manager_returns_registration_failure() -> None:
    with (
        LifecycleManager("127.0.0.1", 0, "sail-celeborn-unavailable") as manager,
        pytest.raises(RuntimeError, match="application error: registration failed: I/O error"),
    ):
        manager.register_shuffle(1, [0], False, 1)


def test_lifecycle_manager_sends_periodic_heartbeats(
    celeborn_master_proxy: EndpointProxy,
) -> None:
    with LifecycleManager(
        celeborn_master_proxy.host,
        celeborn_master_proxy.port,
        "sail-celeborn-heartbeat",
        heartbeat_interval_secs=1,
    ):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if len(_heartbeat_requests(celeborn_master_proxy)) >= _HEARTBEAT_REQUEST_COUNT:
                break
            time.sleep(0.05)

    heartbeat_requests = _heartbeat_requests(celeborn_master_proxy)
    # Immediate and scheduled application-heartbeat RPC requests.
    assert len(heartbeat_requests) >= _HEARTBEAT_REQUEST_COUNT
