"""Exercise transport response matching against real Celeborn services."""

from __future__ import annotations

import struct
import threading
from typing import TYPE_CHECKING

import pytest

from pysail.testing._dev.containers.celeborn import CelebornFrame, CelebornMessageType
from pysail.testing._dev.utils.proxy import ConnectionOpened, FrameReceived, FrameRule, ReplaceMany, RuleApplied

if TYPE_CHECKING:
    from pysail.testing._dev.containers.celeborn import MasterService
    from pysail.testing._dev.utils.proxy import EndpointProxy


def _rpc_failure(request_id: int) -> CelebornFrame:
    error = b"injected RPC failure"
    return CelebornFrame(
        CelebornMessageType.RPC_FAILURE,
        struct.pack(">qi", request_id, len(error)) + error,
        b"",
    )


@pytest.fixture(
    params=[
        pytest.param((CelebornMessageType.RPC_RESPONSE, 0), id="duplicate-response"),
        pytest.param((CelebornMessageType.RPC_FAILURE, 0), id="duplicate-failure"),
        pytest.param((CelebornMessageType.RPC_RESPONSE, 1_000_000), id="later-response-id"),
        pytest.param((CelebornMessageType.RPC_FAILURE, 1_000_000), id="later-failure-id"),
    ]
)
def repeated_rpc_reply(request: pytest.FixtureRequest) -> FrameRule[CelebornFrame]:
    """Leave an extra RPC reply for the next transaction on the same connection."""
    # IDs can be allocated before acquiring the connection lock, so a stale reply
    # can have a larger ID than the active request.
    reply_type, request_id_offset = request.param

    def repeat(event: FrameReceived[CelebornFrame]) -> ReplaceMany[CelebornFrame]:
        frame = event.frame
        request_id = struct.unpack_from(">q", frame.metadata)[0] + request_id_offset
        if reply_type == CelebornMessageType.RPC_FAILURE:
            repeated = _rpc_failure(request_id)
        else:
            repeated = CelebornFrame(
                reply_type,
                struct.pack(">q", request_id) + frame.metadata[8:],
                frame.body,
            )
        return ReplaceMany((frame, repeated))

    return FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: (
            event.direction == "server_to_client" and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
        ),
        action=repeat,
    )


@pytest.fixture
def transport_application_id(request: pytest.FixtureRequest) -> str:
    return f"sail-celeborn-{request.node.name}"


def test_rpc_skips_unrelated_replies(
    celeborn_master_proxy: EndpointProxy,
    celeborn_endpoint_resolver: object,
    repeated_rpc_reply: FrameRule[CelebornFrame],
    transport_application_id: str,
) -> None:
    from pysail import _native

    celeborn_master_proxy.rules.add(repeated_rpc_reply)
    with _native._celeborn.LifecycleManager(  # noqa: SLF001
        [f"{celeborn_master_proxy.host}:{celeborn_master_proxy.port}"],
        transport_application_id,
        endpoint_resolver=celeborn_endpoint_resolver,
    ) as manager:
        assert manager.register_shuffle(1, [0], False, 1)
        assert manager.register_shuffle(2, [0], False, 1)
        manager.unregister_shuffle(1)
        manager.unregister_shuffle(2)
        assert celeborn_master_proxy.events.count(RuleApplied) == 1
        assert celeborn_master_proxy.events.count(ConnectionOpened) == 1


def test_rpc_reuses_connection_after_duplicate_failures(
    celeborn_master_proxy: EndpointProxy,
    celeborn_endpoint_resolver: object,
    transport_application_id: str,
) -> None:
    from pysail import _native

    injecting = threading.Event()

    def fail_twice(event: FrameReceived[CelebornFrame]) -> ReplaceMany[CelebornFrame]:
        failure = _rpc_failure(struct.unpack_from(">q", event.frame.metadata)[0])
        return ReplaceMany((failure, failure))

    celeborn_master_proxy.rules.add(
        FrameRule(
            frame_type=CelebornFrame,
            condition=lambda event: (
                injecting.is_set()
                and event.direction == "server_to_client"
                and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            ),
            action=fail_twice,
            count=None,
        )
    )
    with _native._celeborn.LifecycleManager(  # noqa: SLF001
        [f"{celeborn_master_proxy.host}:{celeborn_master_proxy.port}"],
        transport_application_id,
        endpoint_resolver=celeborn_endpoint_resolver,
    ) as manager:
        assert manager.register_shuffle(1, [0], False, 1)
        injecting.set()
        try:
            with pytest.raises(RuntimeError, match="injected RPC failure"):
                manager.unregister_shuffle(1)
        finally:
            injecting.clear()
        assert manager.register_shuffle(2, [0], False, 1)
        manager.unregister_shuffle(2)
        assert celeborn_master_proxy.events.count(RuleApplied) >= 1
        assert celeborn_master_proxy.events.count(ConnectionOpened) == 1


def test_push_skips_unrelated_replies(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
    repeated_rpc_reply: FrameRule[CelebornFrame],
    transport_application_id: str,
) -> None:
    from pysail import _native

    for proxy in celeborn_push_proxies.values():
        proxy.rules.add(repeated_rpc_reply)
    with (
        _native._celeborn.LifecycleManager(  # noqa: SLF001
            [f"{celeborn_master.host}:{celeborn_master.port}"],
            transport_application_id,
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        _native._celeborn.ShuffleClient(manager) as client,  # noqa: SLF001
    ):
        client.register_shuffle(1, [0], False, 1)
        batches = [b"first push", b"second push"]
        for batch in batches:
            assert client.push_data(1, 0, 0, 0, batch) == len(batch) + 16
        client.mapper_end(1, 0, 0, 1)
        assert list(client.read_partition_stream(1, 0)) == batches
        # Recovery on another worker must not mask a failure to skip the stale reply.
        assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_push_proxies.values()) == 1
        assert sum(proxy.events.count(RuleApplied) for proxy in celeborn_push_proxies.values()) == 1


def test_chunk_fetch_skips_unrelated_rpc_replies(
    celeborn_master: MasterService,
    celeborn_fetch_endpoint_resolver: object,
    celeborn_fetch_proxies: dict[str, EndpointProxy],
    repeated_rpc_reply: FrameRule[CelebornFrame],
    transport_application_id: str,
) -> None:
    from pysail import _native

    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(repeated_rpc_reply)
    with (
        _native._celeborn.LifecycleManager(  # noqa: SLF001
            [f"{celeborn_master.host}:{celeborn_master.port}"],
            transport_application_id,
            endpoint_resolver=celeborn_fetch_endpoint_resolver,
        ) as manager,
        _native._celeborn.ShuffleClient(manager) as client,  # noqa: SLF001
    ):
        client.register_shuffle(1, [0], False, 1)
        client.push_data(1, 0, 0, 0, b"shuffle data")
        client.mapper_end(1, 0, 0, 1)
        # The duplicate OpenStream reply precedes the first chunk. A second read also
        # checks that another OpenStream RPC can reuse the same fetch connection.
        assert list(client.read_partition_stream(1, 0)) == [b"shuffle data"]
        assert list(client.read_partition_stream(1, 0)) == [b"shuffle data"]
        assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_fetch_proxies.values()) == 1
        assert sum(proxy.events.count(RuleApplied) for proxy in celeborn_fetch_proxies.values()) == 1
