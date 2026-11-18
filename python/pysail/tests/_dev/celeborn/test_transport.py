"""Exercise transport response matching against real Celeborn services."""

from __future__ import annotations

import struct
import threading
from typing import TYPE_CHECKING

import pytest

from pysail.testing._dev.containers.celeborn import CelebornFrame, CelebornMessageType
from pysail.testing._dev.utils.proxy import (
    ConnectionOpened,
    FrameReceived,
    FrameRule,
    Replace,
    ReplaceMany,
    RuleApplied,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from pysail import _native
    from pysail.testing._dev.containers.celeborn import MasterService
    from pysail.testing._dev.utils.proxy import EndpointProxy


def _rpc_failure(request_id: int) -> CelebornFrame:
    error = b"injected RPC failure"
    return CelebornFrame(
        CelebornMessageType.RPC_FAILURE,
        struct.pack(">qi", request_id, len(error)) + error,
        b"",
    )


def _fail_twice(event: FrameReceived[CelebornFrame]) -> ReplaceMany[CelebornFrame]:
    failure = _rpc_failure(struct.unpack_from(">q", event.frame.metadata)[0])
    return ReplaceMany((failure, failure))


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

    celeborn_master_proxy.rules.add(
        FrameRule(
            frame_type=CelebornFrame,
            condition=lambda event: (
                injecting.is_set()
                and event.direction == "server_to_client"
                and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            ),
            action=_fail_twice,
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


@pytest.fixture
def push_client(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    transport_application_id: str,
) -> Iterator[_native._celeborn.ShuffleClient]:
    from pysail import _native

    with (
        _native._celeborn.LifecycleManager(  # noqa: SLF001
            [f"{celeborn_master.host}:{celeborn_master.port}"],
            transport_application_id,
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        _native._celeborn.ShuffleClient(manager) as client,  # noqa: SLF001
    ):
        client.register_shuffle(1, [0], False, 1)
        yield client


def test_push_skips_unrelated_replies(
    push_client: _native._celeborn.ShuffleClient,
    celeborn_push_proxies: dict[str, EndpointProxy],
    repeated_rpc_reply: FrameRule[CelebornFrame],
) -> None:
    for proxy in celeborn_push_proxies.values():
        proxy.rules.add(repeated_rpc_reply)
    batches = [b"first push", b"second push"]
    for batch in batches:
        assert push_client.push_data(1, 0, 0, 0, batch) == len(batch) + 16
    push_client.mapper_end(1, 0, 0, 1)
    assert list(push_client.read_partition_stream(1, 0)) == batches
    # Recovery on another worker must not mask a failure to skip the stale reply.
    assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_push_proxies.values()) == 1
    assert sum(proxy.events.count(RuleApplied) for proxy in celeborn_push_proxies.values()) == 1


def test_push_reuses_connection_after_duplicate_failures(
    push_client: _native._celeborn.ShuffleClient,
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> None:
    rule = FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: (
            event.direction == "server_to_client" and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
        ),
        action=_fail_twice,
    )
    for proxy in celeborn_push_proxies.values():
        proxy.rules.add(rule)
    with pytest.raises(RuntimeError, match="injected RPC failure"):
        push_client.push_data(1, 0, 0, 0, b"first")
    assert push_client.push_data(1, 0, 0, 0, b"second") == len(b"second") + 16
    push_client.mapper_end(1, 0, 0, 1)
    # The worker received both batches before the proxy replaced the first acknowledgement.
    assert list(push_client.read_partition_stream(1, 0)) == [b"first", b"second"]
    assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_push_proxies.values()) == 1


@pytest.fixture
def fetch_client(
    celeborn_master: MasterService,
    celeborn_fetch_endpoint_resolver: object,
    transport_application_id: str,
) -> Iterator[_native._celeborn.ShuffleClient]:
    from pysail import _native

    with (
        _native._celeborn.LifecycleManager(  # noqa: SLF001
            [f"{celeborn_master.host}:{celeborn_master.port}"],
            transport_application_id,
            endpoint_resolver=celeborn_fetch_endpoint_resolver,
            compression="none",
        ) as manager,
        _native._celeborn.ShuffleClient(manager) as client,  # noqa: SLF001
    ):
        client.register_shuffle(1, [0], False, 1)
        yield client


@pytest.fixture
def read_partition(fetch_client: _native._celeborn.ShuffleClient) -> Callable[[], list[bytes]]:
    fetch_client.push_data(1, 0, 0, 0, b"shuffle data")
    fetch_client.mapper_end(1, 0, 0, 1)

    def read() -> list[bytes]:
        return list(fetch_client.read_partition_stream(1, 0))

    return read


def test_chunk_fetch_skips_unrelated_rpc_replies(
    read_partition: Callable[[], list[bytes]],
    celeborn_fetch_proxies: dict[str, EndpointProxy],
    repeated_rpc_reply: FrameRule[CelebornFrame],
) -> None:
    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(repeated_rpc_reply)
    # The duplicate OpenStream reply precedes the first chunk. A second read also
    # checks that another OpenStream RPC can reuse the same fetch connection.
    assert read_partition() == [b"shuffle data"]
    assert read_partition() == [b"shuffle data"]
    assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_fetch_proxies.values()) == 1
    assert sum(proxy.events.count(RuleApplied) for proxy in celeborn_fetch_proxies.values()) == 1


def test_rpc_accepts_a_lower_request_id(
    read_partition: Callable[[], list[bytes]],
    celeborn_fetch_proxies: dict[str, EndpointProxy],
) -> None:
    from pysail import _native

    assert read_partition() == [b"shuffle data"]

    # Replay a valid OpenStream request. Each call allocates a distinct stream at the worker.
    [(proxy, open_stream)] = [
        (proxy, event)
        for proxy in celeborn_fetch_proxies.values()
        for event in proxy.events.snapshot()
        if isinstance(event, FrameReceived) and event.frame.message_type == CelebornMessageType.RPC_REQUEST
    ]
    proxy.rules.add(
        FrameRule(
            frame_type=CelebornFrame,
            condition=lambda event: (
                event.direction == "server_to_client" and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            ),
            action=lambda event: ReplaceMany((event.frame, _rpc_failure(1))),
        )
    )
    connection = _native._celeborn.RpcConnection(proxy.host, proxy.port)  # noqa: SLF001
    first = connection.send_rpc(1, open_stream.frame.body)
    second = connection.send_rpc(0, open_stream.frame.body)
    assert first != second
    assert proxy.events.count(RuleApplied) == 1
    responses = [
        event
        for event in proxy.events.snapshot()
        if isinstance(event, FrameReceived)
        and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
        and event.connection_id != open_stream.connection_id
    ]
    assert responses[0].connection_id == responses[1].connection_id
    assert [(struct.unpack_from(">q", e.frame.metadata)[0], e.frame.body) for e in responses] == [
        (1, first),
        (0, second),
    ]


@pytest.mark.parametrize(
    ("message_type", "field_offset", "error"),
    [
        pytest.param(
            CelebornMessageType.CHUNK_FETCH_SUCCESS, 8, "chunk response does not match request", id="chunk-index"
        ),
        pytest.param(
            CelebornMessageType.CHUNK_FETCH_SUCCESS, 16, "chunk response does not match request", id="slice-length"
        ),
        pytest.param(CelebornMessageType.RPC_RESPONSE, 8, "RPC response length", id="rpc-length"),
    ],
)
def test_fetch_discards_connection_after_invalid_reply(
    read_partition: Callable[[], list[bytes]],
    celeborn_fetch_proxies: dict[str, EndpointProxy],
    message_type: int,
    field_offset: int,
    error: str,
) -> None:
    def corrupt(event: FrameReceived[CelebornFrame]) -> ReplaceMany[CelebornFrame]:
        frame = event.frame
        metadata = bytearray(frame.metadata)
        struct.pack_into(">i", metadata, field_offset, 1)
        return ReplaceMany((CelebornFrame(frame.message_type, bytes(metadata), frame.body), frame))

    rule = FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: event.direction == "server_to_client" and event.frame.message_type == message_type,
        action=corrupt,
    )
    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(rule)
    with pytest.raises(RuntimeError, match=error):
        read_partition()
    assert read_partition() == [b"shuffle data"]
    expected_connections = 2  # The invalid reply closes the original connection.
    assert (
        sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_fetch_proxies.values()) == expected_connections
    )


@pytest.mark.parametrize(
    ("length_adjustment", "error", "connections"),
    [(0, "injected chunk failure", 1), (1, "invalid failure message length", 2)],
    ids=["matching-failure", "malformed-failure"],
)
def test_fetch_validates_chunk_failure(
    read_partition: Callable[[], list[bytes]],
    celeborn_fetch_proxies: dict[str, EndpointProxy],
    length_adjustment: int,
    error: str,
    connections: int,
) -> None:
    def fail_chunk(event: FrameReceived[CelebornFrame]) -> Replace[CelebornFrame]:
        message = b"injected chunk failure"
        header = event.frame.metadata + struct.pack(">i", len(message) + length_adjustment) + message
        return Replace(CelebornFrame(CelebornMessageType.CHUNK_FETCH_FAILURE, header, b""))

    rule = FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: (
            event.direction == "server_to_client"
            and event.frame.message_type == CelebornMessageType.CHUNK_FETCH_SUCCESS
        ),
        action=fail_chunk,
    )
    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(rule)
    with pytest.raises(RuntimeError, match=error):
        read_partition()
    assert read_partition() == [b"shuffle data"]
    assert sum(proxy.events.count(ConnectionOpened) for proxy in celeborn_fetch_proxies.values()) == connections


def test_fetch_preserves_partial_batch_header(
    fetch_client: _native._celeborn.ShuffleClient,
    celeborn_fetch_proxies: dict[str, EndpointProxy],
) -> None:
    # Fill one worker chunk, then move the first four bytes of the second batch header
    # across the chunk boundary. The concatenated shuffle data stays identical.
    first = b"x" * (64 * 1024)
    second = b"second batch"
    map_id = 1

    def split_header(event: FrameReceived[CelebornFrame]) -> Replace[CelebornFrame]:
        frame = event.frame
        chunk_index = struct.unpack_from(">i", frame.metadata, 8)[0]
        body = frame.body + struct.pack("=i", map_id) if chunk_index == 0 else frame.body[4:]
        return Replace(CelebornFrame(frame.message_type, frame.metadata, body))

    expected_chunks = 2
    rule = FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: (
            event.direction == "server_to_client"
            and event.frame.message_type == CelebornMessageType.CHUNK_FETCH_SUCCESS
        ),
        action=split_header,
        count=expected_chunks,
    )
    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(rule)
    for batch in [first, second]:
        fetch_client.push_data(1, 0, map_id, 0, batch)
    fetch_client.mapper_end(1, 0, 0, 2)
    fetch_client.mapper_end(1, map_id, 0, 2)
    assert list(fetch_client.read_partition_stream(1, 0)) == [first, second]
    assert sum(proxy.events.count(RuleApplied) for proxy in celeborn_fetch_proxies.values()) == expected_chunks
