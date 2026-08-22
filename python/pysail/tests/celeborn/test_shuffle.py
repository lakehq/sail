"""Integration tests for the Celeborn shuffle client actor."""

from __future__ import annotations

import struct
import time
from typing import TYPE_CHECKING

import pytest

from pysail import _native
from pysail.testing.containers.celeborn import CelebornFrame, CelebornMessageType, CelebornStatus
from pysail.testing.utils.proxy import (
    Close,
    ConnectionAccepted,
    ConnectionClosed,
    ConnectionOpened,
    ConnectionRule,
    FrameReceived,
    ProxyEvent,
    RuleApplied,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import (
        MasterService,
        WorkerService,
    )
    from pysail.testing.utils.proxy import EndpointProxy


ShuffleClient = _native._celeborn.ShuffleClient  # noqa: SLF001
LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001
_DATA = b"hello Celeborn"
_REPLICATION_WORKER_COUNT = 2
_MAX_RECOVERY_TIME_SECONDS = 2
_MULTI_EPOCH_WORKER_COUNT = 2


def _add_drop_next_connection(proxies: dict[str, EndpointProxy]) -> None:
    """Install one shared one-shot rule across every worker push endpoint."""
    rule = ConnectionRule(action=lambda _: Close("injected dropped connection"))
    for proxy in proxies.values():
        proxy.rules.add(rule)


def _event_count(
    proxies: dict[str, EndpointProxy],
    event_type: type[ProxyEvent],
    **attributes: object,
) -> int:
    return sum(proxy.events.count(event_type, **attributes) for proxy in proxies.values())


def _event_workers(
    proxies: dict[str, EndpointProxy],
    event_type: type[ProxyEvent],
    **attributes: object,
) -> tuple[str, ...]:
    return tuple(worker for worker, proxy in proxies.items() if proxy.events.count(event_type, **attributes))


def _split_response_workers(
    proxies: dict[str, EndpointProxy],
    status: int,
) -> tuple[str, ...]:
    """Return workers whose real push response reported the requested split status."""
    return tuple(
        worker
        for worker, proxy in proxies.items()
        if any(
            isinstance(event, FrameReceived)
            and event.direction == "server_to_client"
            and isinstance(event.frame, CelebornFrame)
            and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            and event.frame.body == struct.pack(">B", status)
            for event in proxy.events.snapshot()
        )
    )


def _partition_unique_id(frame: CelebornFrame) -> str:
    """Decode the partition ID from a PUSH_DATA transport header."""
    offset = 9  # request ID and partition mode
    shuffle_key_length = struct.unpack_from(">i", frame.metadata, offset)[0]
    offset += 4 + shuffle_key_length
    partition_id_length = struct.unpack_from(">i", frame.metadata, offset)[0]
    offset += 4
    return frame.metadata[offset : offset + partition_id_length].decode()


def _push_partition_ids(proxies: dict[str, EndpointProxy]) -> tuple[str, ...]:
    """Return partition IDs for proxied PUSH_DATA requests in observation order."""
    events = sorted(
        (
            event
            for proxy in proxies.values()
            for event in proxy.events.snapshot()
            if isinstance(event, FrameReceived)
            and event.direction == "client_to_server"
            and isinstance(event.frame, CelebornFrame)
            and event.frame.message_type == CelebornMessageType.PUSH_DATA
        ),
        key=lambda event: event.timestamp,
    )
    return tuple(_partition_unique_id(event.frame) for event in events)


def _split_response_partition_ids(
    proxies: dict[str, EndpointProxy],
    status: int,
) -> tuple[str, ...]:
    """Return partition IDs for PUSH_DATA requests that received a split response."""
    partition_ids = []
    for proxy in proxies.values():
        # The first eight metadata bytes are the request ID echoed by RPC_RESPONSE.
        requests = {
            (event.connection_id, event.frame.metadata[:8]): _partition_unique_id(event.frame)
            for event in proxy.events.snapshot()
            if isinstance(event, FrameReceived)
            and event.direction == "client_to_server"
            and isinstance(event.frame, CelebornFrame)
            and event.frame.message_type == CelebornMessageType.PUSH_DATA
        }
        partition_ids.extend(
            requests[(event.connection_id, event.frame.metadata[:8])]
            for event in proxy.events.snapshot()
            if isinstance(event, FrameReceived)
            and event.direction == "server_to_client"
            and isinstance(event.frame, CelebornFrame)
            and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            and event.frame.body == struct.pack(">B", status)
        )
    return tuple(partition_ids)


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_workers["celeborn-worker-1"].push_port > 0
    with LifecycleManager(
        celeborn_master.host,
        celeborn_master.port,
        "sail-celeborn-shuffle-integration",
        endpoint_resolver=celeborn_endpoint_resolver,
    ) as manager:
        yield manager


@pytest.fixture(scope="module")
def shuffle_client(
    lifecycle_manager: LifecycleManager,
) -> Generator[ShuffleClient, None, None]:
    with ShuffleClient(lifecycle_manager) as client:
        yield client
    assert lifecycle_manager.running


def test_shuffle_client_registers_and_unregisters(
    shuffle_client: ShuffleClient,
    lifecycle_manager: LifecycleManager,
) -> None:
    assert shuffle_client.running
    workers = shuffle_client.register_shuffle(1, [0, 1], False, 1)
    assert sorted(workers) == [
        "celeborn-worker-1:12000:12001:12002:12003",
        "celeborn-worker-2:12000:12001:12002:12003",
    ]
    lifecycle_manager.unregister_shuffle(1)


def test_shuffle_client_pushes_and_reads_partition(
    shuffle_client: ShuffleClient,
    lifecycle_manager: LifecycleManager,
) -> None:
    shuffle_client.register_shuffle(2, [0], False, 1)
    assert shuffle_client.push_data(2, 0, 0, 0, _DATA) == len(_DATA) + 16
    shuffle_client.mapper_end(2, 0, 0, 1)
    assert b"".join(shuffle_client.read_partition_stream(2, 0)) == _DATA
    lifecycle_manager.unregister_shuffle(2)


def test_shuffle_client_commits_after_all_mappers_end(
    shuffle_client: ShuffleClient,
    lifecycle_manager: LifecycleManager,
) -> None:
    shuffle_client.register_shuffle(3, [0], False, 1)
    shuffle_client.push_data(3, 0, 0, 0, b"first map")
    shuffle_client.mapper_end(3, 0, 0, 2)
    shuffle_client.push_data(3, 0, 1, 0, b"second map")
    shuffle_client.mapper_end(3, 1, 0, 2)
    assert b"".join(shuffle_client.read_partition_stream(3, 0)) == b"first mapsecond map"
    lifecycle_manager.unregister_shuffle(3)


def test_shuffle_client_replicates_data(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_endpoint_resolver: object,
) -> None:
    assert celeborn_workers["celeborn-worker-2"].replicate_port > 0
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-replication-integration",
            endpoint_resolver=celeborn_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(4, [0], True, _REPLICATION_WORKER_COUNT)
        assert len(workers) == _REPLICATION_WORKER_COUNT
        assert client.push_data(4, 0, 0, 0, _DATA) == len(_DATA) + 16
        client.mapper_end(4, 0, 0, 1)
        assert b"".join(client.read_partition_stream(4, 0)) == _DATA
        manager.unregister_shuffle(4)


def test_shuffle_client_returns_registration_failure() -> None:
    with (
        LifecycleManager("127.0.0.1", 0, "sail-celeborn-unavailable") as manager,
        ShuffleClient(manager) as client,
        pytest.raises(RuntimeError, match="application error: registration failed: I/O error"),
    ):
        client.register_shuffle(1, [0], False, 1)


def test_shuffle_client_stop_does_not_stop_lifecycle_manager(
    lifecycle_manager: LifecycleManager,
) -> None:
    with ShuffleClient(lifecycle_manager) as client:
        assert client.running
    assert lifecycle_manager.running


@pytest.mark.parametrize(
    ("split_mode", "split_status"),
    [
        ("hard", CelebornStatus.HARD_SPLIT),
        ("soft", CelebornStatus.SOFT_SPLIT),
    ],
    ids=["hard", "soft"],
)
def test_shuffle_client_handles_a_partition_split(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
    split_mode: str,
    split_status: int,
) -> None:
    """A partition split preserves batches and moves future writes to a new epoch."""
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            f"sail-celeborn-split-{split_mode}",
            endpoint_resolver=celeborn_push_endpoint_resolver,
            partition_split_threshold=4,
            partition_split_mode=split_mode,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        client.register_shuffle(5, [0], False, 1)

        batches = []
        for i in range(16):
            probe = f"partition split probe {i}".encode()
            assert client.push_data(5, 0, 0, 0, probe) == len(probe) + 16
            batches.append(probe)
            time.sleep(0.01)
            if _split_response_workers(celeborn_push_proxies, split_status):
                break
        else:
            pytest.fail(f"Celeborn did not emit split status {split_status} after flushing the threshold")

        split_partition_ids = set(_split_response_partition_ids(celeborn_push_proxies, split_status))
        after_split = b"after partition split"
        assert client.push_data(5, 0, 0, 0, after_split) == len(after_split) + 16
        batches.append(after_split)
        assert _push_partition_ids(celeborn_push_proxies)[-1] not in split_partition_ids

        client.mapper_end(5, 0, 0, 1)

        assert _split_response_workers(celeborn_push_proxies, split_status)
        assert b"".join(client.read_partition_stream(5, 0)) == b"".join(batches)


def test_shuffle_client_revives_a_dropped_push_connection(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> None:
    """A failed first push is retried on the other already-running worker."""
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-first-push",
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(1, [0], False, 1)
        assert workers
        _add_drop_next_connection(celeborn_push_proxies)

        started = time.monotonic()
        assert client.push_data(1, 0, 0, 0, _DATA) == len(_DATA) + 16
        assert time.monotonic() - started < _MAX_RECOVERY_TIME_SECONDS
        assert _event_count(celeborn_push_proxies, ConnectionAccepted) == _MULTI_EPOCH_WORKER_COUNT
        assert _event_count(celeborn_push_proxies, RuleApplied) == 1
        rejected_workers = _event_workers(
            celeborn_push_proxies,
            ConnectionClosed,
            reason="injected dropped connection",
        )
        opened_workers = _event_workers(celeborn_push_proxies, ConnectionOpened)
        assert len(rejected_workers) == 1
        assert len(opened_workers) == 1
        assert set(rejected_workers).isdisjoint(opened_workers)
        client.mapper_end(1, 0, 0, 1)

        assert b"".join(client.read_partition_stream(1, 0)) == _DATA


def test_shuffle_client_reads_data_from_epochs_before_and_after_revive(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> None:
    """A recoverable location change must preserve earlier committed epoch data."""
    before_revive = b"epoch zero"
    after_revive = b"epoch one"
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-multi-epoch",
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(2, [0], False, 1)
        assert workers

        assert client.push_data(2, 0, 0, 0, before_revive) == len(before_revive) + 16
        (first_worker,) = _event_workers(celeborn_push_proxies, ConnectionOpened)
        assert sum(proxy.close_active_connections(reason="test") for proxy in celeborn_push_proxies.values()) == 1

        started = time.monotonic()
        assert client.push_data(2, 0, 0, 0, after_revive) == len(after_revive) + 16
        assert time.monotonic() - started < _MAX_RECOVERY_TIME_SECONDS
        assert _event_workers(
            celeborn_push_proxies,
            ConnectionClosed,
            reason="test",
        ) == (first_worker,)
        assert _event_count(celeborn_push_proxies, ConnectionOpened) == _MULTI_EPOCH_WORKER_COUNT
        assert len(_event_workers(celeborn_push_proxies, ConnectionOpened)) == _MULTI_EPOCH_WORKER_COUNT
        client.mapper_end(2, 0, 0, 1)

        assert b"".join(client.read_partition_stream(2, 0)) == before_revive + after_revive


def test_shuffle_client_does_not_reuse_a_worker_that_failed_in_an_earlier_epoch(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> None:
    """A second revive must not route data back to the worker that failed first."""
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-worker-exclusion",
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        client.register_shuffle(3, [0], False, 1)

        assert client.push_data(3, 0, 0, 0, b"epoch zero") == len(b"epoch zero") + 16
        assert sum(proxy.close_active_connections(reason="test") for proxy in celeborn_push_proxies.values()) == 1
        assert client.push_data(3, 0, 0, 0, b"epoch one") == len(b"epoch one") + 16
        # The worker client is reused across partition epochs. Close its connection before
        # the next push so the failed transport is observed by the reused client.
        assert sum(proxy.close_active_connections(reason="test") for proxy in celeborn_push_proxies.values()) == 1
        with pytest.raises(RuntimeError, match="master error: status 27"):
            client.push_data(3, 0, 0, 0, b"epoch two")

        # Each direct close forces a revive after one of the first two epoch pushes.
        assert (
            _event_count(
                celeborn_push_proxies,
                ConnectionClosed,
                reason="test",
            )
            == 2  # noqa: PLR2004
        )
        assert _event_count(celeborn_push_proxies, ConnectionOpened) == 2  # noqa: PLR2004
        assert len(_event_workers(celeborn_push_proxies, ConnectionOpened)) == 2  # noqa: PLR2004


def test_shuffle_client_reader_discovers_epochs_revived_by_another_client(
    celeborn_master: MasterService,
    celeborn_push_endpoint_resolver: object,
    celeborn_push_proxies: dict[str, EndpointProxy],
) -> None:
    """A pre-registered reader must include epochs created by a different writer."""
    before_revive = b"epoch zero"
    after_revive = b"epoch one"
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-reader-location-update",
            endpoint_resolver=celeborn_push_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as writer,
        ShuffleClient(manager) as reader,
    ):
        writer.register_shuffle(4, [0], False, 1)
        reader.register_shuffle(4, [0], False, 1)

        assert writer.push_data(4, 0, 0, 0, before_revive) == len(before_revive) + 16
        assert sum(proxy.close_active_connections(reason="test") for proxy in celeborn_push_proxies.values()) == 1
        assert writer.push_data(4, 0, 0, 0, after_revive) == len(after_revive) + 16
        writer.mapper_end(4, 0, 0, 1)

        assert (
            _event_count(
                celeborn_push_proxies,
                ConnectionClosed,
                reason="test",
            )
            == 1
        )
        assert _event_count(celeborn_push_proxies, ConnectionOpened) == 2  # noqa: PLR2004
        assert len(_event_workers(celeborn_push_proxies, ConnectionOpened)) == 2  # noqa: PLR2004

        # Replication is disabled, so the two payloads live on different workers.
        # Reading their concatenation proves the reader opens both epoch streams.
        assert b"".join(reader.read_partition_stream(4, 0)) == before_revive + after_revive
