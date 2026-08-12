"""Integration tests for the Celeborn shuffle client actor."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from pysail import _native

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import (
        FaultInjectingTcpProxy,
        MasterService,
        PushFaultController,
        PushFaultSnapshot,
        WorkerService,
    )


ShuffleClient = _native._celeborn.ShuffleClient  # noqa: SLF001
LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001
_DATA = b"hello Celeborn"
_REPLICATION_WORKER_COUNT = 2
_MAX_RECOVERY_TIME_SECONDS = 2
_MULTI_EPOCH_WORKER_COUNT = 2


def _assert_revive_route(snapshot: PushFaultSnapshot) -> None:
    assert len(snapshot.dropped_workers) == 1
    assert len(snapshot.forwarded_workers) == 1
    assert snapshot.dropped_workers[0] != snapshot.forwarded_workers[0]


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_workers["celeborn-worker-1"].push_port > 0
    with LifecycleManager(
        celeborn_master.host,
        celeborn_master.port,
        "sail-celeborn-shuffle-integration",
        endpoint_resolver,
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
    assert workers == [
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
    endpoint_resolver: object,
) -> None:
    assert celeborn_workers["celeborn-worker-2"].replicate_port > 0
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-replication-integration",
            endpoint_resolver,
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


def test_shuffle_client_revives_a_dropped_push_connection(
    celeborn_master: MasterService,
    recovery_endpoint_resolver: object,
    push_fault_controller: PushFaultController,
) -> None:
    """A failed first push is retried on the other already-running worker."""
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-first-push",
            recovery_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(1, [0], False, 1)
        assert workers
        push_fault_controller.reset()
        push_fault_controller.drop_next_connection()

        started = time.monotonic()
        assert client.push_data(1, 0, 0, 0, _DATA) == len(_DATA) + 16
        assert time.monotonic() - started < _MAX_RECOVERY_TIME_SECONDS
        _assert_revive_route(push_fault_controller.snapshot())
        client.mapper_end(1, 0, 0, 1)

        assert b"".join(client.read_partition_stream(1, 0)) == _DATA


def test_shuffle_client_reads_data_from_epochs_before_and_after_revive(
    celeborn_master: MasterService,
    recovery_endpoint_resolver: object,
    push_fault_controller: PushFaultController,
    push_fault_proxies: dict[str, FaultInjectingTcpProxy],
) -> None:
    """A recoverable location change must preserve earlier committed epoch data."""
    before_revive = b"epoch zero"
    after_revive = b"epoch one"
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-multi-epoch",
            recovery_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(2, [0], False, 1)
        assert workers
        push_fault_controller.reset()

        assert client.push_data(2, 0, 0, 0, before_revive) == len(before_revive) + 16
        before_revive_faults = push_fault_controller.snapshot()
        assert len(before_revive_faults.forwarded_workers) == 1
        first_worker = before_revive_faults.forwarded_workers[0]
        for proxy in push_fault_proxies.values():
            proxy.disconnect_clients()

        started = time.monotonic()
        assert client.push_data(2, 0, 0, 0, after_revive) == len(after_revive) + 16
        assert time.monotonic() - started < _MAX_RECOVERY_TIME_SECONDS
        after_revive_faults = push_fault_controller.snapshot()
        assert after_revive_faults.disconnected_workers == (first_worker,)
        assert len(after_revive_faults.forwarded_workers) == _MULTI_EPOCH_WORKER_COUNT
        assert after_revive_faults.forwarded_workers[0] == first_worker
        assert after_revive_faults.forwarded_workers[1] != first_worker
        client.mapper_end(2, 0, 0, 1)

        assert b"".join(client.read_partition_stream(2, 0)) == before_revive + after_revive


def test_shuffle_client_does_not_reuse_a_worker_that_failed_in_an_earlier_epoch(
    celeborn_master: MasterService,
    recovery_endpoint_resolver: object,
    push_fault_controller: PushFaultController,
    push_fault_proxies: dict[str, FaultInjectingTcpProxy],
) -> None:
    """A second revive must not route data back to the worker that failed first."""
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-worker-exclusion",
            recovery_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        client.register_shuffle(3, [0], False, 1)
        push_fault_controller.reset()

        assert client.push_data(3, 0, 0, 0, b"epoch zero") == len(b"epoch zero") + 16
        for proxy in push_fault_proxies.values():
            proxy.disconnect_clients()
        assert client.push_data(3, 0, 0, 0, b"epoch one") == len(b"epoch one") + 16
        push_fault_controller.drop_next_connection()
        with pytest.raises(RuntimeError, match="master error: status 27"):
            client.push_data(3, 0, 0, 0, b"epoch two")

        faults = push_fault_controller.snapshot()
        assert len(faults.disconnected_workers) == 1
        assert len(faults.dropped_workers) == 1
        assert len(faults.forwarded_workers) == 2  # noqa: PLR2004
        assert faults.forwarded_workers == (
            *faults.disconnected_workers,
            *faults.dropped_workers,
        )


def test_shuffle_client_reader_discovers_epochs_revived_by_another_client(
    celeborn_master: MasterService,
    recovery_endpoint_resolver: object,
    push_fault_controller: PushFaultController,
    push_fault_proxies: dict[str, FaultInjectingTcpProxy],
) -> None:
    """A pre-registered reader must include epochs created by a different writer."""
    before_revive = b"epoch zero"
    after_revive = b"epoch one"
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-revive-reader-location-update",
            recovery_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as writer,
        ShuffleClient(manager) as reader,
    ):
        writer.register_shuffle(4, [0], False, 1)
        reader.register_shuffle(4, [0], False, 1)
        push_fault_controller.reset()

        assert writer.push_data(4, 0, 0, 0, before_revive) == len(before_revive) + 16
        for proxy in push_fault_proxies.values():
            proxy.disconnect_clients()
        assert writer.push_data(4, 0, 0, 0, after_revive) == len(after_revive) + 16
        writer.mapper_end(4, 0, 0, 1)

        faults = push_fault_controller.snapshot()
        assert len(faults.disconnected_workers) == 1
        assert len(faults.forwarded_workers) == 2  # noqa: PLR2004
        first_epoch_worker, second_epoch_worker = faults.forwarded_workers
        assert first_epoch_worker == faults.disconnected_workers[0]
        assert second_epoch_worker != first_epoch_worker

        # Replication is disabled, so the two payloads live on different workers.
        # Reading their concatenation proves the reader opens both epoch streams.
        assert b"".join(reader.read_partition_stream(4, 0)) == before_revive + after_revive
