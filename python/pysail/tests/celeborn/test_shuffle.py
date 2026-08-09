"""Integration tests for the Celeborn shuffle client actor."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail import _native

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.tests.celeborn.conftest import MasterService, WorkerService


ShuffleClient = _native._celeborn.ShuffleClient  # noqa: SLF001
LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001
_DATA = b"hello Celeborn"
_REPLICATION_WORKER_COUNT = 2


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_worker: WorkerService,
    endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_worker.push_port > 0
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
    assert workers == ["celeborn-worker:12000:12001:12002:12003"]
    lifecycle_manager.unregister_shuffle(1)


def test_shuffle_client_pushes_and_reads_partition(
    shuffle_client: ShuffleClient,
    lifecycle_manager: LifecycleManager,
) -> None:
    shuffle_client.register_shuffle(2, [0], False, 1)
    assert shuffle_client.push_data(2, 0, 0, 0, _DATA) == len(_DATA) + 16
    shuffle_client.mapper_end(2, 0, 0, 1)
    assert shuffle_client.read_partition(2, 0) == _DATA
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
    assert shuffle_client.read_partition(3, 0) == b"first mapsecond map"
    lifecycle_manager.unregister_shuffle(3)


def test_shuffle_client_replicates_data(
    celeborn_master: MasterService,
    celeborn_replica_worker: WorkerService,
    replication_endpoint_resolver: object,
) -> None:
    assert celeborn_replica_worker.replicate_port > 0
    with (
        LifecycleManager(
            celeborn_master.host,
            celeborn_master.port,
            "sail-celeborn-replication-integration",
            replication_endpoint_resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        workers = client.register_shuffle(4, [0], True, _REPLICATION_WORKER_COUNT)
        assert len(workers) == _REPLICATION_WORKER_COUNT
        assert client.push_data(4, 0, 0, 0, _DATA) == len(_DATA) + 16
        client.mapper_end(4, 0, 0, 1)
        assert client.read_partition(4, 0) == _DATA
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
