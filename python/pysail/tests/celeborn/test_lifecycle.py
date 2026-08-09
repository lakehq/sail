"""Integration tests for the Celeborn lifecycle actor."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail import _native

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.tests.celeborn.conftest import MasterService, WorkerService


LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_worker: WorkerService,
    endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_worker.rpc_port > 0
    with LifecycleManager(
        celeborn_master.host,
        celeborn_master.port,
        "sail-celeborn-integration",
        endpoint_resolver,
    ) as manager:
        yield manager


def test_lifecycle_manager_registers_requests_slots_and_unregisters(
    lifecycle_manager: LifecycleManager,
) -> None:
    assert lifecycle_manager.running
    workers = lifecycle_manager.request_slots(1, [0, 1], False, 1)
    assert len(workers) == 1
    assert workers == ["celeborn-worker:12000:12001:12002:12003"]
    lifecycle_manager.unregister_shuffle(1)


def test_lifecycle_manager_returns_registration_failure() -> None:
    with (
        LifecycleManager("127.0.0.1", 0, "sail-celeborn-unavailable") as manager,
        pytest.raises(RuntimeError, match="application error: registration failed: I/O error"),
    ):
        manager.request_slots(1, [0], False, 1)
