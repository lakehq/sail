from __future__ import annotations

import os
from typing import Generator

import pytest


@pytest.fixture(scope="session")
def remote() -> Generator[str, None, None]:
    """Start a Spark Connect server in Sail local-cluster mode with pinned worker settings.

    This overrides the parent `local_cluster` fixture so that only cache tests run with the
    explicit worker/task-slot configuration.
    """
    old_mode = os.environ.get("SAIL_MODE")
    old_worker_initial = os.environ.get("SAIL_CLUSTER__WORKER_INITIAL_COUNT")
    old_worker_max = os.environ.get("SAIL_CLUSTER__WORKER_MAX_COUNT")
    old_worker_slots = os.environ.get("SAIL_CLUSTER__WORKER_TASK_SLOTS")

    os.environ["SAIL_MODE"] = "local-cluster"
    os.environ["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] = "4"
    os.environ["SAIL_CLUSTER__WORKER_MAX_COUNT"] = "4"
    # Need enough worker slots to schedule a full `repartition(32)` region.
    os.environ["SAIL_CLUSTER__WORKER_TASK_SLOTS"] = "8"

    try:
        # Import lazily to avoid importing `pysail._native` before
        # `pysail/tests/conftest.py::pytest_configure` sets up the environment.
        from pysail.spark import SparkConnectServer

        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        if old_mode is None:
            os.environ.pop("SAIL_MODE", None)
        else:
            os.environ["SAIL_MODE"] = old_mode

        if old_worker_initial is None:
            os.environ.pop("SAIL_CLUSTER__WORKER_INITIAL_COUNT", None)
        else:
            os.environ["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] = old_worker_initial

        if old_worker_max is None:
            os.environ.pop("SAIL_CLUSTER__WORKER_MAX_COUNT", None)
        else:
            os.environ["SAIL_CLUSTER__WORKER_MAX_COUNT"] = old_worker_max

        if old_worker_slots is None:
            os.environ.pop("SAIL_CLUSTER__WORKER_TASK_SLOTS", None)
        else:
            os.environ["SAIL_CLUSTER__WORKER_TASK_SLOTS"] = old_worker_slots

