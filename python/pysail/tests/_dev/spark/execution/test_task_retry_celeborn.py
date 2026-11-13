"""Task retries with the Celeborn shuffle backend: the reader must skip the output that a
failed task attempt pushed before it failed."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests._dev.spark.execution.test_task_retry import ERROR, RUNS, collect, expected, shapes

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing._dev.containers.celeborn import MasterService, WorkerService

# TODO: The Celeborn shuffle reader reads the batches that a failed task attempt pushed
#   before it failed, so a retried task can return duplicate rows. The reader should keep
#   only the batches of the attempt that is committed for each map task.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only"),
    pytest.mark.skip(reason="the Celeborn shuffle reader does not skip the output of failed task attempts"),
]


@pytest.fixture(scope="module")
def remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> Generator[str, None, None]:
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ internal = "{hostname}:{port}", external = "{worker.host}:{mapped_port}" }}'
            for hostname, worker in celeborn_workers.items()
            for port, mapped_port in [
                (12000, worker.rpc_port),
                (12001, worker.push_port),
                (12002, worker.fetch_port),
                (12003, worker.replicate_port),
            ]
        )
    )
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": (
            f'["{celeborn_master.host}:{celeborn_master.port}"]'
        ),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.mark.timeout(900)
@pytest.mark.parametrize("shape", ["upstream-rows", "upstream-aggregate", "join-side", "final-stage"])
def test_task_failure_after_partial_output(spark, shape, tmp_path):
    for run in range(RUNS):
        marker = str(tmp_path / f"marker-{run}")
        rows, error = collect(shapes(spark, marker)[shape])
        assert os.path.exists(marker), "the failure was not injected"
        if error is None:
            assert sorted(rows) == expected(shape)
        else:
            assert shape in ("upstream-rows", "final-stage"), error
            assert ERROR in error, error
