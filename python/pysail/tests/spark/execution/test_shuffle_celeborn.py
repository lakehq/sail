from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import MasterService, WorkerService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only"),
]


@pytest.fixture(scope="module")
def remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> Generator[str, None, None]:
    """Run Spark Connect with a Celeborn shuffle backend."""
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ internal_host = "{hostname}", internal_port = {port}, '
            f'external_host = "{worker.host}", external_port = {mapped_port} }}'
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
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_HOST": celeborn_master.host,
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_PORT": str(celeborn_master.port),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def _master_shuffle_ids(master: MasterService) -> list[str]:
    with urlopen(f"http://{master.host}:{master.http_port}/api/v1/shuffles", timeout=5) as response:
        return json.load(response)["shuffleIds"]


def _application_shuffle_ids(master: MasterService, session_id: str) -> list[str]:
    prefix = f"sail-session-{session_id}-"
    return [shuffle_id for shuffle_id in _master_shuffle_ids(master) if shuffle_id.startswith(prefix)]


def test_repartition_collect_with_celeborn_shuffle(remote):
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        rows = (
            spark.createDataFrame([Row(id=i, group=i % 3) for i in range(30)])
            .repartition(6, "group")
            .groupBy("group")
            .count()
            .orderBy("group")
            .collect()
        )

    assert rows == [
        Row(group=0, count=10),
        Row(group=1, count=10),
        Row(group=2, count=10),
    ]


def test_consumed_celeborn_shuffle_data_is_removed(spark, celeborn_master: MasterService):
    @F.udf("long")
    def identity(value):
        time.sleep(2)
        return value

    session_id = spark.session_id
    assert _application_shuffle_ids(celeborn_master, session_id) == []

    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(
            lambda: spark.range(2).repartition(2).groupBy().count().select(identity("count").alias("count")).collect()
        )
        deadline = time.monotonic() + 2
        while not (shuffle_ids := _application_shuffle_ids(celeborn_master, session_id)):
            if result.done() or time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        assert shuffle_ids, "the shuffle was never registered with the Celeborn master"
        assert result.result() == [Row(count=2)]

    deadline = time.monotonic() + 5
    while shuffle_ids := _application_shuffle_ids(celeborn_master, session_id):
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)

    assert shuffle_ids == [], "the Celeborn shuffle was not removed after the query completed"
