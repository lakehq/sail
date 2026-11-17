from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import PySparkException
from pyspark.sql.types import Row

from pysail.testing._dev.containers.celeborn import CelebornFrame, CelebornMessageType
from pysail.testing._dev.utils.proxy import (
    ConnectionClosed,
    ConnectionOpened,
    Discard,
    FrameReceived,
    FrameRule,
    ReplaceMany,
)
from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

    from pysail.testing._dev.containers.celeborn import MasterService, WorkerService
    from pysail.testing._dev.utils.proxy import EndpointProxy

pytestmark = [
    pytest.mark.integration,
    pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode only"),
]


@pytest.fixture(scope="module")
def remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> Generator[str, None, None]:
    """Run Spark Connect with a Celeborn shuffle backend."""
    with spark_connect_server(envs=_celeborn_envs(celeborn_master, celeborn_workers)) as server:
        yield server.remote


def _celeborn_envs(
    master: MasterService,
    workers: dict[str, WorkerService],
    fetch_proxies: dict[str, EndpointProxy] | None = None,
) -> dict[str, str]:
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ internal = "{hostname}:{port}", external = "{host}:{mapped_port}" }}'
            for hostname, worker in workers.items()
            for port, host, mapped_port in [
                (12000, worker.host, worker.rpc_port),
                (12001, worker.host, worker.push_port),
                (
                    12002,
                    fetch_proxies[hostname].host if fetch_proxies else worker.host,
                    fetch_proxies[hostname].port if fetch_proxies else worker.fetch_port,
                ),
                (12003, worker.host, worker.replicate_port),
            ]
        )
    )
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": f'["{master.host}:{master.port}"]',
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
    }


def test_cancelled_chunk_fetch_does_not_leave_its_reply(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_fetch_proxies: dict[str, EndpointProxy],
) -> None:
    held: list[FrameReceived[CelebornFrame]] = []
    received = threading.Event()

    def hold_chunk(event: FrameReceived[CelebornFrame]) -> Discard:
        held.append(event)
        received.set()
        return Discard()

    def is_later_reply(event: FrameReceived[CelebornFrame]) -> bool:
        return (
            received.is_set()
            and event.direction == "server_to_client"
            and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            and event.attributes["endpoint"] == held[0].attributes["endpoint"]
            and event.connection_id == held[0].connection_id
        )

    hold = FrameRule(
        frame_type=CelebornFrame,
        condition=lambda event: (
            event.direction == "server_to_client"
            and event.frame.message_type == CelebornMessageType.CHUNK_FETCH_SUCCESS
        ),
        action=hold_chunk,
    )
    # Deliver the withheld chunk before the next RPC reply only if the abandoned
    # socket is reused. This avoids depending on a fixed network delay.
    replay = FrameRule(
        frame_type=CelebornFrame,
        condition=is_later_reply,
        action=lambda event: ReplaceMany((held[0].frame, event.frame)),
    )
    for proxy in celeborn_fetch_proxies.values():
        proxy.rules.add(hold)
        proxy.rules.add(replay)

    envs = _celeborn_envs(celeborn_master, celeborn_workers, celeborn_fetch_proxies)
    envs.update(
        {
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
        }
    )
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()

        def collect():
            return spark.range(100, numPartitions=2).repartition(2).collect()

        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(collect)
            try:
                assert received.wait(timeout=10), "the query never fetched a chunk"
                assert spark.interruptAll()
                assert result.exception(timeout=10) is not None
                rows = executor.submit(collect).result(timeout=10)
                assert sorted(row.id for row in rows) == list(range(100))
            finally:
                spark.interruptAll()

        abandoned = held[0]
        proxy = next(
            proxy for proxy in celeborn_fetch_proxies.values() if proxy.name == abandoned.attributes["endpoint"]
        )
        assert proxy.events.count(ConnectionClosed, connection_id=abandoned.connection_id) == 1
        expected_connections = 2  # The canceled fetch and its replacement.
        assert proxy.events.count(ConnectionOpened) == expected_connections


def _server_session_id(spark: SparkSession) -> str:
    rows = spark.sql(
        "select session_id from system.session.sessions where client_session_id = :id",
        args={"id": spark.session_id},
    ).collect()
    assert len(rows) == 1
    return rows[0].session_id


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


@pytest.mark.spark(cross_version=True)
def test_consumed_celeborn_shuffle_data_is_removed(spark, celeborn_master: MasterService):
    @F.udf("long")
    def identity(value):
        time.sleep(2)
        return value

    session_id = _server_session_id(spark)
    assert _application_shuffle_ids(celeborn_master, session_id) == []

    with ThreadPoolExecutor(max_workers=1) as executor:
        # Sum needs the range values even when its row count is known exactly.
        result = executor.submit(
            lambda: (
                spark.range(2)
                .repartition(2)
                .groupBy()
                .agg(F.sum("id").alias("total"))
                .select(identity("total").alias("total"))
                .collect()
            )
        )
        deadline = time.monotonic() + 2
        while not (shuffle_ids := _application_shuffle_ids(celeborn_master, session_id)):
            if result.done() or time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        assert shuffle_ids, "the shuffle was never registered with the Celeborn master"
        assert result.result() == [Row(total=1)]

    deadline = time.monotonic() + 5
    while shuffle_ids := _application_shuffle_ids(celeborn_master, session_id):
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)

    assert shuffle_ids == [], "the Celeborn shuffle was not removed after the query completed"


def test_zero_tail_does_not_run_shuffle_producers(spark):
    df = (
        spark.range(0, 10, 1, 4)
        .selectExpr("id", "raise_error(concat('tail input evaluated: ', cast(id as string))) AS value")
        .repartition(3)
    )
    assert df.tail(0) == []
    with pytest.raises(PySparkException, match="tail input evaluated"):
        df.tail(1)


def test_sorted_tail_with_shuffle(spark):
    df = spark.range(0, 100, 1, 4).repartition(3).orderBy("id", ascending=False)
    assert [row.id for row in df.tail(7)] == list(range(6, -1, -1))
    assert [row.id for row in df.tail(101)] == list(range(99, -1, -1))
    projected = df.selectExpr("concat('row-', cast(id as string)) AS payload")
    assert [row.payload for row in projected.tail(7)] == [f"row-{i}" for i in range(6, -1, -1)]
    assert [row.payload for row in projected.tail(101)] == [f"row-{i}" for i in range(99, -1, -1)]
