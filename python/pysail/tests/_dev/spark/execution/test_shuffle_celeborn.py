from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import PySparkException
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.tests._dev.spark.execution.test_shuffle_storage import check_retried_cte_query_fails

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

    from pysail.testing._dev.containers.celeborn import MasterService, WorkerService

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


_SHARED_CTE = "c AS (SELECT id % 7 AS k, count(*) AS n FROM range(1000) GROUP BY 1)"


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (f"WITH {_SHARED_CTE} SELECT count(*), sum(a.n + b.n) FROM c a JOIN c b ON a.k = b.k", [(7, 2000)]),  # noqa: S608
        (f"WITH {_SHARED_CTE} SELECT count(*), sum(n) FROM (SELECT * FROM c UNION ALL SELECT * FROM c)", [(14, 2000)]),  # noqa: S608
        (
            f"WITH {_SHARED_CTE} SELECT count(*) FROM (SELECT id FROM range(10) WHERE id < 0) e "  # noqa: S608
            "JOIN c a ON e.id = a.k JOIN c b ON a.k = b.k",
            [(0,)],
        ),
        (f"WITH {_SHARED_CTE} SELECT a.k FROM c a JOIN c b ON a.k = b.k ORDER BY 1 LIMIT 3", [(0,), (1,), (2,)]),  # noqa: S608
        # The output of the shared CTE is empty in most partitions, which are read as ranges
        # without batches of a file with batches.
        (
            "WITH c AS (SELECT id % 2 AS k, count(*) AS n FROM range(100) GROUP BY 1) "
            "SELECT count(*), sum(a.n + b.n) FROM c a JOIN c b ON a.k = b.k",
            [(2, 200)],
        ),
        # The output of the shared CTE is empty in every partition.
        (
            "WITH a AS (SELECT sum(id) AS q FROM range(0, 12, 1, 3) HAVING sum(id) < 0) "
            "SELECT count(*) FROM a x JOIN a y ON x.q = y.q",
            [(0,)],
        ),
        (
            "WITH a AS (SELECT max(id) AS p, count(*) AS q FROM range(0, 12, 1, 3)) "
            "SELECT x.q FROM (SELECT * FROM a WHERE p IN (1, 2)) x JOIN (SELECT * FROM a WHERE p IN (1, 2)) y "
            "ON x.q = y.q",
            [],
        ),
    ],
)
def test_shared_cte_with_celeborn_shuffle(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected
    assert "SharedCteExec" in spark.sql(f"EXPLAIN {query}").collect()[0][0]


def test_shared_cte_error_with_celeborn_shuffle(spark):
    cte = "c AS (SELECT id % 7 AS k, max(100 DIV (id - 3)) AS q FROM range(1000) GROUP BY 1)"
    query = f"WITH {cte} SELECT sum(a.q + b.q) FROM c a JOIN c b ON a.k = b.k"  # noqa: S608
    with pytest.raises(PySparkException, match=r"(?i)divi"):
        spark.sql(query).collect()


def test_retried_region_reads_the_output_of_a_blocking_stage(spark):
    check_retried_cte_query_fails(spark)


def test_celeborn_shuffle_is_usable_after_a_shared_cte_without_output(spark):
    query = (
        "WITH a AS (SELECT sum(id) AS q FROM range(0, 12, 1, 3) HAVING sum(id) < 0) "
        "SELECT count(*) FROM a x JOIN a y ON x.q = y.q"
    )
    for _ in range(2):
        assert [tuple(row) for row in spark.sql(query).collect()] == [(0,)]
        query_with_output = f"WITH {_SHARED_CTE} SELECT count(*), sum(a.n + b.n) FROM c a JOIN c b ON a.k = b.k"  # noqa: S608
        assert [tuple(row) for row in spark.sql(query_with_output).collect()] == [(7, 2000)]
