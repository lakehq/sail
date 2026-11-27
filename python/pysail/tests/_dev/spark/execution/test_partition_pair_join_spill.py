# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail worker memory pool")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "1",
            "SAIL_EXECUTION__BATCH_SIZE": "512",
            "SAIL_OPTIMIZER__ENABLE_JOIN_SWAP": "false",
            "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
            "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": str(512 * 1024),
        }
    ) as server:
        yield server.remote


def test_partition_pair_join_reserves_probe_before_buffering_build(spark):
    # The build fits alone. Opening the wide probe on the same worker forces the build to spill.
    query = """
        SELECT /*+ SHUFFLE_REPLICATE_NL(l) */ l.id
        FROM range(0, 40000, 1, 1) l
        JOIN (SELECT rpad(CAST(id AS STRING), 60000, 'x') AS txt FROM range(0, 1, 1, 1)) r
          ON l.id % 10000 + length(r.txt) < 60003
    """
    distributed_plan = spark.sql(f"EXPLAIN CODEGEN {query}").first()[0].split("== Distributed Plan ==")[1]
    assert "PartitionPairJoinExec" in distributed_plan
    assert sorted(row.id for row in spark.sql(query).collect()) == [
        start + offset for start in range(0, 40000, 10000) for offset in range(3)
    ]
