import re

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail spill configuration")


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def execution_mode(request):
    return request.param


@pytest.fixture(scope="module", params=["uncompressed", "lz4_frame", "zstd"])
def remote(request, execution_mode):
    with spark_connect_server(
        envs={
            "SAIL_MODE": execution_mode,
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "1",
            "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
            "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": "33554432",
            "SAIL_RUNTIME__TEMPORARY_FILES__COMPRESSION": request.param,
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        }
    ) as server:
        yield server.remote


def test_range_repartition_spools_wide_payload(spark):
    rows = 120_000
    source = spark.range(rows, numPartitions=4).selectExpr("id", "lpad(CAST(id AS STRING), 512, '0') AS payload")
    result = source.repartitionByRange(4, "id")
    # The 60 MiB payload exceeds the 32 MiB pool; keys alone determine boundaries.
    result.createOrReplaceTempView("range_payload")
    actual = spark.sql("SELECT count(*), sum(id), sum(length(payload)), max(payload) FROM range_payload").first()
    assert tuple(actual) == (rows, rows * (rows - 1) // 2, rows * 512, str(rows - 1).zfill(512))
    assert result.limit(1).first().payload is not None


def test_sort_temporary_file_compression(spark):
    rows = 120_000
    # Retain the padded keys in buffered rows; row ranks verify merge order after spilling.
    query = f"""
        SELECT SUM(id) AS ids, SUM(rn) AS ranks, MIN(id + rn) AS low, MAX(id + rn) AS high,
               MAX(payload) AS last_key
        FROM (
            SELECT id, payload, ROW_NUMBER() OVER (ORDER BY payload) AS rn
            FROM (
                SELECT id, lpad(CAST({rows} - id AS STRING), 512, '0') AS payload
                FROM range({rows})
            )
        )
    """  # noqa: S608 - row count is controlled by the test.
    result = spark.sql(query).first()
    assert tuple(result) == (rows * (rows - 1) // 2, rows * (rows + 1) // 2, rows, rows, str(rows).zfill(512))
    plan = spark.sql(f"EXPLAIN ANALYZE {query}").first()[0]
    assert re.search(r"\bspill_count=[1-9]\d*\b", plan), plan
