from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail scan and distributed planning")


@pytest.fixture(scope="module", params=[("local", 64), ("local-cluster", 64), ("local-cluster", 256)])
def scan_settings(request):
    return request.param


@pytest.fixture(scope="module")
def remote(scan_settings):
    mode, parallelism = scan_settings
    with spark_connect_server(
        envs={
            "SAIL_MODE": mode,
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(parallelism),
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": "32",
        }
    ) as server:
        yield server.remote


def write_row_groups(path, sizes):
    schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])
    with pq.ParquetWriter(path, schema, compression="NONE", use_dictionary=False) as writer:
        start = 0
        for size in sizes:
            writer.write_table(
                pa.table({"id": range(start, start + size), "payload": ["x" * 128] * size}, schema=schema),
                row_group_size=max(size, 1),
            )
            start += size


@pytest.mark.parametrize("sizes", [[20_000], [12_000, 9_000, 20_000]])
def test_parquet_partitions_follow_row_groups(spark, tmp_path, sizes, scan_settings):
    path = tmp_path / "groups.parquet"
    write_row_groups(path, sizes)
    # Each row group is over 1 MiB; arbitrary byte splitting would create many
    # empty ranges, including for the uneven multi-group file.
    query = spark.read.parquet(str(path)).filter("id % 7 = 0").select("id", F.spark_partition_id().alias("pid"))
    plan = query._explain_string()  # noqa: SLF001
    assert f"file_groups={{{len(sizes)} group" in plan
    rows = query.collect()
    assert sorted(row.id for row in rows) == list(range(0, sum(sizes), 7))
    if scan_settings[0] == "local-cluster":
        assert len(Counter(row.pid for row in rows)) == len(sizes)


def test_small_filtered_build_is_materialized_once(spark, tmp_path):
    path = tmp_path / "build.parquet"
    write_row_groups(path, [300])
    build = spark.read.parquet(str(path)).filter("id % 2 = 0").select("id")
    probe = spark.range(1000, numPartitions=8).withColumnRenamed("id", "probe_id")
    query = probe.join(F.broadcast(build), probe.probe_id == build.id).select("probe_id")
    graph = query._explain_string(mode="codegen").split("== Distributed Plan ==", 1)[1]  # noqa: SLF001
    stages = graph.split("=== stage ")[1:]
    scans = [stage for stage in stages if "build.parquet" in stage]
    assert len(scans) == 1
    assert "partitions=1\n" in scans[0]
    producer = scans[0].split(" ===", 1)[0]
    assert f"StageInput(stage={producer}, mode=Broadcast)" in graph
    assert "RoundRobinBatch(64)" not in scans[0]
    assert sorted(row.probe_id for row in query.collect()) == list(range(0, 300, 2))


@pytest.mark.parametrize("kind", ["round_robin", "hash"])
def test_explicit_repartition_after_small_scan_is_preserved(spark, tmp_path, kind):
    path = tmp_path / "explicit.parquet"
    write_row_groups(path, [300])
    query = spark.read.parquet(str(path)).filter("id % 2 = 0")
    query = query.repartition(5) if kind == "round_robin" else query.repartition(5, "id")
    rows = query.select("id", F.spark_partition_id().alias("pid")).collect()
    assert sorted(row.id for row in rows) == list(range(0, 300, 2))
    assert {row.pid for row in rows} == set(range(5))


def test_multiple_small_files_are_grouped_without_losing_rows(spark, tmp_path):
    path = tmp_path / "files"
    path.mkdir()
    for i in range(4):
        pq.write_table(pa.table({"id": range(i * 100, (i + 1) * 100)}), path / f"{i}.parquet")
    query = spark.read.parquet(str(path)).filter("id % 3 = 0")
    assert "file_groups={1 group" in query._explain_string()  # noqa: SLF001
    assert sorted(row.id for row in query.collect()) == list(range(0, 400, 3))


def test_empty_and_pruned_parquet_scans(spark, tmp_path):
    empty = tmp_path / "empty.parquet"
    write_row_groups(empty, [0])
    assert spark.read.parquet(str(empty)).filter("id > 0").collect() == []
    path = tmp_path / "pruned.parquet"
    write_row_groups(path, [12_000, 9_000, 20_000])
    assert spark.read.parquet(str(path)).filter("id < 0").collect() == []


def test_scan_rewrite_preserves_sort_and_limit(spark, tmp_path):
    path = tmp_path / "sorted.parquet"
    write_row_groups(path, [12_000, 9_000, 20_000])
    query = spark.read.parquet(str(path)).filter("id % 3 = 0").orderBy(F.col("id").desc()).limit(17)
    assert [row.id for row in query.collect()] == list(range(40_998, 40_948, -3))
