import json
import re
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.iceberg.utils import create_sql_catalog

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
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": str(parallelism),
        }
    ) as server:
        yield server.remote


def write_row_groups(path, sizes, schema=None):
    schema = schema or pa.schema([("id", pa.int64()), ("payload", pa.string())])
    with pq.ParquetWriter(path, schema, compression="NONE", use_dictionary=False) as writer:
        start = 0
        for size in sizes:
            writer.write_table(
                pa.table({"id": range(start, start + size), "payload": ["x" * 128] * size}, schema=schema),
                row_group_size=max(size, 1),
            )
            start += size


@pytest.mark.parametrize("sizes", [[20_000], [12_000, 9_000, 20_000]])
@pytest.mark.yamlsnapshot(group="plan")
def test_parquet_partitions_follow_row_groups(spark, tmp_path, sizes, scan_settings, snapshot):
    path = tmp_path / "groups.parquet"
    write_row_groups(path, sizes)
    # Each row group is over 1 MiB; arbitrary byte splitting would create many
    # empty ranges, including for the uneven multi-group file.
    query = spark.read.parquet(str(path)).filter("id % 7 = 0").select("id", F.spark_partition_id().alias("pid"))
    plan = normalize_plan_text(query._explain_string())  # noqa: SLF001
    assert plan == snapshot
    rows = query.collect()
    assert sorted(row.id for row in rows) == list(range(0, sum(sizes), 7))
    if scan_settings[0] == "local-cluster":
        assert len(Counter(row.pid for row in rows)) == len(sizes)


@pytest.mark.parametrize("sizes", [[300], [20_000], [12_000, 9_000, 20_000]])
@pytest.mark.yamlsnapshot(group="plan")
def test_small_filtered_build_is_materialized_once(spark, tmp_path, sizes, snapshot):
    path = tmp_path / "build.parquet"
    write_row_groups(path, sizes)
    build = spark.read.parquet(str(path)).filter("id % 2 = 0").select("id")
    probe = spark.range(sum(sizes) * 2, numPartitions=8).withColumnRenamed("id", "probe_id")
    query = probe.join(F.broadcast(build), probe.probe_id == build.id).select("probe_id")
    plan = normalize_plan_text(query._explain_string(mode="codegen"))  # noqa: SLF001
    assert plan == snapshot
    assert sorted(row.probe_id for row in query.collect()) == list(range(0, sum(sizes), 2))


@pytest.mark.parametrize("kind", ["round_robin", "hash"])
def test_explicit_repartition_after_small_scan_is_preserved(spark, tmp_path, kind):
    path = tmp_path / "explicit.parquet"
    write_row_groups(path, [300])
    query = spark.read.parquet(str(path)).filter("id % 2 = 0")
    query = query.repartition(5) if kind == "round_robin" else query.repartition(5, "id")
    rows = query.select("id", F.spark_partition_id().alias("pid")).collect()
    assert sorted(row.id for row in rows) == list(range(0, 300, 2))
    assert {row.pid for row in rows} == set(range(5))


@pytest.mark.yamlsnapshot(group="plan")
def test_multiple_small_files_are_grouped_without_losing_rows(spark, tmp_path, snapshot):
    path = tmp_path / "files"
    path.mkdir()
    for i in range(4):
        pq.write_table(pa.table({"id": range(i * 100, (i + 1) * 100)}), path / f"{i}.parquet")
    query = spark.read.parquet(str(path)).filter("id % 3 = 0")
    plan = normalize_plan_text(query._explain_string())  # noqa: SLF001
    assert plan == snapshot
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


@pytest.mark.parametrize("sizes", [[20_000], [12_000, 9_000, 20_000]])
@pytest.mark.parametrize("table_format", ["delta", "iceberg", "iceberg_identity"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_lake_scan_row_groups(spark, tmp_path, sizes, table_format, metadata_as_data, scan_settings):
    path = tmp_path / "groups.parquet"
    write_row_groups(
        path,
        sizes,
        pa.schema(
            [
                pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"}),
                pa.field("payload", pa.string(), metadata={"PARQUET:field_id": "2"}),
            ]
        ),
    )
    catalog = None
    if table_format == "delta":
        log = tmp_path / "_delta_log"
        log.mkdir()
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": "row-group-scan",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps(
                        {
                            "type": "struct",
                            "fields": [
                                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                                {"name": "payload", "type": "string", "nullable": True, "metadata": {}},
                            ],
                        }
                    ),
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0,
                }
            },
            {
                "add": {
                    "path": path.name,
                    "partitionValues": {},
                    "size": path.stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": sum(sizes)}),
                }
            },
        ]
        (log / "00000000000000000000.json").write_text("".join(json.dumps(action) + "\n" for action in actions))
        location = str(tmp_path)
    else:
        catalog = create_sql_catalog(tmp_path)
        partitioned = table_format == "iceberg_identity"
        spec = (
            PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "payload")) if partitioned else PartitionSpec()
        )
        table = catalog.create_table(
            "default.row_groups",
            schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "payload", StringType())),
            partition_spec=spec,
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record("x" * 128) if partitioned else Record(),
                    record_count=sum(sizes),
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=spec.spec_id,
                )
            )
        location = table.location()
    try:
        query = (
            spark.read.format("delta" if table_format == "delta" else "iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(location)
            .filter("id % 7 = 0")
            .select("id", "payload", F.spark_partition_id().alias("pid"))
        )
        if not metadata_as_data or table_format == "iceberg_identity":
            plan = query._explain_string()  # noqa: SLF001
            groups = re.search(r"DataSourceExec: file_groups=\{(\d+) group", plan)
            assert groups is not None, plan
            assert int(groups[1]) == len(sizes), plan
        for _ in range(2):
            rows = query.collect()
            assert sorted(row.id for row in rows) == list(range(0, sum(sizes), 7))
            assert all(row.payload == "x" * 128 for row in rows)
            if (not metadata_as_data or table_format == "iceberg_identity") and scan_settings[0] == "local-cluster":
                assert len(Counter(row.pid for row in rows)) == len(sizes)
    finally:
        if catalog is not None:
            catalog.drop_table("default.row_groups")
