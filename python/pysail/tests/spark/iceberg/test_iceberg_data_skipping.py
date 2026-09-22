import contextlib
import datetime
import struct
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.typedef import Record
from pyiceberg.types import DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.tests.spark.iceberg.utils import create_sql_catalog


@contextlib.contextmanager
def unavailable_iceberg_files(locations):
    moved = []
    try:
        for location in locations:
            path = Path(unquote(urlparse(location).path))
            backup = path.with_name(path.name + ".unavailable")
            assert path.is_file()
            assert not backup.exists()
            path.rename(backup)
            moved.append((path, backup))
        assert moved
        yield
    finally:
        for path, backup in reversed(moved):
            backup.rename(path)


TIMESTAMPS = [datetime.datetime(year, 1, 1, 12) for year in [2021, 2022, 2023]]  # noqa: DTZ001


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize("metrics", [False, True])
@pytest.mark.parametrize("predicate", ["p = CAST('NaN' AS DOUBLE)", "p > 1", "p != 1", "p < 0"])
def test_mixed_nan_row_group_matches_row_evaluation(spark, tmp_path, lazy, metrics, predicate):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.mixed_nan"
    table = catalog.create_table(
        identifier, schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", DoubleType()))
    )
    values = [
        1.0,
        float("nan"),
        struct.unpack("d", struct.pack("Q", 0xFFF8000000000000))[0],
        struct.unpack("d", struct.pack("Q", 0x7FF8000000000001))[0],
    ]
    rows = list(enumerate(values, 1))
    try:
        path = tmp_path / "mixed-nan.parquet"
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("p", pa.float64(), metadata={b"PARQUET:field_id": b"2"}),
            ]
        )
        pq.write_table(
            pa.Table.from_arrays([pa.array(range(1, 5)), pa.array(values)], schema=arrow_schema),
            path,
            write_page_index=True,
        )
        assert pq.ParquetFile(path).metadata.num_row_groups == 1
        bounds = {2: struct.pack("<d", 1.0)} if metrics else {}
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=len(values),
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=table.spec().spec_id,
                    lower_bounds=bounds,
                    upper_bounds=bounds,
                    null_value_counts={2: 0} if metrics else {},
                    nan_value_counts={2: 3} if metrics else {},
                )
            )
        expected = [
            row.id
            for row in spark.createDataFrame(rows, "id LONG, p DOUBLE")
            .filter(predicate)
            .select("id")
            .orderBy("id")
            .collect()
        ]
        actual = [
            row.id
            for row in spark.read.format("iceberg")
            .option("metadataAsDataRead", lazy)
            .load(table.location())
            .filter(predicate)
            .select("id")
            .orderBy("id")
            .collect()
        ]
        assert actual == expected
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
def test_float_bloom_filter_preserves_equal_signed_zero(spark, tmp_path, lazy):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.float_bloom"
    table = catalog.create_table(
        identifier, schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", DoubleType()))
    )
    try:
        path = tmp_path / "float-bloom.parquet"
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("p", pa.float64(), metadata={b"PARQUET:field_id": b"2"}),
            ]
        )
        pq.write_table(
            pa.Table.from_arrays([pa.array([1, 2]), pa.array([-0.0, 1.0])], schema=schema),
            path,
            bloom_filter_options={"p": {"ndv": 2, "fpp": 0.000001}},
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=2,
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=table.spec().spec_id,
                    lower_bounds={2: struct.pack("<d", -0.0)},
                    upper_bounds={2: struct.pack("<d", 1.0)},
                    null_value_counts={2: 0},
                    nan_value_counts={2: 0},
                )
            )
        frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
        assert [row.id for row in frame.filter("p = CAST(0 AS DOUBLE)").select("id").collect()] == [1]
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
def test_promoted_truncate_partition_preserves_wrapped_int_values(spark, tmp_path, lazy):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.promoted_truncate"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "p", IntegerType())),
        partition_spec=PartitionSpec(PartitionField(1, 1000, TruncateTransform(10), "truncated")),
        properties={"write.metadata.metrics.default": "none"},
    )
    minimum = -(2**31)
    try:
        path = tmp_path / "wrapped-int.parquet"
        schema = pa.schema([pa.field("p", pa.int32(), metadata={b"PARQUET:field_id": b"1"})])
        pq.write_table(pa.Table.from_arrays([pa.array([minimum], pa.int32())], schema=schema), path)
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record(2**31 - 2),
                    record_count=1,
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=table.spec().spec_id,
                )
            )
        table.update_schema().update_column("p", LongType()).commit()
        frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
        for predicate in [f"p = CAST({minimum} AS BIGINT)", "p < 0", f"p <= {minimum}"]:
            assert [row.p for row in frame.filter(predicate).collect()] == [minimum]
        assert frame.filter(f"p > {minimum}").count() == 0
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
def test_identity_like_escapes_preserve_literal_wildcards(spark, tmp_path, lazy):
    from pyspark.sql import functions as sf

    catalog = create_sql_catalog(tmp_path)
    identifier = "default.escaped_like"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "p", StringType())),
        partition_spec=PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "p")),
        properties={"write.metadata.metrics.default": "none"},
    )
    try:
        for value in ["a%", "a_", "a\\tail", "ab"]:
            table.append(pa.table({"p": [value]}))
        frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
        for pattern, expected in [(r"a\%", "a%"), (r"a\_", "a_"), (r"a\\%", "a\\tail")]:
            assert [row.p for row in frame.filter(sf.col("p").like(pattern)).collect()] == [expected]
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
def test_lazy_limit_does_not_open_later_manifests(spark, tmp_path, snapshot):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.lazy_limit_manifests"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType())),
        properties={"write.metadata.metrics.default": "none", "commit.manifest-merge.enabled": "false"},
    )
    try:
        for value in range(3):
            table.append(pa.table({"id": pa.array([value], pa.int64())}))
        manifests = table.current_snapshot().manifests(table.io)
        expected = pq.read_table(manifests[0].fetch_manifest_entry(table.io)[0].data_file.file_path)["id"][0].as_py()
        with unavailable_iceberg_files([manifest.manifest_path for manifest in manifests[1:]]):
            result = spark.read.format("iceberg").option("metadataAsDataRead", True).load(table.location()).limit(1)
            assert [row.id for row in result.collect()] == [expected]
            assert normalize_plan_text(result._explain_string()) == snapshot  # noqa: SLF001
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("unavailable", ["data", "manifest"])
@pytest.mark.parametrize(
    ("transform", "field_type", "arrow_type", "partition_values", "predicate"),
    [
        pytest.param(IdentityTransform(), IntegerType(), pa.int32(), [1, 2, 3], "p = 2", id="identity"),
        pytest.param(BucketTransform(16), LongType(), pa.int64(), [0, 1, 20], "p = 1", id="bucket"),
        pytest.param(TruncateTransform(10), IntegerType(), pa.int32(), [-1, 11, 21], "p = 11", id="truncate-int"),
        pytest.param(
            TruncateTransform(2),
            StringType(),
            pa.string(),
            ["aaaa", "bbbb", "cccc"],
            "p = 'bbbb'",
            id="truncate-string",
        ),
        *[
            pytest.param(
                transform,
                TimestampType(),
                pa.timestamp("us"),
                TIMESTAMPS,
                "p = TIMESTAMP_NTZ '2022-01-01 12:00:00'",
                id=name,
            )
            for name, transform in [
                ("year", YearTransform()),
                ("month", MonthTransform()),
                ("day", DayTransform()),
                ("hour", HourTransform()),
            ]
        ],
    ],
)
def test_partition_pruning_avoids_unrelated_io(
    spark, tmp_path, unavailable, transform, field_type, arrow_type, partition_values, predicate, snapshot
):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.partition_skipping"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", field_type)),
        partition_spec=PartitionSpec(PartitionField(2, 1000, transform, "partition_value")),
        properties={"write.metadata.metrics.default": "none", "commit.manifest-merge.enabled": "false"},
    )
    try:
        for identifier_value, value in enumerate(partition_values):
            table.append(pa.table({"id": pa.array([identifier_value], pa.int64()), "p": pa.array([value], arrow_type)}))
        files = [task.file for task in table.scan().plan_files()]
        target_partition = transform.transform(field_type)(partition_values[1])
        excluded = {file.file_path for file in files if file.partition[0] != target_partition}
        assert excluded
        assert all(not file.lower_bounds and not file.upper_bounds for file in files)
        manifests = table.current_snapshot().manifests(table.io)
        locations = sorted(excluded)
        if unavailable == "manifest":
            locations = [
                manifest.manifest_path
                for manifest in manifests
                if {entry.data_file.file_path for entry in manifest.fetch_manifest_entry(table.io)} <= excluded
            ]
        assert [
            row.id
            for row in spark.read.format("iceberg").load(table.location()).filter(predicate).select("id").collect()
        ] == [1]
        with unavailable_iceberg_files(locations):
            result = spark.read.format("iceberg").load(table.location()).filter(predicate).select("id")
            assert [row.id for row in result.collect()] == [1]
            assert normalize_plan_text(result._explain_string()) == snapshot  # noqa: SLF001
        assert spark.read.format("iceberg").load(table.location()).count() == len(partition_values)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("rows_per_file", [1, 2])
@pytest.mark.parametrize("lazy", [False, True])
def test_unpartitioned_numeric_metrics_avoid_data_file_io(spark, tmp_path, rows_per_file, lazy, snapshot):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.numeric_skipping"
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "id", LongType())))
    try:
        for value in [0, 10, 20]:
            table.append(pa.table({"id": pa.array(range(value, value + rows_per_file), pa.int64())}))
        files = [task.file for task in table.scan().plan_files()]
        target_value = 10
        excluded = [
            file.file_path
            for file in files
            if int.from_bytes(file.lower_bounds[1], "little", signed=True) != target_value
        ]
        assert len(excluded) == len(files) - 1
        with unavailable_iceberg_files(excluded):
            result = (
                spark.read.format("iceberg")
                .option("metadataAsDataRead", lazy)
                .load(table.location())
                .filter("id = 10")
                .select("id")
            )
            assert [row.id for row in result.collect()] == [10]
            assert normalize_plan_text(result._explain_string()) == snapshot  # noqa: SLF001
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize(
    ("expressions", "expected"),
    [
        (["count(*)"], (4,)),
        (["count(value)"], (3,)),
        (["min(id)", "max(id)"], (1, 4)),
        (["count(*)", "count(value)", "min(id)", "max(id)"], (4, 3, 1, 4)),
        (["count(42)", "min(42)", "max(CAST(NULL AS BIGINT))"], (4, 42, None)),
    ],
    ids=["count-star", "count-column", "numeric-extrema", "combined", "literals"],
)
def test_metadata_aggregate_avoids_all_data_file_io(spark, tmp_path, expressions, expected):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.metadata_aggregate"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "value", LongType())),
    )
    try:
        for ids, values in [([1, 2], [10, None]), ([3, 4], [30, 40])]:
            table.append(pa.table({"id": pa.array(ids, pa.int64()), "value": pa.array(values, pa.int64())}))
        paths = [task.file.file_path for task in table.scan().plan_files()]
        assert len(paths) > 1
        with unavailable_iceberg_files(paths):
            result = spark.read.format("iceberg").load(table.location()).selectExpr(*expressions).collect()
            assert [tuple(row) for row in result] == [expected]
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize(
    ("rows_per_file", "metrics"),
    [
        (1, "full"),
        (1, "none"),
        (3, "full"),
        (3, "none"),
    ],
)
def test_metadata_aggregate_casts_and_partial_results(spark, tmp_path, metrics, rows_per_file):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.aggregate_casts"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "text", StringType())),
        properties={"write.metadata.metrics.default": metrics},
    )
    try:
        ids, texts = [2, 3, 10], ["2", "invalid", "10"]
        for offset in range(0, len(ids), rows_per_file):
            table.append(
                pa.table(
                    {
                        "id": pa.array(ids[offset : offset + rows_per_file], pa.int64()),
                        "text": pa.array(texts[offset : offset + rows_per_file], pa.string()),
                    }
                )
            )
        frame = spark.read.format("iceberg").load(table.location())
        assert tuple(frame.selectExpr("count(*)", "min(id)", "sum(id)").first()) == (3, 2, 15)
        assert tuple(frame.selectExpr("count(try_cast(text AS BIGINT))").first()) == (2,)
        assert tuple(frame.filter("id > 2").selectExpr("count(*)", "min(id)", "max(id)").first()) == (2, 3, 10)
        assert tuple(frame.selectExpr("min(CAST(id AS STRING))", "max(CAST(id AS STRING))").first()) == ("10", "3")
    finally:
        catalog.drop_table(identifier)


def test_metadata_aggregate_uses_selected_snapshot(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.aggregate_snapshot"
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "id", LongType())))
    try:
        table.append(pa.table({"id": pa.array([1, 2], pa.int64())}))
        snapshot_id = table.current_snapshot().snapshot_id
        table.append(pa.table({"id": pa.array([3, 4], pa.int64())}))
        with unavailable_iceberg_files([task.file.file_path for task in table.scan().plan_files()]):
            previous = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load(table.location())
            assert tuple(previous.selectExpr("count(*)", "min(id)", "max(id)").first()) == (2, 1, 2)
            current = spark.read.format("iceberg").load(table.location())
            assert tuple(current.selectExpr("count(*)", "min(id)", "max(id)").first()) == (4, 1, 4)
    finally:
        catalog.drop_table(identifier)


def test_metadata_aggregate_respects_matching_deletes(spark, tmp_path):
    from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot

    catalog = create_sql_catalog(tmp_path)
    identifier = "default.aggregate_deletes"
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "id", LongType())))
    try:
        table.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
        _append_equality_delete_snapshot(table, pa.table({"id": pa.array([1, 1, 99], pa.int64())}), [1])
        frame = spark.read.format("iceberg").load(table.location())
        assert tuple(frame.selectExpr("count(*)", "count(id)", "min(id)", "max(id)").first()) == (2, 2, 2, 3)
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize("partitioned", [False, True])
def test_nan_partition_equality_preserves_matching_rows(spark, tmp_path, lazy, partitioned):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.nan_partition"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", DoubleType())),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p"))
        if partitioned
        else PartitionSpec(),
        properties={"write.metadata.metrics.default": "none"},
    )
    try:
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("p", pa.float64(), metadata={b"PARQUET:field_id": b"2"}),
            ]
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            for identifier_value, value in [(1, 1.0), (2, float("nan"))]:
                path = tmp_path / f"partition-{identifier_value}.parquet"
                pq.write_table(
                    pa.Table.from_arrays([pa.array([identifier_value]), pa.array([value])], schema=arrow_schema), path
                )
                append.append_data_file(
                    DataFile.from_args(
                        content=DataFileContent.DATA,
                        file_path=path.as_uri(),
                        file_format=FileFormat.PARQUET,
                        partition=Record(value) if partitioned else Record(),
                        record_count=1,
                        file_size_in_bytes=path.stat().st_size,
                        spec_id=table.spec().spec_id,
                    )
                )
        manifests = table.current_snapshot().manifests(table.io)
        if partitioned and (len(manifests) != 1 or not manifests[0].partitions[0].contains_nan):
            pytest.fail("Fixture requires a single manifest containing a NaN partition")
        if partitioned and manifests[0].partitions[0].upper_bound is None:
            pytest.fail("Fixture requires finite partition bounds")
        result = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", lazy)
            .load(table.location())
            .filter("p = CAST('NaN' AS DOUBLE)")
            .select("id")
            .collect()
        )
        assert [row.id for row in result] == [2]
    finally:
        catalog.drop_table(identifier)


def test_legacy_timestamp_partition_pruning_preserves_matching_rows(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.timestamp_partition"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "ts", TimestampType())),
        partition_spec=PartitionSpec(PartitionField(2, 1000, DayTransform(), "ts_day")),
        properties={"write.metadata.metrics.default": "none"},
    )
    try:
        path = tmp_path / "timestamp.parquet"
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("ts", pa.timestamp("us"), metadata={b"PARQUET:field_id": b"2"}),
            ]
        )
        pq.write_table(
            pa.Table.from_arrays([pa.array([1]), pa.array([-1], pa.timestamp("us"))], schema=arrow_schema),
            path,
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition=Record(0),
                    record_count=1,
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=table.spec().spec_id,
                )
            )
        frame = spark.read.format("iceberg").load(table.location())
        assert [row.id for row in frame.select("id").collect()] == [1]
        result = frame.filter("ts = TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'").select("id").collect()
        assert [row.id for row in result] == [1]
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("p IN (1, 3)", [1, 3]),
        ("p = 1 OR p = 3", [1, 3]),
        ("p NOT IN (1, 3)", [2]),
        ("NOT (p = 1 OR p = 3)", [2]),
        ("p IN (1, NULL)", [1]),
        ("p NOT IN (1, NULL)", []),
        ("p IS NULL", [4]),
        ("p IS NOT NULL", [1, 2, 3]),
        ("p <=> 2", [2]),
        ("p <=> NULL", [4]),
        ("p > 1 AND p <= 2", [2]),
        ("p BETWEEN 1 AND 2", [1, 2]),
        ("p IN (" + ",".join(map(str, range(21))) + ")", [1, 2, 3]),
    ],
)
def test_predicates_prune_before_opening_files(spark, tmp_path, lazy, partitioned, predicate, expected):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.predicate_skipping"
    spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")) if partitioned else PartitionSpec()
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", LongType())),
        partition_spec=spec,
        properties={"write.metadata.metrics.default": "none" if partitioned else "full"},
    )
    try:
        for identifier_value, value in enumerate([1, 2, 3, None], 1):
            table.append(pa.table({"id": pa.array([identifier_value], pa.int64()), "p": pa.array([value], pa.int64())}))
        excluded = []
        for task in table.scan().plan_files():
            path = Path(unquote(urlparse(task.file.file_path).path))
            if pq.ParquetFile(path).read(columns=["id"])["id"][0].as_py() not in expected:
                excluded.append(task.file.file_path)
        with unavailable_iceberg_files(excluded):
            frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
            assert [row.id for row in frame.filter(predicate).select("id").orderBy("id").collect()] == expected
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("all_null", [False, True])
def test_metadata_extrema_ignore_all_null_files(spark, tmp_path, partitioned, all_null):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.null_extrema"
    spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")) if partitioned else PartitionSpec()
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "value", LongType()), NestedField(2, "p", LongType())),
        partition_spec=spec,
    )
    try:
        for partition, values in [(1, [None, None]), (2, [None, None] if all_null else [3, 4])]:
            table.append(pa.table({"value": pa.array(values, pa.int64()), "p": pa.array([partition] * 2, pa.int64())}))
        with unavailable_iceberg_files([task.file.file_path for task in table.scan().plan_files()]):
            frame = spark.read.format("iceberg").load(table.location())
            expected = (4, 0, None, None) if all_null else (4, 2, 3, 4)
            assert tuple(frame.selectExpr("count(*)", "count(value)", "min(value)", "max(value)").first()) == expected
            if partitioned:
                assert frame.filter("p IN (1, 2)").count() == expected[0]
                assert frame.filter("p = 1").count() == expected[0] // 2
    finally:
        catalog.drop_table(identifier)


@pytest.mark.parametrize("lazy", [False, True])
def test_prefix_and_nested_file_metrics(spark, tmp_path, lazy):
    from pyiceberg.types import StructType

    catalog = create_sql_catalog(tmp_path)
    identifier = "default.nested_skipping"
    table = catalog.create_table(
        identifier,
        schema=Schema(
            NestedField(1, "id", LongType()),
            NestedField(2, "text", StringType()),
            NestedField(3, "details", StructType(NestedField(4, "value", LongType()))),
        ),
        properties={
            "write.metadata.metrics.column.text": "truncate(2)",
            "write.metadata.metrics.column.details.value": "full",
        },
    )
    try:
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                pa.field("text", pa.string(), metadata={b"PARQUET:field_id": b"2"}),
                pa.field(
                    "details",
                    pa.struct([pa.field("value", pa.int64(), metadata={b"PARQUET:field_id": b"4"})]),
                    metadata={b"PARQUET:field_id": b"3"},
                ),
            ]
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            for identifier_value, text in [(1, "aaaa"), (2, "bbbb"), (3, "cccc")]:
                path = tmp_path / f"nested-{identifier_value}.parquet"
                batch = pa.Table.from_pylist(
                    [{"id": identifier_value, "text": text, "details": {"value": identifier_value}}],
                    schema=arrow_schema,
                )
                pq.write_table(batch, path)
                bound = identifier_value.to_bytes(8, "little", signed=True)
                append.append_data_file(
                    DataFile.from_args(
                        content=DataFileContent.DATA,
                        file_path=path.as_uri(),
                        file_format=FileFormat.PARQUET,
                        partition=Record(),
                        record_count=1,
                        file_size_in_bytes=path.stat().st_size,
                        spec_id=table.spec().spec_id,
                        null_value_counts={1: 0, 2: 0, 4: 0},
                        lower_bounds={1: bound, 2: text[:2].encode(), 4: bound},
                        upper_bounds={1: bound, 2: (text[0] + chr(ord(text[1]) + 1)).encode(), 4: bound},
                    )
                )
        leaf_field_id = 4
        assert all(leaf_field_id in task.file.lower_bounds for task in table.scan().plan_files())
        target_id = 2
        excluded = [
            task.file.file_path
            for task in table.scan().plan_files()
            if int.from_bytes(task.file.lower_bounds[1], "little", signed=True) != target_id
        ]
        with unavailable_iceberg_files(excluded):
            for predicate in ["text LIKE 'bb%'", "text = 'bbbb'", "startswith(text, 'bb')", "details.value = 2"]:
                frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
                assert [row.id for row in frame.filter(predicate).select("id").collect()] == [2]
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("lazy", [False, True])
def test_filtered_limit_keeps_later_matches_and_projection(spark, tmp_path, lazy, snapshot):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.filtered_limit"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", LongType())),
        properties={"write.metadata.metrics.default": "none"},
    )
    try:
        for value in [1, 2, 3]:
            table.append(pa.table({"id": pa.array([value], pa.int64()), "p": pa.array([value], pa.int64())}))
        frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
        filtered = frame.filter("p = 1").select("id").limit(1)
        offset = frame.orderBy("id").offset(1).limit(1)
        assert [row.id for row in filtered.collect()] == [1]
        assert sorted(row.id for row in offset.collect()) == [2]
        assert {
            "filtered": normalize_plan_text(filtered._explain_string()),  # noqa: SLF001
            "offset": normalize_plan_text(offset._explain_string()),  # noqa: SLF001
        } == snapshot
        if lazy:
            with unavailable_iceberg_files([task.file.file_path for task in table.scan().plan_files()]):
                assert spark.read.format("iceberg").option("metadataAsDataRead", True).load(
                    table.location()
                ).count() == len([1, 2, 3])
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("lazy", [False, True])
def test_disjoint_equality_delete_does_not_open_delete_file(spark, tmp_path, lazy, snapshot):
    from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot

    catalog = create_sql_catalog(tmp_path)
    identifier = "default.delete_overlap"
    table = catalog.create_table(identifier, schema=Schema(NestedField(1, "id", LongType())))
    try:
        table.append(pa.table({"id": pa.array([1, 2], pa.int64())}))
        value = 999
        _append_equality_delete_snapshot(
            table,
            pa.table({"id": pa.array([value], pa.int64())}),
            [1],
            metrics={
                "value_counts": {1: 1},
                "null_value_counts": {1: 0},
                "lower_bounds": {1: value.to_bytes(8, "little", signed=True)},
                "upper_bounds": {1: value.to_bytes(8, "little", signed=True)},
            },
        )
        paths = list(Path(unquote(urlparse(table.location()).path)).rglob("equality-delete-*.parquet"))
        with unavailable_iceberg_files([path.as_uri() for path in paths]):
            frame = spark.read.format("iceberg").option("metadataAsDataRead", lazy).load(table.location())
            assert sorted(row.id for row in frame.collect()) == [1, 2]
            assert frame.count() == len([1, 2])
            assert normalize_plan_text(frame._explain_string()) == snapshot  # noqa: SLF001
    finally:
        catalog.drop_table(identifier)


@pytest.mark.yamlsnapshot(group="plan")
def test_identity_grouping_and_distinct_use_partition_metadata(spark, tmp_path, snapshot):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.partition_groups"
    table = catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "p", StringType())),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
        properties={"write.metadata.metrics.default": "none"},
    )
    try:
        for value in ["a", "b", None]:
            table.append(pa.table({"id": pa.array([1, 2], pa.int64()), "p": pa.array([value, value], pa.string())}))
        with unavailable_iceberg_files([task.file.file_path for task in table.scan().plan_files()]):
            frame = spark.read.format("iceberg").load(table.location())
            distinct = frame.select("p").distinct()
            grouped = frame.groupBy("p").count()
            assert {row.p for row in distinct.collect()} == {"a", "b", None}
            assert {(row.p, row["count"]) for row in grouped.collect()} == {
                ("a", 2),
                ("b", 2),
                (None, 2),
            }
            assert {
                "distinct": normalize_plan_text(distinct._explain_string()),  # noqa: SLF001
                "grouped": normalize_plan_text(grouped._explain_string()),  # noqa: SLF001
            } == snapshot
    finally:
        catalog.drop_table(identifier)
