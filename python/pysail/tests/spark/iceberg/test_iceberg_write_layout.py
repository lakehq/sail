# ruff: noqa: S608

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import LongType, NestedField, TimestampType

from pysail.testing.spark.steps.iceberg import _find_latest_metadata
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path


@pytest.mark.parametrize("format_version", [2, 3])
def test_cow_bucket_pruning_without_metrics(spark, sql_catalog, tmp_path, format_version):
    name = "cow_bucket_pruning"
    updated_id = 7
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "value", LongType(), required=False)),
        partition_spec=PartitionSpec(PartitionField(1, 1000, BucketTransform(16), "id_bucket")),
        properties={"format-version": "2", "write.metadata.metrics.default": "none"},
    )
    moved = []
    try:
        table.append(pa.table({"id": list(range(128)), "value": list(range(128))}))
        files = table.inspect.files().to_pylist()
        assert all(not file["lower_bounds"] for file in files)
        bucket = BucketTransform(16).transform(LongType())(updated_id)
        for index, file in enumerate(files):
            if file["partition"]["id_bucket"] != bucket:
                path = _local_file_path(file["file_path"])
                backup = tmp_path / f"unread-{index}.parquet"
                path.rename(backup)
                moved.append((path, backup))
        assert moved
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
        if format_version == 3:  # noqa: PLR2004
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"UPDATE {name} SET value=-1 WHERE id={updated_id}").collect()
        for path, backup in moved:
            backup.rename(path)
        moved.clear()
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [
            (i, -1 if i == updated_id else i) for i in range(128)
        ]
    finally:
        for path, backup in moved:
            backup.rename(path)
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("include_null", [False, True])
@pytest.mark.parametrize("whole_day", [False, True])
def test_cow_day_metadata_delete_without_metrics(spark, sql_catalog, tmp_path, format_version, include_null, whole_day):
    name = "cow_day_delete"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "ts", TimestampType(), required=False)),
        partition_spec=PartitionSpec(PartitionField(2, 1000, DayTransform(), "day")),
        properties={"format-version": "2", "write.metadata.metrics.default": "none"},
    )
    moved = []
    try:
        table.append(
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "ts": pa.array(
                        [datetime(2026, 1, 1), datetime(2026, 1, 1, 23, 59), datetime(2026, 1, 2), None],  # noqa: DTZ001
                        type=pa.timestamp("us"),
                    ),
                }
            )
        )
        path = _local_file_path(table.location())
        for index, file in enumerate(table.inspect.files().to_pylist()):
            assert not file["lower_bounds"]
            if not whole_day:
                continue
            source = _local_file_path(file["file_path"])
            backup = tmp_path / f"unread-{index}.parquet"
            source.rename(backup)
            moved.append((source, backup))
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
        if format_version == 3:  # noqa: PLR2004
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        start = "2026-01-01" if whole_day else "2026-01-01 12:00:00"
        condition = f"ts >= TIMESTAMP_NTZ '{start}' AND ts < TIMESTAMP_NTZ '2026-01-02'"
        if include_null:
            condition = f"({condition}) OR ts IS NULL"
        spark.sql(f"DELETE FROM {name} WHERE {condition}").collect()
        for source, backup in moved:
            backup.rename(source)
        moved.clear()
        expected = ([] if whole_day else [1]) + [3] + ([] if include_null else [4])
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == expected
        assert all(entry.data_file.record_count == 1 for entry in _current_manifest_entries(path, ManifestContent.DATA))
    finally:
        for source, backup in moved:
            backup.rename(source)
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


@pytest.mark.parametrize("format_version", [2, 3])
def test_cow_output_partition_sort_and_compression(spark, sql_catalog, format_version):
    name = "cow_sorted_output"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            *[NestedField(i, column, LongType(), required=False) for i, column in enumerate(["id", "value", "part"], 1)]
        ),
        partition_spec=PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "part")),
        sort_order=SortOrder(
            SortField(
                source_id=2,
                transform=IdentityTransform(),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_FIRST,
            )
        ),
        properties={"format-version": "2"},
    )
    try:
        for offset in range(3):
            table.append(
                pa.table(
                    {"id": [offset * 2, offset * 2 + 1], "value": pa.array([offset, None], pa.int64()), "part": [0, 0]}
                )
            )
        path = _local_file_path(table.location())
        before = _find_latest_metadata(path)
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
        if format_version == 3:  # noqa: PLR2004
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        spark.sql(f"UPDATE {name} SET value=value+10").collect()
        files = _current_manifest_entries(path, ManifestContent.DATA)
        assert len(files) == 1
        file = files[0].data_file
        assert file.sort_order_id == before["default-sort-order-id"]
        parquet = pq.ParquetFile(_local_file_path(file.file_path))
        assert parquet.read(columns=["value"]).column("value").to_pylist() == [None, None, None, 12, 11, 10]
        assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"
        assert file.file_size_in_bytes == _local_file_path(file.file_path).stat().st_size
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(6))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


def test_iceberg_writer_options_override_properties_and_roll_files(spark, tmp_path):
    path = tmp_path / "rolling"
    row_count = 2200
    spark.sql(f"""CREATE TABLE rolling_files (id BIGINT, payload STRING) USING iceberg LOCATION '{path.as_uri()}'
        TBLPROPERTIES ('write.parquet.compression-codec'='snappy', 'write.target-file-size-bytes'='536870912')""")
    try:
        spark.range(row_count).selectExpr("id", "repeat(sha2(cast(id AS STRING),256),16) AS payload").coalesce(
            1
        ).write.format("iceberg").mode("append").option("compression-codec", "uncompressed").option(
            "target-file-size-bytes", "32768"
        ).save(path.as_uri())
        files = _current_manifest_entries(path, ManifestContent.DATA)
        assert len(files) > 1
        assert sum(entry.data_file.record_count for entry in files) == row_count
        assert all(0 < entry.data_file.record_count < row_count for entry in files)
        for entry in files:
            file = entry.data_file
            parquet = pq.ParquetFile(_local_file_path(file.file_path))
            assert parquet.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"
            assert parquet.metadata.num_rows == file.record_count
            assert _local_file_path(file.file_path).stat().st_size == file.file_size_in_bytes
        assert [row.id for row in spark.table("rolling_files").orderBy("id").collect()] == list(range(row_count))
    finally:
        spark.sql("DROP TABLE IF EXISTS rolling_files")
