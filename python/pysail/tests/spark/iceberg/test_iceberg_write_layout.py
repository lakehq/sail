# ruff: noqa: S608

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import FixedType, LongType, NestedField, TimestampType, UUIDType

from pysail.testing.spark.steps.iceberg import _current_deletion_vectors, _current_snapshot, _find_latest_metadata
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


@pytest.mark.parametrize("mode", [None, "none", "counts", "truncate(4)", "full"])
def test_file_metrics_modes_preserve_conservative_long_bounds(spark, tmp_path, mode):
    path = tmp_path / "metrics"
    properties = "'write.metadata.metrics.column.id'='full', 'write.metadata.metrics.column.st.x'='counts'"
    if mode is not None:
        properties += f", 'write.metadata.metrics.default'='{mode}'"
    spark.sql(f"""CREATE TABLE file_metrics (id BIGINT, s STRING, b BINARY, st STRUCT<x: STRING>)
        USING iceberg LOCATION '{path.as_uri()}' TBLPROPERTIES ({properties})""")
    lower_text, upper_text = "界😀" + "a" * 80, "界😀" + "z" * 80
    lower_binary, upper_binary = b"\x01\xff" * 40, b"\x02\xff" * 40
    rows = [
        (1, lower_text, lower_binary, (lower_text,)),
        (2, upper_text, upper_binary, (upper_text,)),
        (3, None, None, None),
    ]
    try:
        spark.createDataFrame(rows, "id LONG, s STRING, b BINARY, st STRUCT<x: STRING>").coalesce(1).write.format(
            "iceberg"
        ).mode("append").save(path.as_uri())
        entries = _current_manifest_entries(path, ManifestContent.DATA)
        assert len(entries) == 1
        file = entries[0].data_file
        count_ids = {1, 5} if mode == "none" else {1, 2, 3, 5}
        assert set(file.column_sizes) == count_ids
        assert file.value_counts == dict.fromkeys(count_ids, len(rows))
        assert file.null_value_counts == {field_id: int(field_id != 1) for field_id in count_ids}
        assert file.lower_bounds[1] == (1).to_bytes(8, "little", signed=True)
        assert file.upper_bounds[1] == (3).to_bytes(8, "little", signed=True)
        if mode in {"none", "counts"}:
            assert set(file.lower_bounds) == set(file.upper_bounds) == {1}
        else:
            assert set(file.lower_bounds) == set(file.upper_bounds) == {1, 2, 3}
            if mode == "full":
                expected = (lower_text, upper_text, lower_binary, upper_binary)
            else:
                width = 16 if mode is None else 4
                expected = (
                    lower_text[:width],
                    upper_text[: width - 1] + "{",
                    lower_binary[:width],
                    upper_binary[: width - 2] + b"\x03",
                )
            assert (file.lower_bounds[2].decode(), file.upper_bounds[2].decode()) == expected[:2]
            assert (file.lower_bounds[3], file.upper_bounds[3]) == expected[2:]
        actual = spark.read.format("iceberg").load(path.as_uri())
        assert [row.id for row in actual.where(actual.s == upper_text).collect()] == [2]
        assert [row.id for row in actual.where(actual.b == F.lit(upper_binary)).collect()] == [2]
        assert [row.id for row in actual.orderBy("id").collect()] == [1, 2, 3]
    finally:
        spark.sql("DROP TABLE IF EXISTS file_metrics")


@pytest.mark.parametrize("nested", [False, True])
def test_overwrite_schema_preserves_variant_storage(spark, tmp_path, nested):
    path = tmp_path / "variant_overwrite"
    value = "parse_json('{\"x\":2}')"
    column = "v"
    if nested:
        value = f"named_struct('v', {value})"
        column = "v.v"
    frame = spark.sql(f"SELECT 2 AS id, {value} AS v")
    variant_type = "STRUCT<v: VARIANT>" if nested else "VARIANT"
    spark.sql(f"""CREATE TABLE overwrite_variant (id INT, v {variant_type}) USING iceberg
        LOCATION '{path.as_uri()}' TBLPROPERTIES ('format-version'='3')""")
    try:
        frame.write.format("iceberg").mode("append").save(path.as_uri())
        before = _find_latest_metadata(path)
        frame.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").save(path.as_uri())
        after = _find_latest_metadata(path)
        schema = next(schema for schema in after["schemas"] if schema["schema-id"] == after["current-schema-id"])
        field = schema["fields"][1]
        if nested:
            field = field["type"]["fields"][0]
        assert field["type"] == "variant"
        assert after["last-column-id"] == before["last-column-id"]
        assert spark.read.format("iceberg").load(path.as_uri()).selectExpr("id", f"to_json({column})").collect() == [
            (2, '{"x":2}')
        ]
        for entry in _current_manifest_entries(path, ManifestContent.DATA):
            arrow_field = pq.ParquetFile(_local_file_path(entry.data_file.file_path)).schema_arrow.field("v")
            if nested:
                arrow_field = arrow_field.type.field("v")
            assert arrow_field.metadata[b"PARQUET:field_id"] == str(field["id"]).encode()
            assert all(b"PARQUET:field_id" not in (child.metadata or {}) for child in arrow_field.type)
    finally:
        spark.sql("DROP TABLE IF EXISTS overwrite_variant")


@pytest.mark.parametrize("format_version", [2, 3])
def test_uuid_and_fixed16_have_distinct_parquet_annotations(spark, sql_catalog, format_version):
    identifier = "default.uuid_annotations"
    table = sql_catalog.create_table(
        identifier,
        Schema(NestedField(1, "u", UUIDType(), required=False), NestedField(2, "f", FixedType(16), required=False)),
        properties={"format-version": "2"},
    )
    try:
        spark.sql(f"CREATE TABLE uuid_annotations USING iceberg LOCATION '{table.location()}'")
        if format_version == 3:  # noqa: PLR2004
            spark.sql("ALTER TABLE uuid_annotations SET TBLPROPERTIES ('format-version'='3')")
        values = [(bytes.fromhex("00112233445566778899aabbccddeeff"), b"abcdefghijklmnop")]
        spark.createDataFrame(values, "u BINARY, f BINARY").write.format("iceberg").mode("append").save(
            table.location()
        )
        path = _local_file_path(table.location())
        for entry in _current_manifest_entries(path, ManifestContent.DATA):
            schema = pq.ParquetFile(_local_file_path(entry.data_file.file_path)).schema
            assert [(column.physical_type, column.logical_type.type) for column in schema] == [
                ("FIXED_LEN_BYTE_ARRAY", "UUID"),
                ("FIXED_LEN_BYTE_ARRAY", "NONE"),
            ]
        frame = spark.read.format("iceberg").load(table.location())
        assert [tuple(row) for row in frame.collect()] == values
        copy_path = path.parent / "uuid_copy"
        frame.write.format("iceberg").save(copy_path.as_uri())
        metadata = _find_latest_metadata(copy_path)
        schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"])
        assert [field["type"] for field in schema["fields"]] == ["uuid", "fixed[16]"]
        assert [tuple(row) for row in spark.read.format("iceberg").load(copy_path.as_uri()).collect()] == values
        frame.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").save(table.location())
        metadata = _find_latest_metadata(path)
        schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"])
        assert [field["type"] for field in schema["fields"]] == ["uuid", "fixed[16]"]
        assert [tuple(row) for row in spark.read.format("iceberg").load(table.location()).collect()] == values
    finally:
        spark.sql("DROP TABLE IF EXISTS uuid_annotations")
        sql_catalog.drop_table(identifier)


def test_deletion_vectors_share_puffin_and_preserve_siblings_after_replacement(spark, tmp_path):
    path = tmp_path / "packed_vectors"
    row_count = 20000
    spark.sql(f"""CREATE TABLE packed_vectors (id BIGINT) USING iceberg LOCATION '{path.as_uri()}'
        TBLPROPERTIES ('format-version'='3', 'write.target-file-size-bytes'='1', 'write.delete.mode'='merge-on-read')""")
    try:
        spark.range(row_count).coalesce(1).writeTo("packed_vectors").append()
        spark.sql("DELETE FROM packed_vectors WHERE id % 1000 = 0").collect()
        first_vectors = _current_deletion_vectors(path)
        files = [entry.data_file for entry in _current_manifest_entries(path, ManifestContent.DELETES)]
        expected_deletes = row_count // 1000
        assert len(files) == len(first_vectors) == expected_deletes
        assert sum(map(len, first_vectors.values())) == expected_deletes
        puffins = {file.file_path for file in files}
        assert len(puffins) < len(files)
        first_snapshot = _current_snapshot(_find_latest_metadata(path))
        assert int(first_snapshot["summary"]["total-delete-files"]) == len(files)
        assert int(first_snapshot["summary"]["added-delete-files"]) == len(files)
        spark.sql("DELETE FROM packed_vectors WHERE id=1").collect()
        next_vectors = _current_deletion_vectors(path)
        assert len(next_vectors) == len(first_vectors)
        assert sum(map(len, next_vectors.values())) == expected_deletes + 1
        assert sum(first_vectors[target] != positions for target, positions in next_vectors.items()) == 1
        assert all(_local_file_path(puffin).exists() for puffin in puffins)
        expected = [i for i in range(row_count) if i % 1000 and i != 1]
        assert [row.id for row in spark.table("packed_vectors").orderBy("id").collect()] == expected
        history = (
            spark.read.format("iceberg").option("snapshot-id", str(first_snapshot["snapshot-id"])).load(path.as_uri())
        )
        assert [row.id for row in history.orderBy("id").collect()] == [i for i in range(row_count) if i % 1000]
    finally:
        spark.sql("DROP TABLE IF EXISTS packed_vectors")
