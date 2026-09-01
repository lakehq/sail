import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.avro.file import AvroFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, LongType, NestedField, StringType
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.test_iceberg_equality_delete import (
    _append_equality_delete_snapshot,
)
from pysail.tests.spark.iceberg.utils import pyiceberg_file_io_properties


def _create_partitioned_table(spark, table_name: str, location: Path) -> None:
    escaped_location = escape_sql_string_literal(str(location))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, category STRING, value BIGINT)
        USING iceberg
        PARTITIONED BY (category)
        LOCATION '{escaped_location}'
        """
    )


def _rows(spark, table_name: str) -> list[tuple[int, str, int]]:
    rows = spark.table(table_name).select("id", "category", "value").orderBy("id")
    return [tuple(row) for row in rows.collect()]


def _live_data_file_paths(location: Path) -> set[str]:
    table = StaticTable.from_metadata(
        str(location),
        properties=pyiceberg_file_io_properties(),
    )
    return {str(task.file.file_path) for task in table.scan().plan_files()}


def _metadata_file_count(location: Path) -> int:
    return len(list((location / "metadata").glob("*.metadata.json")))


def _publish_injected_metadata(location: Path) -> None:
    metadata_dir = location / "metadata"
    injected = max(metadata_dir.glob("[0-9]*-*.metadata.json"))
    version = int(injected.name.split("-", maxsplit=1)[0])
    (metadata_dir / f"v{version}.metadata.json").write_bytes(injected.read_bytes())
    (metadata_dir / "version-hint.text").write_text(str(version))


def test_iceberg_predicate_overwrite_rewrites_only_candidate_partition(spark, tmp_path):
    table_name = "iceberg_predicate_overwrite"
    location = tmp_path / table_name
    _create_partitioned_table(spark, table_name, location)
    try:
        spark.createDataFrame(
            [(1, "A", 10), (2, "B", 20), (3, "A", 30), (4, "B", 40)],
            schema="id BIGINT, category STRING, value BIGINT",
        ).writeTo(table_name).append()
        initial_live_files = _live_data_file_paths(location)

        spark.createDataFrame(
            [(5, "A", 100), (6, "A", 200)],
            schema="id BIGINT, category STRING, value BIGINT",
        ).writeTo(table_name).overwrite(F.col("category") == "A")

        assert _rows(spark, table_name) == [
            (2, "B", 20),
            (4, "B", 40),
            (5, "A", 100),
            (6, "A", 200),
        ]
        assert initial_live_files & _live_data_file_paths(location)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_predicate_overwrite_supports_date_identity_partition(spark, tmp_path):
    table_name = "iceberg_predicate_overwrite_date_partition"
    location = tmp_path / table_name
    escaped_location = escape_sql_string_literal(str(location))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, event_date DATE, value BIGINT)
        USING iceberg
        PARTITIONED BY (event_date)
        LOCATION '{escaped_location}'
        """
    )
    try:
        schema = "id BIGINT, event_date DATE, value BIGINT"
        first_day = date(2026, 1, 1)
        second_day = date(2026, 1, 2)
        spark.createDataFrame(
            [(1, first_day, 10), (2, second_day, 20), (3, first_day, 30)],
            schema=schema,
        ).writeTo(table_name).append()
        initial_live_files = _live_data_file_paths(location)

        spark.createDataFrame(
            [(4, first_day, 100)],
            schema=schema,
        ).writeTo(table_name).overwrite(F.col("event_date") == first_day)

        rows = spark.table(table_name).select("id", "event_date", "value").orderBy("id")
        assert [tuple(row) for row in rows.collect()] == [
            (2, second_day, 20),
            (4, first_day, 100),
        ]
        assert initial_live_files & _live_data_file_paths(location)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_predicate_overwrite_rejects_non_partition_column(spark, tmp_path):
    table_name = "iceberg_predicate_overwrite_non_partition"
    location = tmp_path / table_name
    _create_partitioned_table(spark, table_name, location)
    try:
        schema = "id BIGINT, category STRING, value BIGINT"
        spark.createDataFrame([(1, "A", 10)], schema=schema).writeTo(table_name).append()
        replacement = spark.createDataFrame([(2, "A", 20)], schema=schema)
        with pytest.raises(Exception, match="identity-partition columns"):
            replacement.writeTo(table_name).overwrite(F.col("value").isNotNull())
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("mode", ["predicate", "dynamic"])
def test_iceberg_scoped_overwrite_rejects_v3_table(spark, tmp_path, mode):
    table_name = f"iceberg_scoped_overwrite_v3_{mode}"
    location = tmp_path / table_name
    _create_partitioned_table(spark, table_name, location)
    try:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')")
        schema = "id BIGINT, category STRING, value BIGINT"
        replacement = spark.createDataFrame([(2, "A", 20)], schema=schema)
        writer = replacement.writeTo(table_name)

        def overwrite():
            if mode == "predicate":
                writer.overwrite(F.col("category") == "A")
            else:
                writer.overwritePartitions()

        with pytest.raises(Exception, match=r"v3.*overwrite"):
            overwrite()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("mode", ["predicate", "dynamic"])
def test_iceberg_scoped_overwrite_rejects_active_delete_files(spark, tmp_path, mode):
    table_name = f"iceberg_scoped_overwrite_active_delete_{mode}"
    location = tmp_path / table_name
    _create_partitioned_table(spark, table_name, location)
    try:
        schema = "id BIGINT, category STRING, value BIGINT"
        spark.createDataFrame([(1, "A", 10), (2, "B", 20), (3, "A", 30)], schema=schema).writeTo(table_name).append()
        table = StaticTable.from_metadata(str(location), properties=pyiceberg_file_io_properties())
        _append_equality_delete_snapshot(
            table,
            pa.table({"id": [1]}),
            [1],
            partition=Record("A"),
        )
        _publish_injected_metadata(location)
        spark.sql(f"DROP TABLE {table_name}")
        escaped_location = escape_sql_string_literal(str(location))
        spark.sql(f"CREATE TABLE {table_name} USING iceberg LOCATION '{escaped_location}'")
        expected_rows = [(2, "B", 20), (3, "A", 30)]
        assert _rows(spark, table_name) == expected_rows
        metadata_files_before = _metadata_file_count(location)

        replacement = spark.createDataFrame([(4, "A", 40)], schema=schema)

        def overwrite():
            if mode == "predicate":
                replacement.writeTo(table_name).overwrite(F.col("category") == "A")
            else:
                replacement.writeTo(table_name).overwritePartitions()

        with pytest.raises(Exception, match="active delete files"):
            overwrite()

        assert _rows(spark, table_name) == expected_rows
        assert _metadata_file_count(location) == metadata_files_before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_dynamic_partition_overwrite_preserves_untouched_partitions(spark, tmp_path):
    table_name = "iceberg_dynamic_partition_overwrite"
    location = tmp_path / table_name
    _create_partitioned_table(spark, table_name, location)
    try:
        schema = "id BIGINT, category STRING, value BIGINT"
        spark.createDataFrame(
            [(1, "A", 10), (2, "B", 20), (3, "A", 30), (4, "B", 40)],
            schema=schema,
        ).writeTo(table_name).append()

        spark.createDataFrame(
            [(5, "A", 100), (6, "A", 200)],
            schema=schema,
        ).writeTo(table_name).overwritePartitions()
        assert _rows(spark, table_name) == [
            (2, "B", 20),
            (4, "B", 40),
            (5, "A", 100),
            (6, "A", 200),
        ]

        spark.createDataFrame(
            [(7, "C", 300)],
            schema=schema,
        ).writeTo(table_name).overwritePartitions()
        assert _rows(spark, table_name) == [
            (2, "B", 20),
            (4, "B", 40),
            (5, "A", 100),
            (6, "A", 200),
            (7, "C", 300),
        ]

        metadata_files_before = _metadata_file_count(location)
        spark.createDataFrame([], schema=schema).writeTo(table_name).overwritePartitions()
        assert _metadata_file_count(location) == metadata_files_before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_dynamic_overwrite_matches_promoted_partition_values(spark, sql_catalog):
    identifier = "default.dynamic_overwrite_promoted_partition"
    table_name = "iceberg_dynamic_overwrite_promoted_partition"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "part", IntegerType(), required=False),
            NestedField(3, "value", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "part")),
    )
    try:
        table.append(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "part": pa.array([7, 8], type=pa.int32()),
                    "value": pa.array(["replace", "keep"], type=pa.string()),
                }
            )
        )
        table.update_schema().update_column("part", LongType()).commit()

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        location = escape_sql_string_literal(table.location())
        spark.sql(f"CREATE TABLE {table_name} USING iceberg LOCATION '{location}'")
        spark.createDataFrame(
            [(3, 7, "replacement")],
            schema="id BIGINT, part BIGINT, value STRING",
        ).writeTo(table_name).overwritePartitions()

        rows = spark.table(table_name).select("id", "part", "value").orderBy("id").collect()
        assert [tuple(row) for row in rows] == [
            (2, 8, "keep"),
            (3, 7, "replacement"),
        ]
        external_table = StaticTable.from_metadata(
            table.location(),
            properties=pyiceberg_file_io_properties(),
        )
        rewritten_manifest = next(
            manifest
            for manifest in external_table.current_snapshot().manifests(external_table.io)
            if manifest.deleted_files_count
        )
        with AvroFile(external_table.io.new_input(rewritten_manifest.manifest_path)) as manifest:
            manifest_schema = json.loads(manifest.header.meta["schema"])
        part_field = next(field for field in manifest_schema["fields"] if field["name"] == "part")
        assert part_field["type"] == "long"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        sql_catalog.drop_table(identifier)


def test_iceberg_v1_dynamic_overwrite_writes_v1_metadata_shapes(spark, tmp_path):
    table_name = "iceberg_v1_dynamic_partition_overwrite"
    location = tmp_path / table_name
    escaped_location = escape_sql_string_literal(str(location))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, category STRING, value BIGINT)
        USING iceberg
        PARTITIONED BY (category)
        LOCATION '{escaped_location}'
        TBLPROPERTIES ('format-version' = '1')
        """
    )
    try:
        schema = "id BIGINT, category STRING, value BIGINT"
        spark.createDataFrame(
            [(1, "A", 10), (2, "B", 20), (3, "A", 30)],
            schema=schema,
        ).writeTo(table_name).append()
        spark.createDataFrame([(4, "A", 40)], schema=schema).writeTo(table_name).overwritePartitions()

        assert _rows(spark, table_name) == [(2, "B", 20), (4, "A", 40)]
        assert len(_live_data_file_paths(location)) == 2

        metadata_dir = location / "metadata"
        version = int((metadata_dir / "version-hint.text").read_text(encoding="utf-8"))
        metadata = json.loads((metadata_dir / f"v{version}.metadata.json").read_text(encoding="utf-8"))
        assert metadata["format-version"] == 1
        assert metadata["schema"]["schema-id"] == 0
        assert metadata["partition-spec"] == [
            {"source-id": 2, "field-id": 1000, "name": "category", "transform": "identity"}
        ]
        assert metadata["table-uuid"]
        assert "last-sequence-number" not in metadata
        assert metadata["refs"]["main"]["snapshot-id"] == metadata["current-snapshot-id"]
        assert all("sequence-number" not in snapshot for snapshot in metadata["snapshots"])
        assert metadata["snapshots"][-1]["summary"]["operation"] == "overwrite"

        external_table = StaticTable.from_metadata(
            str(location),
            properties=pyiceberg_file_io_properties(),
        )
        current_snapshot = external_table.current_snapshot()
        with AvroFile(external_table.io.new_input(current_snapshot.manifest_list)) as manifest_list:
            manifest_list_schema = json.loads(manifest_list.header.meta["avro.schema"])
        assert [field["name"] for field in manifest_list_schema["fields"]] == [
            "manifest_path",
            "manifest_length",
            "partition_spec_id",
            "added_snapshot_id",
            "added_files_count",
            "existing_files_count",
            "deleted_files_count",
            "added_rows_count",
            "existing_rows_count",
            "deleted_rows_count",
            "partitions",
            "key_metadata",
        ]

        manifest_path = current_snapshot.manifests(external_table.io)[0].manifest_path
        with AvroFile(external_table.io.new_input(manifest_path)) as manifest:
            manifest_schema = json.loads(manifest.header.meta["avro.schema"])
        snapshot_id = next(field for field in manifest_schema["fields"] if field["name"] == "snapshot_id")
        assert snapshot_id["type"] == ["null", "long"]
        data_file = next(field for field in manifest_schema["fields"] if field["name"] == "data_file")
        assert [field["name"] for field in data_file["type"]["fields"]] == [
            "file_path",
            "file_format",
            "partition",
            "record_count",
            "file_size_in_bytes",
            "block_size_in_bytes",
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
            "key_metadata",
            "split_offsets",
            "sort_order_id",
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_appends_after_legacy_v1_metadata(spark, sql_catalog):
    identifier = "default.legacy_v1_append"
    table_name = "iceberg_legacy_v1_append"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "value", StringType(), required=False),
        ),
        properties={"format-version": "1"},
    )
    try:
        table.append(
            pa.table(
                {
                    "id": pa.array([1], type=pa.int64()),
                    "value": pa.array(["first"], type=pa.string()),
                }
            )
        )
        with table.io.new_input(table.metadata_location).open() as stream:
            metadata = json.load(stream)
        for field in (
            "schemas",
            "current-schema-id",
            "partition-specs",
            "default-spec-id",
            "last-partition-id",
            "refs",
        ):
            metadata.pop(field, None)
        with table.io.new_output(table.metadata_location).create(overwrite=True) as stream:
            stream.write(json.dumps(metadata).encode())

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        location = escape_sql_string_literal(table.location())
        spark.sql(f"CREATE TABLE {table_name} USING iceberg LOCATION '{location}'")
        spark.createDataFrame(
            [(2, "second")],
            schema="id BIGINT, value STRING",
        ).writeTo(table_name).append()

        rows = spark.table(table_name).select("id", "value").orderBy("id").collect()
        assert [tuple(row) for row in rows] == [(1, "first"), (2, "second")]
        external_table = StaticTable.from_metadata(
            table.location(),
            properties=pyiceberg_file_io_properties(),
        )
        external_rows = sorted((row["id"], row["value"]) for row in external_table.scan().to_arrow().to_pylist())
        assert external_rows == [(1, "first"), (2, "second")]
        assert external_table.metadata.format_version == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        sql_catalog.drop_table(identifier)


def test_iceberg_v1_append_preserves_non_main_snapshot_ref(spark, sql_catalog):
    identifier = "default.v1_snapshot_ref"
    table_name = "iceberg_v1_snapshot_ref"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(NestedField(1, "id", LongType(), required=False)),
        properties={"format-version": "1"},
    )
    try:
        table.append(pa.table({"id": pa.array([1], type=pa.int64())}))
        branch_snapshot_id = table.current_snapshot().snapshot_id
        table.manage_snapshots().create_branch(branch_snapshot_id, "audit").commit()

        location = escape_sql_string_literal(table.location())
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING iceberg LOCATION '{location}'")
        spark.createDataFrame([(2,)], schema="id BIGINT").writeTo(table_name).append()

        external_table = StaticTable.from_metadata(
            table.location(),
            properties=pyiceberg_file_io_properties(),
        )
        assert external_table.snapshot_by_name("audit").snapshot_id == branch_snapshot_id
        assert external_table.current_snapshot().snapshot_id != branch_snapshot_id
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        sql_catalog.drop_table(identifier)
