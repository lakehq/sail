from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.table import StaticTable
from pyiceberg.typedef import Record
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
