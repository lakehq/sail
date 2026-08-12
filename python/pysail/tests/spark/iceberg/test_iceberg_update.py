# ruff: noqa: S608

from pathlib import Path

import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus, read_manifest_list

from pysail.testing.spark.steps.iceberg import (
    _current_manifest_list,
    _current_snapshot,
    _find_latest_metadata,
    _latest_metadata_path,
    _pyarrow_input_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _uri_sql(path: Path) -> str:
    return escape_sql_string_literal(path.as_uri())


def _drop_table(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def _parquet_file_paths(table_path: Path) -> set[Path]:
    return {path.relative_to(table_path) for path in table_path.rglob("*.parquet")}


@pytest.mark.parametrize("update_mode", [None, "copy-on-write"], ids=["default", "explicit-cow"])
def test_iceberg_update_copy_on_write_preserves_false_and_unknown_predicates(spark, tmp_path, update_mode):
    table_name = "iceberg_update_copy_on_write"
    table_path = tmp_path / table_name
    mode_property = "" if update_mode is None else f", 'write.update.mode' = '{update_mode}'"

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value STRING, selected BOOLEAN)
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES ('format-version' = '2'{mode_property})
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'change', true), (2, 'keep', false), (3, 'unknown', NULL)")

        spark.sql(
            f"""
            UPDATE {table_name} AS target
            SET value = concat(target.value, '-updated')
            WHERE target.selected
            """
        ).collect()

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value, selected FROM {table_name} ORDER BY id").collect()]
        assert rows == [
            (1, "change-updated", True),
            (2, "keep", False),
            (3, "unknown", None),
        ]
        metadata = _find_latest_metadata(table_path)
        snapshot = _current_snapshot(metadata)
        assert snapshot["summary"]["operation"] == "overwrite"
        assert snapshot["summary"]["deleted-data-files"] == "1"
        assert snapshot["summary"]["deleted-records"] == "3"
        assert snapshot["summary"]["added-records"] == "3"
        assert all(manifest.get("content") == "data" for manifest in _current_manifest_list(metadata)["manifests"])
    finally:
        _drop_table(spark, table_name)


def test_iceberg_update_copy_on_write_without_predicate_accepts_path_target(spark, tmp_path):
    table_name = "iceberg_update_copy_on_write_path"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value INT)
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES ('format-version' = '2')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 10), (2, 20)")

        spark.sql(
            f"""
            UPDATE iceberg.`{table_path.as_uri()}` AS target
            SET value = target.value + 1
            """
        ).collect()

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value FROM {table_name} ORDER BY id").collect()]
        assert rows == [(1, 11), (2, 21)]
    finally:
        _drop_table(spark, table_name)


def test_iceberg_update_copy_on_write_can_move_rows_between_partitions(spark, tmp_path):
    table_name = "iceberg_update_copy_on_write_partition"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value STRING, part STRING)
            USING iceberg
            PARTITIONED BY (part)
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'copy-on-write')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'move', 'A'), (2, 'stay-a', 'A')")
        spark.sql(f"INSERT INTO {table_name} VALUES (3, 'stay-b', 'B')")
        before_manifest_paths = {
            manifest["manifest-path"]
            for manifest in _current_manifest_list(_find_latest_metadata(table_path))["manifests"]
        }

        spark.sql(f"UPDATE {table_name} SET part = 'B' WHERE id = 1").collect()

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value, part FROM {table_name} ORDER BY id").collect()]
        assert rows == [(1, "move", "B"), (2, "stay-a", "A"), (3, "stay-b", "B")]
        metadata = _find_latest_metadata(table_path)
        snapshot = _current_snapshot(metadata)
        assert snapshot["summary"]["operation"] == "overwrite"
        assert snapshot["summary"]["deleted-data-files"] == "1"
        after_manifest_paths = {manifest["manifest-path"] for manifest in _current_manifest_list(metadata)["manifests"]}
        assert before_manifest_paths & after_manifest_paths
    finally:
        _drop_table(spark, table_name)


def test_iceberg_copy_on_write_rewrites_live_rows_after_merge_on_read_delete(spark, tmp_path):
    table_name = "iceberg_cow_after_mor_delete"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, value STRING)
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '2',
              'write.delete.mode' = 'merge-on-read',
              'write.update.mode' = 'copy-on-write'
            )
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'update'), (2, 'mor-delete'), (3, 'keep')")
        spark.sql(f"DELETE FROM {table_name} WHERE id = 2").collect()

        spark.sql(f"UPDATE {table_name} SET value = 'updated' WHERE id = 1").collect()

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value FROM {table_name} ORDER BY id").collect()]
        assert rows == [(1, "updated"), (3, "keep")]
        metadata = _find_latest_metadata(table_path)
        snapshot = _current_snapshot(metadata)
        assert snapshot["summary"]["operation"] == "overwrite"
        manifests = _current_manifest_list(metadata)["manifests"]
        assert any(manifest.get("content") == "deletes" for manifest in manifests)
        assert snapshot["summary"]["total-delete-files"] == "1"
    finally:
        _drop_table(spark, table_name)


@pytest.mark.parametrize("format_version", ["1", "2"])
def test_iceberg_copy_on_write_operations_support_table_format_versions(spark, tmp_path, format_version):
    table_name = f"iceberg_cow_format_v{format_version}"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value STRING)
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES ('format-version' = '{format_version}')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'old'), (2, 'delete')")
        spark.sql(f"UPDATE {table_name} SET value = 'updated' WHERE id = 1").collect()
        spark.sql("CREATE OR REPLACE TEMP VIEW iceberg_cow_format_source AS SELECT 3 AS id, 'inserted' AS value")
        spark.sql(
            f"""
            MERGE INTO {table_name} AS target
            USING iceberg_cow_format_source AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET value = source.value
            WHEN NOT MATCHED THEN INSERT *
            """
        ).collect()
        spark.sql(f"DELETE FROM {table_name} WHERE id = 2").collect()

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value FROM {table_name} ORDER BY id").collect()]
        assert rows == [(1, "updated"), (3, "inserted")]
        metadata = _find_latest_metadata(table_path)
        snapshot = _current_snapshot(metadata)
        if format_version == "1":
            assert "last-sequence-number" not in metadata
            assert all("sequence-number" not in item for item in metadata["snapshots"])
        else:
            assert metadata["last-sequence-number"] == snapshot["sequence-number"]
        io = PyArrowFileIO()
        entries = [
            entry
            for manifest in read_manifest_list(_pyarrow_input_file(io, snapshot["manifest-list"]))
            if manifest.content == ManifestContent.DATA
            for entry in manifest.fetch_manifest_entry(io)
        ]
        assert sum(
            entry.data_file.record_count for entry in entries if entry.status != ManifestEntryStatus.DELETED
        ) == len(rows)
        assert all(entry.snapshot_id for entry in entries)
    finally:
        _drop_table(spark, table_name)


def test_iceberg_v3_copy_on_write_rejects_target_rewrites_without_side_effects(spark, tmp_path):
    table_name = "iceberg_cow_v3_row_lineage_reject"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, value STRING)
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES ('format-version' = '3')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'old')")
        spark.sql("CREATE OR REPLACE TEMP VIEW iceberg_cow_v3_insert_source AS SELECT 2 AS id, 'inserted' AS value")
        spark.sql(
            f"""
            MERGE INTO {table_name} AS target
            USING iceberg_cow_v3_insert_source AS source
            ON target.id = source.id
            WHEN NOT MATCHED THEN INSERT *
            """
        ).collect()
        assert _current_snapshot(_find_latest_metadata(table_path))["summary"]["operation"] == "append"

        spark.sql("CREATE OR REPLACE TEMP VIEW iceberg_cow_v3_match_source AS SELECT 1 AS id, 'new' AS value")
        before_metadata_path = _latest_metadata_path(table_path)
        before_parquet_files = _parquet_file_paths(table_path)
        statements = [
            f"UPDATE {table_name} SET value = 'updated' WHERE id = 1",
            f"DELETE FROM {table_name} WHERE id = 1",
            f"""
            MERGE INTO {table_name} AS target
            USING iceberg_cow_v3_match_source AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET value = source.value
            """,
        ]
        for statement in statements:
            with pytest.raises(Exception, match=r"v3 copy-on-write.*row lineage"):
                spark.sql(statement).collect()
            assert _latest_metadata_path(table_path) == before_metadata_path
            assert _parquet_file_paths(table_path) == before_parquet_files

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value FROM {table_name} ORDER BY id").collect()]
        assert rows == [(1, "old"), (2, "inserted")]
    finally:
        _drop_table(spark, table_name)
