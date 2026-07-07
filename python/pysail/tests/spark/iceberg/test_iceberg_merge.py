from pathlib import Path

import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestContent, read_manifest_list

from pysail.testing.spark.steps.iceberg import (
    _current_manifest_list,
    _current_snapshot,
    _find_latest_metadata,
    _pyarrow_input_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _uri_sql(path: Path) -> str:
    return escape_sql_string_literal(path.as_uri())


def _drop_table(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def _current_manifests(table_path: Path) -> list[dict]:
    metadata = _find_latest_metadata(table_path)
    return _current_manifest_list(metadata)["manifests"]


def _manifest_count(manifests: list[dict], *, content: str, key: str) -> int:
    return sum(manifest.get(key) or 0 for manifest in manifests if manifest.get("content") == content)


def _current_manifest_entries(table_path: Path, content: ManifestContent):
    metadata = _find_latest_metadata(table_path)
    snapshot = _current_snapshot(metadata)
    io = PyArrowFileIO()
    entries = []
    for manifest in read_manifest_list(_pyarrow_input_file(io, snapshot["manifest-list"])):
        if manifest.content == content:
            entries.extend(manifest.fetch_manifest_entry(io))
    return entries


def test_iceberg_merge_mor_update_delete_insert_writes_delete_manifest_and_reads_rows(spark, tmp_path):
    table_name = "iceberg_merge_mor_rows"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              value STRING,
              flag STRING
            )
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '2',
              'write.merge.mode' = 'merge-on-read',
              'write.data.path' = 'custom_data'
            )
            """
        )
        spark.sql(
            """
            INSERT INTO iceberg_merge_mor_rows
            SELECT * FROM VALUES
              (1, 'old', 'keep'),
              (2, 'old', 'update'),
              (3, 'old', 'delete')
            """
        )
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_mor_source AS
            SELECT * FROM VALUES
              (2, 'new', 'insert'),
              (3, 'ignored', 'delete'),
              (4, 'ins', 'insert')
            AS src(id, value, flag)
            """
        )

        spark.sql(
            """
            MERGE INTO iceberg_merge_mor_rows AS t
            USING iceberg_merge_mor_source AS s
            ON t.id = s.id
            WHEN MATCHED AND t.flag = 'update' THEN
              UPDATE SET value = s.value
            WHEN MATCHED AND t.flag = 'delete' THEN
              DELETE
            WHEN NOT MATCHED THEN
              INSERT (id, value, flag) VALUES (s.id, s.value, s.flag)
            """
        )

        rows = [
            tuple(row) for row in spark.sql("SELECT id, value, flag FROM iceberg_merge_mor_rows ORDER BY id").collect()
        ]
        assert rows == [
            (1, "old", "keep"),
            (2, "new", "update"),
            (4, "ins", "insert"),
        ]

        metadata = _find_latest_metadata(table_path)
        current_snapshot = _current_snapshot(metadata)
        assert current_snapshot["summary"]["operation"] == "overwrite"
        expected_snapshot_count = 2
        assert len(metadata["snapshots"]) == expected_snapshot_count

        manifests = _current_manifest_list(metadata)["manifests"]
        expected_deleted_row_count = 2
        assert _manifest_count(manifests, content="deletes", key="added-files-count") >= 1
        assert _manifest_count(manifests, content="deletes", key="added-rows-count") >= expected_deleted_row_count
        assert _manifest_count(manifests, content="data", key="deleted-files-count") == 0
        assert _manifest_count(manifests, content="data", key="deleted-rows-count") == 0

        custom_prefix = f"{(table_path / 'custom_data').as_uri()}/"
        data_entries = _current_manifest_entries(table_path, ManifestContent.DATA)
        delete_entries = _current_manifest_entries(table_path, ManifestContent.DELETES)
        assert data_entries
        assert delete_entries
        assert all(entry.data_file.file_path.startswith(custom_prefix) for entry in data_entries)
        assert all(entry.data_file.file_path.startswith(f"{custom_prefix}delete-") for entry in delete_entries)
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_rejects_multiple_source_rows_for_one_target_row(spark, tmp_path):
    table_name = "iceberg_merge_cardinality"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              value STRING
            )
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '2',
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO iceberg_merge_cardinality VALUES (1, 'target')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_cardinality_source AS
            SELECT * FROM VALUES
              (1, 'source-a'),
              (1, 'source-b')
            AS src(id, value)
            """
        )

        with pytest.raises(Exception, match=r"MERGE_CARDINALITY_VIOLATION|Multiple source rows"):
            spark.sql(
                f"""
                MERGE INTO {table_name} AS t
                USING iceberg_merge_cardinality_source AS s
                ON t.id = s.id
                WHEN MATCHED THEN
                  UPDATE SET value = s.value
                WHEN NOT MATCHED THEN
                  INSERT *
                """
            ).collect()

        rows = [tuple(row) for row in spark.sql("SELECT id, value FROM iceberg_merge_cardinality").collect()]
        assert rows == [(1, "target")]
        assert len(_current_manifests(table_path)) == 1
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_rejects_position_deletes_on_v1_table(spark, tmp_path):
    table_name = "iceberg_merge_v1_position_delete"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              value STRING
            )
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '1',
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO iceberg_merge_v1_position_delete VALUES (1, 'old')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_v1_source AS
            SELECT * FROM VALUES (1, 'new') AS src(id, value)
            """
        )

        with pytest.raises(Exception, match=r"format-version 2|position delete writes"):
            spark.sql(
                f"""
                MERGE INTO {table_name} AS t
                USING iceberg_merge_v1_source AS s
                ON t.id = s.id
                WHEN MATCHED THEN
                  UPDATE SET value = s.value
                """
            ).collect()

        rows = [tuple(row) for row in spark.sql("SELECT id, value FROM iceberg_merge_v1_position_delete").collect()]
        assert rows == [(1, "old")]
        assert len(_find_latest_metadata(table_path)["snapshots"]) == 1
    finally:
        _drop_table(spark, table_name)
