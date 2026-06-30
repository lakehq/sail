from pathlib import Path

import pytest

from pysail.testing.spark.steps.iceberg import _current_manifest_list, _current_snapshot, _find_latest_metadata
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
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql(
            f"""
            INSERT INTO {table_name}
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
            f"""
            MERGE INTO {table_name} AS t
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
            tuple(row)
            for row in spark.sql(f"SELECT id, value, flag FROM {table_name} ORDER BY id").collect()
        ]
        assert rows == [
            (1, "old", "keep"),
            (2, "new", "update"),
            (4, "ins", "insert"),
        ]

        metadata = _find_latest_metadata(table_path)
        current_snapshot = _current_snapshot(metadata)
        assert current_snapshot["summary"]["operation"] == "overwrite"
        assert len(metadata["snapshots"]) == 2

        manifests = _current_manifest_list(metadata)["manifests"]
        assert _manifest_count(manifests, content="deletes", key="added-files-count") >= 1
        assert _manifest_count(manifests, content="deletes", key="added-rows-count") >= 2
        assert _manifest_count(manifests, content="data", key="deleted-files-count") == 0
        assert _manifest_count(manifests, content="data", key="deleted-rows-count") == 0
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
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'target')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_cardinality_source AS
            SELECT * FROM VALUES
              (1, 'source-a'),
              (1, 'source-b')
            AS src(id, value)
            """
        )

        with pytest.raises(Exception, match="MERGE_CARDINALITY_VIOLATION|Multiple source rows"):
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

        rows = [tuple(row) for row in spark.sql(f"SELECT id, value FROM {table_name}").collect()]
        assert rows == [(1, "target")]
        assert len(_current_manifests(table_path)) == 1
    finally:
        _drop_table(spark, table_name)
