from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFileContent, ManifestContent, read_manifest_list
from pyiceberg.typedef import Record
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from pysail.testing.spark.steps.iceberg import (
    _current_manifest_list,
    _current_snapshot,
    _find_latest_metadata,
    _latest_metadata_path,
    _metadata_file_version,
    _pyarrow_input_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal

UNPARTITIONED_LAST_PARTITION_ID = 999


def _uri_sql(path: Path) -> str:
    return escape_sql_string_literal(path.as_uri())


def _local_file_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        pytest.fail(f"cannot inspect non-local file URI: {uri}")
    return Path(url2pathname(f"//{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path))


def _drop_table(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def _parquet_file_paths(table_path: Path) -> set[Path]:
    return {path.relative_to(table_path) for path in table_path.rglob("*.parquet")}


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


def _assert_current_snapshot_metadata(
    table_path: Path,
    metadata: dict,
    *,
    expected_metadata_version: int,
    expected_snapshot_count: int,
) -> dict:
    snapshot = _current_snapshot(metadata)
    assert metadata["current-snapshot-id"] == snapshot["snapshot-id"]
    assert metadata["last-sequence-number"] == snapshot["sequence-number"]
    assert metadata["last-partition-id"] == UNPARTITIONED_LAST_PARTITION_ID
    assert metadata["refs"]["main"]["snapshot-id"] == snapshot["snapshot-id"]
    assert metadata["snapshot-log"][-1]["snapshot-id"] == snapshot["snapshot-id"]
    assert len(metadata["snapshots"]) == expected_snapshot_count
    assert _metadata_file_version(_latest_metadata_path(table_path)) == expected_metadata_version
    assert metadata["metadata-log"][-1]["metadata-file"].endswith(
        f"/metadata/v{expected_metadata_version - 1}.metadata.json"
    )
    return snapshot


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
        current_snapshot = _assert_current_snapshot_metadata(
            table_path,
            metadata,
            expected_metadata_version=3,
            expected_snapshot_count=2,
        )
        assert current_snapshot["summary"]["operation"] == "overwrite"
        assert current_snapshot["summary"]["total-data-files"] == "2"
        assert current_snapshot["summary"]["total-delete-files"] == "1"
        assert current_snapshot["summary"]["total-records"] == "5"

        manifests = _current_manifest_list(metadata)["manifests"]
        expected_deleted_row_count = 2
        data_manifests = [manifest for manifest in manifests if manifest.get("content") == "data"]
        delete_manifests = [manifest for manifest in manifests if manifest.get("content") == "deletes"]
        assert any(manifest["sequence-number"] < current_snapshot["sequence-number"] for manifest in data_manifests)
        assert any(
            manifest["sequence-number"] == current_snapshot["sequence-number"]
            and (manifest.get("added-files-count") or 0) > 0
            for manifest in data_manifests
        )
        assert all(manifest["sequence-number"] == current_snapshot["sequence-number"] for manifest in delete_manifests)
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
        referenced_data_files = {entry.data_file.file_path for entry in data_entries}
        deleted_row_file_paths = [
            path
            for entry in delete_entries
            for path in pq.read_table(_local_file_path(entry.data_file.file_path), columns=["file_path"])
            .column("file_path")
            .to_pylist()
        ]
        assert deleted_row_file_paths
        assert all(entry.data_file.content == DataFileContent.POSITION_DELETES for entry in delete_entries)
        assert all(path in referenced_data_files for path in deleted_row_file_paths)
        assert all(entry.data_file.sort_order_id is None for entry in delete_entries)
        assert all(not entry.data_file.equality_ids for entry in delete_entries)
        assert all(getattr(entry.data_file, "content_offset", None) is None for entry in delete_entries)
        assert all(getattr(entry.data_file, "content_size_in_bytes", None) is None for entry in delete_entries)
        assert sum(entry.data_file.record_count for entry in delete_entries) >= expected_deleted_row_count
    finally:
        _drop_table(spark, table_name)


def test_iceberg_dataframe_merge_into_mor_update_delete_insert(spark, tmp_path):
    if not hasattr(SparkDataFrame, "mergeInto"):
        pytest.skip("DataFrame.mergeInto requires Spark 4.0+ (missing in this PySpark)")

    table_name = "iceberg_dataframe_merge_mor"
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
            """
            INSERT INTO iceberg_dataframe_merge_mor
            SELECT * FROM VALUES
              (1, 'old', 'keep'),
              (2, 'old', 'update'),
              (3, 'old', 'delete')
            """
        )

        source_df = spark.createDataFrame(
            [(2, "new", "insert"), (3, "ignored", "delete"), (4, "ins", "insert")],
            "src_id INT, src_value STRING, src_flag STRING",
        )
        (
            source_df.mergeInto(table_name, F.expr("id = src_id"))
            .whenMatched(F.expr("flag = 'update'"))
            .update(assignments={"value": F.col("src_value")})
            .whenMatched(F.expr("flag = 'delete'"))
            .delete()
            .whenNotMatched()
            .insert(
                assignments={
                    "id": F.col("src_id"),
                    "value": F.col("src_value"),
                    "flag": F.col("src_flag"),
                }
            )
            .merge()
        )

        rows = [
            tuple(row)
            for row in spark.sql("SELECT id, value, flag FROM iceberg_dataframe_merge_mor ORDER BY id").collect()
        ]
        assert rows == [
            (1, "old", "keep"),
            (2, "new", "update"),
            (4, "ins", "insert"),
        ]

        metadata = _find_latest_metadata(table_path)
        snapshot = _assert_current_snapshot_metadata(
            table_path,
            metadata,
            expected_metadata_version=3,
            expected_snapshot_count=2,
        )
        assert snapshot["summary"]["operation"] == "overwrite"
        assert snapshot["summary"]["added-position-deletes"] == "2"
        assert snapshot["summary"]["total-data-files"] == "2"
        assert snapshot["summary"]["total-delete-files"] == "1"
        assert snapshot["summary"]["total-records"] == "5"
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


@pytest.mark.parametrize("merge_mode", [None, "copy-on-write"], ids=["default", "explicit-cow"])
def test_iceberg_merge_rejects_copy_on_write_before_writing_empty_table(spark, tmp_path, merge_mode):
    table_name = "iceberg_merge_copy_on_write_reject"
    table_path = tmp_path / table_name
    mode_property = "" if merge_mode is None else f", 'write.merge.mode' = '{merge_mode}'"

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
            TBLPROPERTIES ('format-version' = '2'{mode_property})
            """
        )
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_copy_on_write_source AS
            SELECT * FROM VALUES (1, 'new') AS src(id, value)
            """
        )
        before_metadata_path = _latest_metadata_path(table_path)
        before_parquet_files = _parquet_file_paths(table_path)

        with pytest.raises(Exception, match=r"write\.merge\.mode=copy-on-write|copy-on-write.*not supported"):
            spark.sql(
                f"""
                MERGE INTO {table_name} AS t
                USING iceberg_merge_copy_on_write_source AS s
                ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT *
                """
            ).collect()

        assert spark.sql("SELECT * FROM iceberg_merge_copy_on_write_reject").collect() == []
        assert _latest_metadata_path(table_path) == before_metadata_path
        assert _parquet_file_paths(table_path) == before_parquet_files
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
        before_metadata_path = _latest_metadata_path(table_path)
        before_parquet_files = _parquet_file_paths(table_path)

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
        assert _latest_metadata_path(table_path) == before_metadata_path
        assert _parquet_file_paths(table_path) == before_parquet_files
        assert len(_find_latest_metadata(table_path)["snapshots"]) == 1
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_rejects_position_deletes_on_v3_table_without_metadata_commit(spark, tmp_path):
    table_name = "iceberg_merge_v3_position_delete"
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
              'format-version' = '3',
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO iceberg_merge_v3_position_delete VALUES (1, 'old')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_v3_source AS
            SELECT * FROM VALUES (1, 'new') AS src(id, value)
            """
        )
        before_metadata_path = _latest_metadata_path(table_path)
        before_parquet_files = _parquet_file_paths(table_path)

        with pytest.raises(Exception, match=r"deletion vectors|v3 MERGE MOR position delete"):
            spark.sql(
                f"""
                MERGE INTO {table_name} AS t
                USING iceberg_merge_v3_source AS s
                ON t.id = s.id
                WHEN MATCHED THEN
                  UPDATE SET value = s.value
                """
            ).collect()

        rows = [tuple(row) for row in spark.sql("SELECT id, value FROM iceberg_merge_v3_position_delete").collect()]
        assert rows == [(1, "old")]
        assert _latest_metadata_path(table_path) == before_metadata_path
        assert _parquet_file_paths(table_path) == before_parquet_files
        assert len(_find_latest_metadata(table_path)["snapshots"]) == 1
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_with_schema_evolution_is_rejected_without_side_effects(spark, tmp_path):
    table_name = "iceberg_merge_schema_evolution_reject"
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
        spark.sql("INSERT INTO iceberg_merge_schema_evolution_reject VALUES (1, 'old')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_schema_evolution_source AS
            SELECT 1 AS id, 'new' AS value, 'extra' AS extra
            """
        )
        before_metadata_path = _latest_metadata_path(table_path)
        before_parquet_files = _parquet_file_paths(table_path)

        with pytest.raises(Exception, match=r"Iceberg MERGE WITH SCHEMA EVOLUTION.*not supported"):
            spark.sql(
                f"""
                MERGE WITH SCHEMA EVOLUTION INTO {table_name} AS t
                USING iceberg_merge_schema_evolution_source AS s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            ).collect()

        assert spark.table(table_name).schema.names == ["id", "value"]
        rows = [
            tuple(row) for row in spark.sql("SELECT id, value FROM iceberg_merge_schema_evolution_reject").collect()
        ]
        assert rows == [(1, "old")]
        assert _latest_metadata_path(table_path) == before_metadata_path
        assert _parquet_file_paths(table_path) == before_parquet_files
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_updates_rows_beyond_the_first_input_batch(spark, tmp_path):
    table_name = "iceberg_merge_multi_batch"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
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
        spark.sql("INSERT INTO iceberg_merge_multi_batch SELECT id, 'old' FROM range(50000)")
        assert len(_parquet_file_paths(table_path)) == 1
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_multi_batch_source AS
            SELECT 20000L AS id, 'updated' AS value
            """
        )

        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING iceberg_merge_multi_batch_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET value = s.value
            """
        ).collect()

        rows = [
            tuple(row)
            for row in spark.sql("SELECT id, value FROM iceberg_merge_multi_batch WHERE id = 20000").collect()
        ]
        assert rows == [(20000, "updated")]
    finally:
        _drop_table(spark, table_name)


@pytest.mark.skip(
    reason="Known bug: MERGE uses stream-local positions after Parquet file splitting or row-group reordering"
)
def test_iceberg_merge_uses_absolute_positions_for_large_multi_row_group_file(spark, tmp_path):
    table_name = "iceberg_merge_split_file_position"
    table_path = tmp_path / table_name
    target_id = 1_100_000

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
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
        spark.sql(
            f"""
            INSERT INTO {table_name}
            SELECT id, sha2(CAST(id AS STRING), 256) AS value
            FROM range(1200000)
            """
        )

        data_files = sorted(table_path / path for path in _parquet_file_paths(table_path))
        assert len(data_files) == 1
        data_file = data_files[0]
        parquet_file = pq.ParquetFile(data_file)
        assert data_file.stat().st_size > 10 * 1024 * 1024
        assert parquet_file.metadata.num_row_groups >= 2  # noqa: PLR2004
        expected_position = None
        row_offset = 0
        for batch in parquet_file.iter_batches(columns=["id"]):
            ids = batch.column("id").to_pylist()
            if target_id in ids:
                expected_position = row_offset + ids.index(target_id)
                break
            row_offset += batch.num_rows
        assert expected_position is not None
        assert expected_position >= parquet_file.metadata.row_group(0).num_rows

        spark.sql(
            f"""
            CREATE OR REPLACE TEMP VIEW iceberg_merge_split_file_source AS
            SELECT {target_id}L AS id, 'updated' AS value
            """
        )
        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING iceberg_merge_split_file_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET value = s.value
            """
        ).collect()

        rows = [
            tuple(row)
            for row in spark.sql(f"SELECT id, value FROM {table_name} WHERE id = {target_id}").collect()
        ]
        assert rows == [(target_id, "updated")]

        delete_entries = _current_manifest_entries(table_path, ManifestContent.DELETES)
        positions = sorted(
            position
            for entry in delete_entries
            for position in pq.read_table(_local_file_path(entry.data_file.file_path), columns=["pos"])
            .column("pos")
            .to_pylist()
        )
        assert positions == [expected_position]
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_metadata_as_data_read_preserves_row_level_metadata(spark, tmp_path):
    table_name = "iceberg_merge_metadata_as_data"
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
            OPTIONS (metadataAsDataRead 'true')
            TBLPROPERTIES (
              'format-version' = '2',
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO iceberg_merge_metadata_as_data VALUES (1, 'old')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_metadata_as_data_source AS
            SELECT 1 AS id, 'updated' AS value
            """
        )

        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING iceberg_merge_metadata_as_data_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET value = s.value
            """
        ).collect()

        rows = [
            tuple(row) for row in spark.read.format("iceberg").load(table_path.as_uri()).select("id", "value").collect()
        ]
        assert rows == [(1, "updated")]
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_position_delete_round_trips_null_partition_value(spark, tmp_path):
    table_name = "iceberg_merge_null_partition"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              value STRING,
              part STRING
            )
            USING iceberg
            PARTITIONED BY (part)
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '2',
              'write.merge.mode' = 'merge-on-read'
            )
            """
        )
        spark.sql("INSERT INTO iceberg_merge_null_partition VALUES (1, 'old', NULL)")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_null_partition_source AS
            SELECT 1 AS id, 'updated' AS value
            """
        )

        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING iceberg_merge_null_partition_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET value = s.value
            """
        ).collect()

        rows = [tuple(row) for row in spark.sql("SELECT id, value, part FROM iceberg_merge_null_partition").collect()]
        assert rows == [(1, "updated", None)]
        delete_entries = _current_manifest_entries(table_path, ManifestContent.DELETES)
        assert len(delete_entries) == 1
        assert delete_entries[0].data_file.partition == Record(None)
    finally:
        _drop_table(spark, table_name)


def test_iceberg_merge_row_delta_snapshot_operation_tracks_written_files(spark, tmp_path):
    table_name = "iceberg_merge_row_delta_operation"
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
        spark.sql(
            """
            INSERT INTO iceberg_merge_row_delta_operation VALUES
              (1, 'keep'),
              (2, 'delete')
            """
        )
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_row_delta_source AS
            SELECT 3 AS id, 'insert' AS value
            """
        )

        spark.sql(
            """
            MERGE INTO iceberg_merge_row_delta_operation AS t
            USING iceberg_merge_row_delta_source AS s
            ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
            """
        ).collect()

        insert_snapshot = _current_snapshot(_find_latest_metadata(table_path))
        assert insert_snapshot["summary"]["operation"] == "append"

        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_row_delta_source AS
            SELECT 2 AS id, 'ignored' AS value
            """
        )
        spark.sql(
            """
            MERGE INTO iceberg_merge_row_delta_operation AS t
            USING iceberg_merge_row_delta_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN DELETE
            """
        ).collect()

        delete_snapshot = _current_snapshot(_find_latest_metadata(table_path))
        assert delete_snapshot["summary"]["operation"] == "delete"
        rows = [
            tuple(row)
            for row in spark.sql("SELECT id, value FROM iceberg_merge_row_delta_operation ORDER BY id").collect()
        ]
        assert rows == [(1, "keep"), (3, "insert")]
    finally:
        _drop_table(spark, table_name)


def test_iceberg_noop_merge_reuses_parent_manifests_with_overwrite_snapshot(spark, tmp_path):
    table_name = "iceberg_merge_noop_snapshot"
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
        spark.sql("INSERT INTO iceberg_merge_noop_snapshot VALUES (1, 'keep')")
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW iceberg_merge_noop_source AS
            SELECT 2 AS id, 'unmatched' AS value
            """
        )

        before_metadata = _find_latest_metadata(table_path)
        before_manifest_paths = {manifest["manifest-path"] for manifest in _current_manifests(table_path)}
        before_manifest_files = set((table_path / "metadata").glob("manifest-*.avro"))

        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING iceberg_merge_noop_source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET value = s.value
            """
        ).collect()

        after_metadata = _find_latest_metadata(table_path)
        after_snapshot = _current_snapshot(after_metadata)
        after_manifest_paths = {manifest["manifest-path"] for manifest in _current_manifests(table_path)}
        after_manifest_files = set((table_path / "metadata").glob("manifest-*.avro"))
        assert len(after_metadata["snapshots"]) == len(before_metadata["snapshots"]) + 1
        assert after_snapshot["summary"]["operation"] == "overwrite"
        assert after_manifest_paths == before_manifest_paths
        assert after_manifest_files == before_manifest_files
        rows = [tuple(row) for row in spark.sql("SELECT id, value FROM iceberg_merge_noop_snapshot").collect()]
        assert rows == [(1, "keep")]
    finally:
        _drop_table(spark, table_name)
