import time
import uuid
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    read_manifest_list,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

from pysail.testing.spark.steps.iceberg import (
    _current_manifest_list,
    _current_snapshot,
    _find_latest_metadata,
    _latest_metadata_path,
    _metadata_file_version,
    _pyarrow_input_file,
    _write_metadata_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.utils import create_sql_catalog


def _uri_sql(path: Path) -> str:
    return escape_sql_string_literal(path.as_uri())


def _drop_table(spark, name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")


class _EqualityDeleteManifestWriter(ManifestWriterV2):
    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        meta = dict(super()._meta)
        meta["content"] = "deletes"
        return meta


def _local_table_path(location: str) -> Path:
    parsed = urlparse(location)
    return Path(url2pathname(f"//{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path))


def _new_snapshot_id() -> int:
    return uuid.uuid4().int & ((1 << 63) - 1)


def _append_equality_delete_snapshot(
    table,
    delete_rows: pa.Table,
    equality_ids: list[int],
    *,
    partition: Record | None = None,
) -> Path:
    table_path = _local_table_path(table.location())
    metadata_dir = table_path / "metadata"
    data_dir = table_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    delete_file_path = data_dir / f"equality-delete-{uuid.uuid4()}.parquet"
    pq.write_table(delete_rows, delete_file_path)

    metadata = _find_latest_metadata(table_path)
    parent_snapshot = _current_snapshot(metadata)
    snapshot_id = _new_snapshot_id()
    sequence_number = metadata.get("last-sequence-number", 0) + 1

    io = PyArrowFileIO()
    delete_data_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=delete_file_path.as_uri(),
        file_format=FileFormat.PARQUET,
        partition=partition or Record(),
        record_count=delete_rows.num_rows,
        file_size_in_bytes=delete_file_path.stat().st_size,
        equality_ids=equality_ids,
    )

    manifest_path = metadata_dir / f"manifest-{uuid.uuid4()}.avro"
    with _EqualityDeleteManifestWriter(
        table.spec(),
        table.schema(),
        io.new_output(manifest_path.as_uri()),
        snapshot_id,
        "null",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                data_file=delete_data_file,
            )
        )
        delete_manifest = writer.to_manifest_file()

    existing_manifests = list(read_manifest_list(_pyarrow_input_file(io, parent_snapshot["manifest-list"])))
    manifest_list_path = metadata_dir / f"snap-{snapshot_id}-0-{uuid.uuid4()}.avro"
    with write_manifest_list(
        2,
        io.new_output(manifest_list_path.as_uri()),
        snapshot_id,
        parent_snapshot["snapshot-id"],
        sequence_number,
        "null",
    ) as writer:
        writer.add_manifests([*existing_manifests, delete_manifest])

    latest_metadata_path = _latest_metadata_path(table_path)
    latest_metadata_version = _metadata_file_version(latest_metadata_path)
    assert latest_metadata_version is not None

    previous_updated_ms = metadata["last-updated-ms"]
    updated_ms = int(time.time() * 1000)
    metadata["current-snapshot-id"] = snapshot_id
    metadata["last-sequence-number"] = sequence_number
    metadata["last-updated-ms"] = updated_ms
    metadata.setdefault("snapshots", []).append(
        {
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": parent_snapshot["snapshot-id"],
            "sequence-number": sequence_number,
            "timestamp-ms": updated_ms,
            "manifest-list": manifest_list_path.as_uri(),
            "summary": {
                "operation": "delete",
                "added-delete-files": "1",
                "added-equality-delete-files": "1",
                "added-equality-deletes": str(delete_rows.num_rows),
            },
            "schema-id": metadata.get("current-schema-id", 0),
        }
    )
    metadata.setdefault("snapshot-log", []).append({"snapshot-id": snapshot_id, "timestamp-ms": updated_ms})
    metadata.setdefault("metadata-log", []).append(
        {"metadata-file": latest_metadata_path.as_uri(), "timestamp-ms": previous_updated_ms}
    )
    metadata.setdefault("refs", {})["main"] = {"snapshot-id": snapshot_id, "type": "branch"}

    new_metadata_path = metadata_dir / f"{latest_metadata_version + 1:05d}-{uuid.uuid4()}.metadata.json"
    _write_metadata_file(new_metadata_path, metadata)
    return table_path


def _current_delete_entries(table_path: Path) -> list[ManifestEntry]:
    metadata = _find_latest_metadata(table_path)
    snapshot = _current_snapshot(metadata)
    io = PyArrowFileIO()
    entries = []
    for manifest in read_manifest_list(_pyarrow_input_file(io, snapshot["manifest-list"])):
        if manifest.content == ManifestContent.DELETES:
            entries.extend(manifest.fetch_manifest_entry(io))
    return entries


def _delete_manifest_count(table_path: Path, key: str) -> int:
    manifests = _current_manifest_list(_find_latest_metadata(table_path))["manifests"]
    return sum(manifest.get(key) or 0 for manifest in manifests if manifest.get("content") == "deletes")


def test_iceberg_sql_delete_writes_equality_delete_file_and_filters_rows(spark, tmp_path):
    table_name = "iceberg_sql_equality_delete"
    table_path = tmp_path / table_name

    _drop_table(spark, table_name)
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id BIGINT,
              name STRING,
              flag STRING
            )
            USING iceberg
            LOCATION '{_uri_sql(table_path)}'
            TBLPROPERTIES (
              'format-version' = '2'
            )
            """
        )
        spark.sql(
            f"""
            INSERT INTO {table_name}
            SELECT * FROM VALUES
              (1, 'keep-1', 'keep'),
              (2, 'drop-2', 'drop'),
              (3, 'keep-3', 'keep')
            """
        )

        spark.sql(f"DELETE FROM {table_name} WHERE flag = 'drop'").collect()

        rows = [
            tuple(row)
            for row in spark.sql(f"SELECT id, name, flag FROM {table_name} ORDER BY id").collect()
        ]
        assert rows == [(1, "keep-1", "keep"), (3, "keep-3", "keep")]

        metadata = _find_latest_metadata(table_path)
        snapshot = _current_snapshot(metadata)
        summary = snapshot["summary"]
        assert summary["operation"] == "delete"
        assert summary["added-delete-files"] == "1"
        assert summary["added-equality-delete-files"] == "1"
        assert summary["added-equality-deletes"] == "1"
        assert summary["deleted-records"] == "1"
        assert "added-position-delete-files" not in summary

        assert _delete_manifest_count(table_path, "added-files-count") == 1
        assert _delete_manifest_count(table_path, "added-rows-count") == 1

        entries = _current_delete_entries(table_path)
        assert len(entries) == 1
        delete_file = entries[0].data_file
        assert delete_file.content == DataFileContent.EQUALITY_DELETES
        assert delete_file.equality_ids == [1, 2, 3]
        assert getattr(delete_file, "referenced_data_file", None) is None

        delete_rows = pq.read_table(_local_table_path(delete_file.file_path)).to_pylist()
        assert delete_rows == [{"id": 2, "name": "drop-2", "flag": "drop"}]
    finally:
        _drop_table(spark, table_name)


def test_iceberg_unpartitioned_equality_delete_filters_matching_rows_and_records_manifest(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.equality_delete_global"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        ),
        properties={"format-version": "2"},
    )
    try:
        table.append(pa.table({"id": [1, 2, 2, 3], "name": ["keep-1", "drop-a", "drop-b", "keep-3"]}))
        table_path = _append_equality_delete_snapshot(table, pa.table({"id": [2]}), [1])

        rows = [
            tuple(row)
            for row in spark.read.format("iceberg").load(table.location()).select("id", "name").orderBy("id").collect()
        ]
        assert rows == [(1, "keep-1"), (3, "keep-3")]

        assert _delete_manifest_count(table_path, "added-files-count") == 1
        assert _delete_manifest_count(table_path, "added-rows-count") == 1

        entries = _current_delete_entries(table_path)
        assert len(entries) == 1
        assert entries[0].data_file.content == DataFileContent.EQUALITY_DELETES
        assert entries[0].data_file.equality_ids == [1]
        assert entries[0].sequence_number == _current_snapshot(_find_latest_metadata(table_path))["sequence-number"]
    finally:
        catalog.drop_table(identifier)


def test_iceberg_equality_delete_matches_multiple_columns_with_nulls(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.equality_delete_nulls"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "category", StringType(), required=False),
            NestedField(3, "marker", StringType(), required=False),
        ),
        properties={"format-version": "2"},
    )
    try:
        table.append(
            pa.table(
                {
                    "id": [1, 2, 3, 4, 5],
                    "category": ["keep", "drop", "drop", None, None],
                    "marker": ["x", None, "x", "x", None],
                }
            )
        )
        delete_rows = pa.table(
            {
                "category": pa.array(["drop", None], type=pa.string()),
                "marker": pa.array([None, None], type=pa.string()),
            }
        )
        _append_equality_delete_snapshot(table, delete_rows, [2, 3])

        rows = [
            tuple(row)
            for row in (
                spark.read.format("iceberg")
                .load(table.location())
                .select("id", "category", "marker")
                .orderBy("id")
                .collect()
            )
        ]
        assert rows == [
            (1, "keep", "x"),
            (3, "drop", "x"),
            (4, None, "x"),
        ]
    finally:
        catalog.drop_table(identifier)


def test_iceberg_partitioned_equality_delete_only_applies_within_delete_partition(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.equality_delete_partitioned"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "payload", StringType(), required=False),
            NestedField(3, "part", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "part")),
        properties={"format-version": "2"},
    )
    try:
        table.append(
            pa.table(
                {
                    "id": [2, 2, 3],
                    "payload": ["drop-a", "keep-b", "keep-a"],
                    "part": ["A", "B", "A"],
                }
            )
        )
        table_path = _append_equality_delete_snapshot(table, pa.table({"id": [2]}), [1], partition=Record("A"))

        rows = [
            tuple(row)
            for row in (
                spark.read.format("iceberg")
                .load(table.location())
                .select("id", "payload", "part")
                .orderBy("part", "id", "payload")
                .collect()
            )
        ]
        assert rows == [(3, "keep-a", "A"), (2, "keep-b", "B")]

        entries = _current_delete_entries(table_path)
        assert len(entries) == 1
        assert entries[0].data_file.partition == Record("A")
    finally:
        catalog.drop_table(identifier)
