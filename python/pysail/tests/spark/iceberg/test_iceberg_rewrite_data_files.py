# ruff: noqa: S608

# Semantic coverage: Apache Iceberg 1.11.0 TestRewriteDataFilesAction
# testBinPackWithV2PositionDeletes, testBinPackWithDVs, testBinPackWithDeleteAllData,
# removeDanglingDVsFromDeleteManifest and testRewriteDataFilesPreservesLineage.
from itertools import pairwise

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.avro.file import AvroFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    MANIFEST_ENTRY_SCHEMAS,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntryStatus,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType, StructType

from pysail.testing.spark.steps.iceberg import (
    _current_deletion_vectors,
    _current_manifest_list,
    _current_row_lineage,
    _current_snapshot,
    _find_latest_metadata,
    _pyarrow_input_file,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot
from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path

FORMAT_VERSION_WITH_ROW_LINEAGE = 3
FRAGMENTED_DELETE_STRIDE = 2


def _rewrite(spark, name, options=None):
    options = options or {"rewrite-all": "true", "target-file-size-bytes": "1048576"}
    values = ", ".join(f"'{key}', '{value}'" for key, value in options.items())
    return spark.sql(f"CALL system.rewrite_data_files('{name}', options => map({values}))").first()


def _data_entries(path):
    return {entry.data_file.file_path: entry for entry in _current_manifest_entries(path, ManifestContent.DATA)}


def _delete_files(path):
    # PyIceberg's DataFile wrapper does not expose V3 blob offsets or explicit target paths.
    metadata = _find_latest_metadata(path)
    schema = MANIFEST_ENTRY_SCHEMAS[metadata["format-version"]]
    fields = schema.find_field("data_file").field_type.fields
    io = PyArrowFileIO()
    files = []
    for manifest in _current_manifest_list(metadata)["manifests"]:
        if manifest["content"] != "deletes":
            continue
        with AvroFile(_pyarrow_input_file(io, manifest["manifest-path"]), schema) as entries:
            files.extend(
                {field.name: entry[4][index] for index, field in enumerate(fields)}
                for entry in entries
                if entry[0] != ManifestEntryStatus.DELETED
            )
    return files


def _removed_delete_count(delete_files, selected_files):
    return sum(
        file["content"] == DataFileContent.POSITION_DELETES and file.get("referenced_data_file") in selected_files
        for file in delete_files
    )


def _blob_identity(file):
    return (file["file_path"], file["content_offset"], file["content_size_in_bytes"], file["referenced_data_file"])


def _assert_rewrite_commit(path, before, old_files, result, removed_deletes):
    after = _find_latest_metadata(path)
    snapshot = _current_snapshot(after)
    assert after["snapshots"][:-1] == before["snapshots"]
    assert snapshot["parent-snapshot-id"] == before["current-snapshot-id"]
    assert snapshot["sequence-number"] == before["last-sequence-number"] + 1
    assert snapshot["summary"]["operation"] == "replace"
    new_files = _data_entries(path)
    added = new_files.keys() - old_files.keys()
    removed = old_files.keys() - new_files.keys()
    assert result.rewritten_data_files_count == len(removed)
    assert result.added_data_files_count == len(added)
    assert result.rewritten_bytes_count == sum(old_files[name].data_file.file_size_in_bytes for name in removed)
    assert result.failed_data_files_count == 0
    assert result.removed_delete_files_count == removed_deletes
    assert int(snapshot["summary"].get("removed-delete-files", 0)) == removed_deletes
    for name in added:
        # V3 uses the new sequence for null lineage from upgraded files; V2 pins the starting sequence.
        expected_sequence = (
            snapshot["sequence-number"]
            if after["format-version"] == FORMAT_VERSION_WITH_ROW_LINEAGE
            else _current_snapshot(before)["sequence-number"]
        )
        assert new_files[name].sequence_number == expected_sequence
        assert new_files[name].file_sequence_number == snapshot["sequence-number"]
    for name in old_files.keys() & new_files.keys():
        assert new_files[name] == old_files[name]
    assert all(_local_file_path(name).exists() for name in old_files)
    assert after["schemas"] == before["schemas"]
    assert after["last-column-id"] == before["last-column-id"]
    return after


def test_rewrite_preserves_imported_name_mapping(spark, sql_catalog, tmp_path):
    name = "rewrite_imported_names"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "payload", StructType(NestedField(3, "old", LongType(), required=False)), required=False),
        ),
    )
    imported = tmp_path / "imported.parquet"
    pq.write_table(
        pa.table({"id": [1, 2], "payload": pa.array([{"old": 10}, None], pa.struct([("old", pa.int64())]))}),
        imported,
    )
    table.add_files([imported.as_uri()])
    with table.update_schema() as update:
        update.rename_column("id", "key")
        update.rename_column(("payload", "old"), "value")
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
    try:
        expected = [{"key": 1, "payload": {"value": 10}}, {"key": 2, "payload": None}]
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("key").collect()] == expected

        result = spark.sql(f"CALL system.rewrite_data_files('{name}', options => map('rewrite-all', 'true'))").first()

        assert result.rewritten_data_files_count == 1
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("key").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


def test_rewrite_preserves_omitted_identity_partition_values(spark, sql_catalog, tmp_path):
    name = "rewrite_identity_values"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "p", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
    )
    imported = tmp_path / "partitioned.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array([1, 2])],
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"}),
                ]
            ),
        ),
        imported,
    )
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=imported.as_uri(),
        file_format=FileFormat.PARQUET,
        partition=Record("x"),
        record_count=2,
        file_size_in_bytes=imported.stat().st_size,
        spec_id=table.spec().spec_id,
    )
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        append.append_data_file(data_file)
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{table.location()}'")
    try:
        expected = [(1, "x"), (2, "x")]
        assert [tuple(row) for row in spark.table(name).select("id", "p").orderBy("id").collect()] == expected

        result = spark.sql(f"CALL system.rewrite_data_files('{name}', options => map('rewrite-all', 'true'))").first()

        assert result.rewritten_data_files_count == 1
        assert [tuple(row) for row in spark.table(name).select("id", "p").orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


@pytest.mark.parametrize("files_per_group", [1, 2])
def test_rewrite_thresholds_apply_after_group_size_limit(spark, tmp_path, files_per_group):
    name = "rewrite_group_thresholds"
    location = (tmp_path / name).as_uri()
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{location}'")
    try:
        for value in range(6):
            spark.sql(f"INSERT INTO {name} VALUES ({value})")
        files = spark.table(f"{name}.files").select("file_path", "file_size_in_bytes").orderBy("file_path").collect()
        snapshots = spark.table(f"{name}.snapshots").orderBy("snapshot_id").collect()
        group_size = max(row.file_size_in_bytes for row in files) * files_per_group
        statement = (
            f"CALL system.rewrite_data_files('{name}', options => map("
            f"'max-file-group-size-bytes', '{group_size}', "
            "'target-file-size-bytes', '1048576', 'min-input-files', '2'))"
        )
        result = spark.sql(statement).first()
        if files_per_group == 1:
            assert tuple(result) == (0, 0, 0, 0, 0)
            assert tuple(spark.sql(statement).first()) == (0, 0, 0, 0, 0)
            assert spark.table(f"{name}.snapshots").orderBy("snapshot_id").collect() == snapshots
            assert (
                spark.table(f"{name}.files").select("file_path", "file_size_in_bytes").orderBy("file_path").collect()
                == files
            )
        else:
            assert result.rewritten_data_files_count == len(files)
            assert result.added_data_files_count == len(files) // files_per_group
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(6))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("version", [2, 3], ids=["position-deletes", "deletion-vectors"])
@pytest.mark.parametrize(
    "delete_stride", [FRAGMENTED_DELETE_STRIDE, 1000], ids=["fragmented-fallback", "row-selection"]
)
def test_rewrite_mor_preserves_rows_lineage_history_and_subsequent_dml(spark, tmp_path, version, delete_stride):
    selection_run_limit = 4096
    merge_id = 8192
    subsequent_update_id = 8193
    name = "rewrite_mor_lifecycle"
    path = tmp_path / name
    location = escape_sql_string_literal(str(path))
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, value BIGINT) USING iceberg LOCATION '{location}'
        TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read',
                       'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read',
                       'write.parquet.row-group-size-bytes'='16384')""")
    try:
        spark.sql(f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ id AS id, id AS value FROM range(18000)")
        original = _find_latest_metadata(path)
        parquets = [pq.ParquetFile(_local_file_path(file)) for file in _data_entries(path)]
        assert parquets
        assert all(parquet.metadata.num_row_groups > 1 for parquet in parquets)
        # Include the old positions removed by UPDATE/MERGE when checking the selection-run bound.
        if delete_stride == FRAGMENTED_DELETE_STRIDE:
            run_counts = []
            for parquet in parquets:
                deleted = [
                    row["id"] % delete_stride == 0 or row["id"] in (8191, merge_id, 16383)
                    for row in parquet.read(columns=["id"]).to_pylist()
                ]
                run_counts.append(1 + sum(left != right for left, right in pairwise(deleted)))
            assert max(run_counts) > selection_run_limit
        spark.sql(f"UPDATE {name} SET value=-1 WHERE id IN (8191, 16383)").collect()
        spark.sql(f"""MERGE INTO {name} t USING (SELECT {merge_id}L AS id) s ON t.id=s.id
            WHEN MATCHED THEN UPDATE SET value=-2""").collect()
        spark.sql(f"DELETE FROM {name} WHERE id % {delete_stride} = 0").collect()
        expected = [
            (i, -1 if i in (8191, 16383) else -2 if i == merge_id else i) for i in range(18000) if i % delete_stride
        ]
        before = _find_latest_metadata(path)
        schema = spark.table(name).schema
        lineage = _current_row_lineage(path) if version == FORMAT_VERSION_WITH_ROW_LINEAGE else None
        delete_paths = {file["file_path"] for file in _delete_files(path)}
        for _ in range(2):
            old_files = _data_entries(path)
            # Generic multi-file position deletes are retained; explicit target references are pruned.
            removed_deletes = _removed_delete_count(_delete_files(path), old_files)
            result = _rewrite(spark, name)
            before = _assert_rewrite_commit(path, before, old_files, result, removed_deletes)
            assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
            assert spark.table(name).schema == schema
            assert all(_local_file_path(name).exists() for name in delete_paths)
            if version == FORMAT_VERSION_WITH_ROW_LINEAGE:
                assert _current_row_lineage(path) == lineage
                assert _current_deletion_vectors(path) == {}
                for entry in _data_entries(path).values():
                    parquet = pq.ParquetFile(_local_file_path(entry.data_file.file_path))
                    fields = parquet.schema_arrow
                    assert fields.field("_row_id").metadata[b"PARQUET:field_id"] == b"2147483540"
                    assert fields.field("_last_updated_sequence_number").metadata[b"PARQUET:field_id"] == b"2147483539"
                    physical = parquet.read(columns=["id", "_row_id", "_last_updated_sequence_number"]).to_pylist()
                    assert all(
                        (row["_row_id"], row["_last_updated_sequence_number"]) == lineage[row["id"]] for row in physical
                    )
        historical = spark.read.format("iceberg").option("snapshot-id", original["current-snapshot-id"]).load(str(path))
        assert [tuple(row) for row in historical.orderBy("id").collect()] == [(i, i) for i in range(18000)]
        spark.sql(f"UPDATE {name} SET value=99 WHERE id={subsequent_update_id}").collect()
        expected = [(i, 99 if i == subsequent_update_id else value) for i, value in expected]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        if lineage is not None:
            updated = _current_row_lineage(path)
            assert updated[subsequent_update_id] == (
                lineage[subsequent_update_id][0],
                _find_latest_metadata(path)["last-sequence-number"],
            )
            assert {i: pair for i, pair in updated.items() if i != subsequent_update_id} == {
                i: pair for i, pair in lineage.items() if i != subsequent_update_id
            }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(("scope", "evolution"), [("shared", "none"), ("global", "reuse"), ("partition", "rename")])
def test_rewrite_applies_equality_deletes_by_id_and_scope(spark, sql_catalog, scope, evolution):
    name = f"rewrite_equality_{scope}"
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(
            NestedField(1, "id", LongType()), NestedField(2, "key", LongType()), NestedField(3, "part", StringType())
        ),
    )
    original_spec = table.spec()
    try:
        if scope != "shared":
            with table.update_spec() as update:
                update.add_identity("part")
        table.append(pa.table({"id": [1, 2], "key": [7, 8], "part": ["A", "A"]}))
        table.append(pa.table({"id": [3, 4], "key": pa.array([7, None], pa.int64()), "part": ["B", "B"]}))
        if evolution == "rename":
            with table.update_schema() as update:
                update.rename_column("key", "renamed_key")
        elif evolution == "reuse":
            with table.update_schema() as update:
                update.delete_column("key")
            with table.update_schema() as update:
                update.add_column("key", LongType())
        path = _append_equality_delete_snapshot(
            table,
            pa.table({"key": pa.array([7, None], pa.int64())}),
            [2],
            partition=Record("A") if scope == "partition" else None,
            partition_spec=original_spec if scope == "global" else None,
        )
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
        expected_ids = [2, 3, 4] if scope == "partition" else [2]
        before = _find_latest_metadata(path)
        old_files = _data_entries(path)
        old_deletes = _current_manifest_entries(path, ManifestContent.DELETES)
        schema = spark.table(name).schema
        result = _rewrite(spark, name)
        _assert_rewrite_commit(path, before, old_files, result, 0)
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == expected_ids
        assert spark.table(name).schema == schema
        assert _current_manifest_entries(path, ManifestContent.DELETES) == old_deletes
        key = "renamed_key" if evolution == "rename" else "key"
        expected_keys = [8, 7, None] if scope == "partition" else [None] if evolution == "reuse" else [8]
        assert [row[0] for row in spark.table(name).orderBy("id").select(key).collect()] == expected_keys
        for entry in _data_entries(path).values():
            assert set(pq.ParquetFile(_local_file_path(entry.data_file.file_path)).schema_arrow.names) == set(
                schema.names
            )
        # Retained equality deletes must not remove newly inserted rows with the same key.
        spark.sql(f"INSERT INTO {name} (id, {key}, part) VALUES (5, 7, 'A')")
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [*expected_ids, 5]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


@pytest.mark.parametrize("version", [2, 3])
def test_rewrite_all_deleted_file_commits_removal_and_then_is_noop(spark, tmp_path, version):
    name = "rewrite_empty_survivors"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'
        TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read')""")
    try:
        spark.sql(f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ id * 2 FROM range(10)")
        original = _find_latest_metadata(path)
        spark.sql(f"DELETE FROM {name} WHERE id % 2 = 0").collect()
        old_files = _data_entries(path)
        assert old_files
        before = _find_latest_metadata(path)
        removed = _removed_delete_count(_delete_files(path), old_files)
        assert removed == (1 if version == FORMAT_VERSION_WITH_ROW_LINEAGE else 0)
        result = _rewrite(spark, name)
        after = _assert_rewrite_commit(path, before, old_files, result, removed)
        assert result.added_data_files_count == 0
        assert _data_entries(path) == {}
        assert spark.table(name).collect() == []
        assert tuple(_rewrite(spark, name)) == (0, 0, 0, 0, 0)
        assert _find_latest_metadata(path) == after
        historical = spark.read.format("iceberg").option("snapshot-id", original["current-snapshot-id"]).load(str(path))
        assert [row.id for row in historical.orderBy("id").collect()] == list(range(0, 20, 2))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_rewrite_selected_files_preserves_shared_puffin_siblings(spark, tmp_path):
    name = "rewrite_shared_puffin"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'
        TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')""")
    try:
        starts = []
        total = 0
        # More input files than writer partitions guarantees a shared Puffin object.
        for size in range(1, 10):
            starts.append(total)
            end = total + size * 100
            spark.sql(f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ id FROM range({total}, {end})")
            total = end
        spark.sql(f"DELETE FROM {name} WHERE id IN ({','.join(map(str, starts))})").collect()
        before = _find_latest_metadata(path)
        old_files = _data_entries(path)
        deletes = _delete_files(path)
        blobs = [_blob_identity(file) for file in deletes]
        sizes_by_puffin = {}
        for file, _, _, target in blobs:
            sizes_by_puffin.setdefault(file, set()).add(old_files[target].data_file.file_size_in_bytes)
        shared_sizes = next(sizes for sizes in sizes_by_puffin.values() if len(sizes) > 1)
        cutoff = max(shared_sizes)
        selected = {name for name, entry in old_files.items() if entry.data_file.file_size_in_bytes < cutoff}
        assert selected
        assert selected < old_files.keys()
        selected_puffins = {file for file, _, _, target in blobs if target in selected}
        untouched_puffins = {file for file, _, _, target in blobs if target not in selected}
        assert selected_puffins & untouched_puffins
        payloads = {file: _local_file_path(file).read_bytes() for file, _, _, _ in blobs}
        lineage = _current_row_lineage(path)
        result = _rewrite(
            spark,
            name,
            {
                "min-file-size-bytes": str(cutoff),
                "target-file-size-bytes": str(cutoff + 1),
                "max-file-size-bytes": str(cutoff * 10),
                "min-input-files": "1",
            },
        )
        _assert_rewrite_commit(path, before, old_files, result, sum(target in selected for _, _, _, target in blobs))
        assert old_files.keys() - _data_entries(path).keys() == selected
        retained = _delete_files(path)
        assert {_blob_identity(file) for file in retained} == {blob for blob in blobs if blob[3] not in selected}
        assert all(_local_file_path(file).read_bytes() == payload for file, payload in payloads.items())
        assert _current_row_lineage(path) == lineage
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [
            i for i in range(total) if i not in starts
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("with_deletes", [False, True])
def test_rewrite_after_v3_upgrade_assigns_lineage_with_new_commit_sequence(spark, tmp_path, with_deletes):
    deleted_id = 3
    updated_id = 2
    name = "rewrite_upgrade"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, value BIGINT) USING iceberg
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES (
          'format-version'='2', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')""")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20)")
        spark.sql(f"INSERT INTO {name} VALUES (3, 30), (4, 40)")
        legacy = _find_latest_metadata(path)
        if with_deletes:
            spark.sql(f"DELETE FROM {name} WHERE id % {deleted_id} = 0").collect()
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        before = _find_latest_metadata(path)
        assert before["next-row-id"] == 0
        expected = [(i, i * 10) for i in range(1, 5) if not with_deletes or i != deleted_id]
        old_files = _data_entries(path)
        removed = _removed_delete_count(_delete_files(path), old_files)
        result = _rewrite(spark, name)
        deletes = _delete_files(path)
        # V2 position files without explicit target references remain live but cannot match new paths.
        after = _assert_rewrite_commit(path, before, old_files, result, removed)
        assert all(file["file_format"] == "PARQUET" for file in deletes)
        lineage = _current_row_lineage(path)
        assert set(lineage) == {i for i, _ in expected}
        assert {row_id for row_id, _ in lineage.values()} == set(range(len(expected)))
        assert {sequence for _, sequence in lineage.values()} == {after["last-sequence-number"]}
        for entry in _data_entries(path).values():
            physical = pq.ParquetFile(_local_file_path(entry.data_file.file_path)).read().to_pylist()
            assert all(row["_row_id"] is None and row["_last_updated_sequence_number"] is None for row in physical)
        # A second rewrite materializes the inherited values; it must not assign them again.
        _rewrite(spark, name)
        assert _current_row_lineage(path) == lineage
        spark.sql(f"UPDATE {name} SET value=99 WHERE id={updated_id}").collect()
        current = _current_row_lineage(path)
        assert current[updated_id] == (lineage[updated_id][0], _find_latest_metadata(path)["last-sequence-number"])
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [
            (i, 99 if i == updated_id else value) for i, value in expected
        ]
        history = spark.read.format("iceberg").option("snapshot-id", legacy["current-snapshot-id"]).load(str(path))
        assert [tuple(row) for row in history.orderBy("id").collect()] == [(i, i * 10) for i in range(1, 5)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("nested", [False, True], ids=["scalar-key", "dropped-nested-key"])
def test_partial_rewrite_respects_shared_global_equality_delete_sequences(spark, sql_catalog, nested):
    older_batch_count = 2
    name = "rewrite_partial_equality"
    key_field = (
        NestedField(2, "payload", StructType(NestedField(3, "key", LongType()), NestedField(4, "kept", LongType())))
        if nested
        else NestedField(2, "key", LongType())
    )
    table = sql_catalog.create_table(
        f"default.{name}",
        Schema(NestedField(1, "id", LongType()), key_field, NestedField(5, "part", StringType())),
    )
    path = _local_file_path(table.location())
    global_spec = table.spec()
    equality_id = table.schema().find_field("payload.key" if nested else "key").field_id
    payload_id = table.schema().find_field("payload").field_id if nested else None
    payload_type = pa.struct([("key", pa.int64()), ("kept", pa.int64())])
    arrow_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("payload", payload_type) if nested else ("key", pa.int64()),
            ("part", pa.string()),
        ]
    )
    try:
        with table.update_spec() as update:
            update.add_identity("part")
        batches = [
            ([1, 2, 3], [7, 8, None], "A"),
            (list(range(1000, 1060)), [7 if i % 3 == 0 else None if i % 3 == 1 else 8 for i in range(60)], "B"),
            ([101, 102], [7, None], "A"),
            ([201, 202], [7, None], "A"),
        ]
        expected = []
        sequences = []
        for batch_index, (ids, keys, part) in enumerate(batches):
            rows = []
            for row_id, key in zip(ids, keys, strict=True):
                row = {"id": row_id, "part": part}
                row.update(
                    {"payload": None if key is None else {"key": key, "kept": row_id * 10}} if nested else {"key": key}
                )
                rows.append(row)
                # The delete applies only to the two older files, including the untouched partition.
                if batch_index >= older_batch_count or key not in (7, None):
                    expected.append(
                        {**row, "payload": None if key is None else {"kept": row_id * 10}} if nested else row
                    )
            table.append(pa.Table.from_pylist(rows, schema=arrow_schema))
            sequences.append(_current_snapshot(_find_latest_metadata(path))["sequence-number"])
        if nested:
            with table.update_schema() as update:
                update.delete_column(("payload", "key"))
            delete_type = pa.struct(
                [pa.field("key", pa.int64(), metadata={b"PARQUET:field_id": str(equality_id).encode()})]
            )
            delete_rows = pa.Table.from_arrays(
                [pa.array([{"key": 7}, None], type=delete_type)],
                schema=pa.schema(
                    [pa.field("payload", delete_type, metadata={b"PARQUET:field_id": str(payload_id).encode()})]
                ),
            )
        else:
            delete_rows = pa.table({"key": pa.array([7, None], pa.int64())})
        _append_equality_delete_snapshot(
            table,
            delete_rows,
            [equality_id],
            partition_spec=global_spec,
            data_sequence_number=sequences[older_batch_count],
            assign_root_ids=not nested,
        )
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
        old_files = _data_entries(path)
        assert sorted(entry.sequence_number for entry in old_files.values()) == sequences
        assert sequences[0] < sequences[1] < sequences[2] < sequences[3]
        untouched = {file for file, entry in old_files.items() if entry.data_file.partition[0] == "B"}
        assert len(untouched) == 1
        selected = old_files.keys() - untouched
        cutoff = min(old_files[file].data_file.file_size_in_bytes for file in untouched)
        assert all(old_files[file].data_file.file_size_in_bytes < cutoff for file in selected)
        old_deletes = _current_manifest_entries(path, ManifestContent.DELETES)
        assert len(old_deletes) == 1
        assert old_deletes[0].sequence_number == sequences[older_batch_count]
        before = _find_latest_metadata(path)
        schema = spark.table(name).schema
        expected.sort(key=lambda row: row["id"])
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("id").collect()] == expected
        result = _rewrite(
            spark,
            name,
            {
                "min-file-size-bytes": str(cutoff),
                "target-file-size-bytes": str(cutoff + 1),
                "max-file-size-bytes": str(cutoff * 10),
                "min-input-files": "2",
            },
        )
        after = _assert_rewrite_commit(path, before, old_files, result, 0)
        assert old_files.keys() - _data_entries(path).keys() == selected
        assert _current_manifest_entries(path, ManifestContent.DELETES) == old_deletes
        assert spark.table(name).schema == schema
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("id").collect()] == expected
        if nested:
            for file in _data_entries(path).keys() - untouched:
                physical_schema = pq.ParquetFile(_local_file_path(file)).schema_arrow
                assert physical_schema.field("payload").type.get_field_index("key") == -1
        # A subsequent rewrite applies the retained global delete to the formerly untouched old file,
        # without deleting matching keys from equal/newer-sequence data already compacted.
        files = _data_entries(path)
        result = _rewrite(spark, name)
        _assert_rewrite_commit(path, after, files, result, 0)
        assert _current_manifest_entries(path, ManifestContent.DELETES) == old_deletes
        assert [row.asDict(recursive=True) for row in spark.table(name).orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(f"default.{name}")


def test_first_v3_partial_rewrite_assigns_untouched_legacy_lineage_then_preserves_it(spark, tmp_path):
    selected_file_count = 2
    selected_row_count = 6
    name = "rewrite_partial_upgrade"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, value BIGINT, part STRING) USING iceberg PARTITIONED BY (part)
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES (
          'format-version'='2', 'write.update.mode'='merge-on-read')""")
    try:
        for start, end, part in [(0, 3, "A"), (3, selected_row_count, "A"), (selected_row_count, 106, "B")]:
            spark.sql(
                f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ id AS id, id AS value, '{part}' AS part FROM range({start}, {end})"
            )
        legacy = _find_latest_metadata(path)
        old_files = _data_entries(path)
        selected = {file for file, entry in old_files.items() if entry.data_file.partition[0] == "A"}
        untouched = old_files.keys() - selected
        assert len(selected) == selected_file_count
        assert len(untouched) == 1
        untouched_sequence = old_files[next(iter(untouched))].sequence_number
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        before = _find_latest_metadata(path)
        assert before["current-snapshot-id"] == legacy["current-snapshot-id"]
        assert before["next-row-id"] == 0
        result = _rewrite(spark, name, {"min-input-files": "2", "target-file-size-bytes": "1048576"})
        after = _assert_rewrite_commit(path, before, old_files, result, 0)
        assert old_files.keys() - _data_entries(path).keys() == selected
        lineage = _current_row_lineage(path)
        assert set(lineage) == set(range(106))
        assert {row_id for row_id, _ in lineage.values()} == set(range(106))
        assert {sequence for row_id, (_, sequence) in lineage.items() if row_id < selected_row_count} == {
            after["last-sequence-number"]
        }
        assert {sequence for row_id, (_, sequence) in lineage.items() if row_id >= selected_row_count} == {
            untouched_sequence
        }
        for file in untouched:
            assert "_row_id" not in pq.ParquetFile(_local_file_path(file)).schema_arrow.names
        expected = [(i, i, "A" if i < selected_row_count else "B") for i in range(106)]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
        files = _data_entries(path)
        result = _rewrite(spark, name)
        _assert_rewrite_commit(path, after, files, result, 0)
        assert _current_row_lineage(path) == lineage
        spark.sql(f"UPDATE {name} SET value=value+1000 WHERE id IN (2, 8)").collect()
        updated = _current_row_lineage(path)
        sequence = _find_latest_metadata(path)["last-sequence-number"]
        assert updated == {
            row_id: (pair[0], sequence) if row_id in (2, 8) else pair for row_id, pair in lineage.items()
        }
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [
            (i, value + 1000 if i in (2, 8) else value, part) for i, value, part in expected
        ]
        history = spark.read.format("iceberg").option("snapshot-id", legacy["current-snapshot-id"]).load(str(path))
        assert [tuple(row) for row in history.orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
