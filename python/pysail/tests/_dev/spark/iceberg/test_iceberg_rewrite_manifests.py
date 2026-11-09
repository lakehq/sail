# ruff: noqa: S608

import hashlib

import pyarrow as pa
import pytest
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FixedType, LongType, NestedField
from pyspark.sql.types import IntegerType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import (
    _current_manifest_list,
    _current_row_lineage,
    _current_snapshot,
    _find_latest_metadata,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path
from pysail.tests._dev.spark.iceberg.test_iceberg_rewrite_data_files import _delete_files

ROW_LINEAGE_VERSION = 3


def _manifests(path):
    return _current_manifest_list(_find_latest_metadata(path))["manifests"]


def _file_state(path):
    fields = (
        "content",
        "file_path",
        "file_format",
        "partition",
        "record_count",
        "file_size_in_bytes",
        "column_sizes",
        "value_counts",
        "null_value_counts",
        "nan_value_counts",
        "lower_bounds",
        "upper_bounds",
        "key_metadata",
        "split_offsets",
        "equality_ids",
        "sort_order_id",
    )
    return {
        entry.data_file.file_path: (
            entry.snapshot_id,
            entry.sequence_number,
            entry.file_sequence_number,
            tuple(getattr(entry.data_file, field) for field in fields),
        )
        for content in (ManifestContent.DATA, ManifestContent.DELETES)
        for entry in _current_manifest_entries(path, content)
    }


def _payloads(paths):
    return {str(path): hashlib.sha256(_local_file_path(str(path)).read_bytes()).hexdigest() for path in paths}


def _rewrite(spark, name, arguments=""):
    frame = spark.sql(f"CALL system.rewrite_manifests(table => '{name}'{arguments})")
    assert frame.schema == StructType(
        [
            StructField("rewritten_manifests_count", IntegerType(), False),
            StructField("added_manifests_count", IntegerType(), False),
        ]
    )
    return tuple(frame.first())


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("partitioned", [False, True])
def test_manifest_compaction_preserves_files_history_and_noops(spark, tmp_path, version, partitioned):
    name = "manifest_compaction"
    path = tmp_path / name
    partition = "PARTITIONED BY (p)" if partitioned else ""
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p STRING) USING iceberg {partition}
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}')""")
    try:
        empty = _find_latest_metadata(path)
        assert _rewrite(spark, name) == (0, 0)
        assert _find_latest_metadata(path) == empty
        spark.sql(f"INSERT INTO {name} VALUES (1, 'z'), (2, 'a')")
        single = _find_latest_metadata(path)
        assert len(_manifests(path)) == 1
        assert _rewrite(spark, name) == (0, 0)
        assert _find_latest_metadata(path) == single
        spark.sql(f"INSERT INTO {name} VALUES (3, 'a'), (4, 'z')")
        before = _find_latest_metadata(path)
        files = _file_state(path)
        payloads = _payloads(files)
        old_manifests = _payloads(m["manifest-path"] for m in _manifests(path))
        lineage = _current_row_lineage(path) if version == ROW_LINEAGE_VERSION else None
        expected = spark.table(name).orderBy("id").collect()

        assert _rewrite(spark, name, f", use_caching => {str(partitioned).lower()}") == (2, 1)

        after = _find_latest_metadata(path)
        snapshot = _current_snapshot(after)
        assert after["snapshots"][:-1] == before["snapshots"]
        assert snapshot["parent-snapshot-id"] == before["current-snapshot-id"]
        assert snapshot["summary"]["operation"] == "replace"
        assert snapshot.get("sequence-number", 0) == (before["last-sequence-number"] + 1 if version > 1 else 0)
        assert _file_state(path) == files
        assert _payloads(files) == payloads
        assert _payloads(old_manifests) == old_manifests
        assert len(_manifests(path)) == 1
        assert all(
            entry.status == ManifestEntryStatus.EXISTING
            for entry in _current_manifest_entries(path, ManifestContent.DATA)
        )
        assert spark.table(name).orderBy("id").collect() == expected
        historical = spark.read.format("iceberg").option("snapshot-id", before["current-snapshot-id"]).load(str(path))
        assert historical.orderBy("id").collect() == expected
        if lineage is not None:
            assert _current_row_lineage(path) == lineage
            assert after["next-row-id"] == before["next-row-id"]
            assert snapshot["added-rows"] == 0
        assert _rewrite(spark, name) == (0, 0)
        assert _find_latest_metadata(path) == after
        spark.sql(f"INSERT INTO {name} VALUES (5, 'b')")
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2, 3, 4, 5]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_fixed_identity_preserves_metrics_and_data(spark, sql_catalog):
    name = "manifest_fixed_identity"
    identifier = f"default.{name}"
    table = sql_catalog.create_table(
        identifier,
        Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "p", FixedType(3), required=False)),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
    )
    path = _local_file_path(table.location())
    try:
        for identifier_value, value in [(1, b"abc"), (2, b"xyz")]:
            table.append(
                pa.table({"id": pa.array([identifier_value], pa.int64()), "p": pa.array([value], pa.binary(3))})
            )
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
        before = spark.table(f"{name}.files").orderBy("file_path").collect()
        payloads = _payloads(row.file_path for row in before)
        expected_manifest_count = 2
        assert len(_manifests(path)) == expected_manifest_count
        # First write two manifests in Iceberg's length-prefixed fixed encoding, then
        # compact those manifests. The second rewrite must use the normalized reader.
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('commit.manifest.target-size-bytes'='1')")
        assert _rewrite(spark, name) == (2, 2)
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('commit.manifest.target-size-bytes'='8388608')")
        assert _rewrite(spark, name) == (2, 1)
        assert len(_manifests(path)) == 1
        assert spark.table(f"{name}.files").orderBy("file_path").collect() == before
        assert _payloads(payloads) == payloads
        assert [(row.id, bytes(row.p)) for row in spark.table(name).orderBy("id").collect()] == [
            (1, b"abc"),
            (2, b"xyz"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


def test_manifest_rewrite_selects_current_or_explicit_spec(spark, tmp_path):
    name = "manifest_specs"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p STRING) USING iceberg
        LOCATION '{escape_sql_string_literal(str(path))}'""")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")
        spark.sql(f"INSERT INTO {name} VALUES (2, 'b')")
        old = _manifests(path)
        spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD p")
        spark.sql(f"INSERT INTO {name} VALUES (3, 'a')")
        before = _find_latest_metadata(path)
        assert _rewrite(spark, name) == (0, 0)
        assert _find_latest_metadata(path) == before
        retained = [m for m in _manifests(path) if m["manifest-path"] not in {m["manifest-path"] for m in old}]
        files = _file_state(path)
        assert _rewrite(spark, name, ", spec_id => 0") == (2, 1)
        after = _manifests(path)
        expected_manifest_count = 2
        assert len(after) == expected_manifest_count
        assert all(m in after for m in retained)
        assert _file_state(path) == files
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2, 3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_rolls_large_manifests(spark, tmp_path):
    name = "manifest_rolling"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)
        LOCATION '{escape_sql_string_literal(str(path))}'""")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 4), (2, 1), (3, 3), (4, 2)")
        files = _file_state(path)
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('commit.manifest.target-size-bytes'='1')")
        assert _rewrite(spark, name) == (1, 4)
        assert len(_manifests(path)) == len(files)
        assert _file_state(path) == files
        # Reset the property and compact again: splitting must not change the underlying files.
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('commit.manifest.target-size-bytes'='8388608')")
        assert _rewrite(spark, name) == (4, 1)
        assert _file_state(path) == files
        entries = _current_manifest_entries(path, ManifestContent.DATA)
        assert [entry.data_file.partition[0] for entry in entries] == [1, 2, 3, 4]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("version", "operation"),
    [(2, "delete"), (2, "update"), (3, "delete")],
    ids=["equality-deletes", "position-deletes", "deletion-vectors"],
)
def test_manifest_rewrite_preserves_delete_files_and_visibility(spark, tmp_path, version, operation):
    name = "manifest_deletes"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, value BIGINT) USING iceberg
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES (
        'format-version'='{version}', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read')""")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20)")
        spark.sql(f"INSERT INTO {name} VALUES (3, 30), (4, 40)")
        for identifier in (1, 3):
            statement = (
                f"DELETE FROM {name} WHERE id={identifier}"
                if operation == "delete"
                else (f"UPDATE {name} SET value=value+1 WHERE id={identifier}")
            )
            spark.sql(statement).collect()
        expected = spark.table(name).orderBy("id").collect()
        before = _find_latest_metadata(path)
        manifests = _manifests(path)
        files = _file_state(path)
        deletes = _delete_files(path)
        minimum_delete_file_count = 2
        assert len(deletes) >= minimum_delete_file_count
        payloads = _payloads(files)
        lineage = _current_row_lineage(path) if version == ROW_LINEAGE_VERSION else None
        rewritten = sum(
            len(group)
            for content in ("data", "deletes")
            if len(group := [m for m in manifests if m["content"] == content]) > 1
        )
        assert _rewrite(spark, name) == (rewritten, 2)
        assert _file_state(path) == files
        assert sorted(_delete_files(path), key=lambda file: file["file_path"]) == sorted(
            deletes, key=lambda file: file["file_path"]
        )
        assert _payloads(files) == payloads
        assert spark.table(name).orderBy("id").collect() == expected
        if lineage is not None:
            assert _current_row_lineage(path) == lineage
        historical = spark.read.format("iceberg").option("snapshot-id", before["current-snapshot-id"]).load(str(path))
        assert historical.orderBy("id").collect() == expected
        spark.sql(f"INSERT INTO {name} VALUES (1, 99)")
        assert spark.table(name).where("id = 1 AND value = 99").count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    "arguments",
    [
        "",
        "NULL",
        "''",
        "'manifest_args', use_caching => 1",
        "'manifest_args', use_caching => 0L",
        "'manifest_args', use_caching => 1.5",
        "'manifest_args', use_caching => CAST(1 AS DOUBLE)",
        "'manifest_args', use_caching => 1 + 1",
        "'manifest_args', use_caching => CAST(NULL AS INT)",
        "'manifest_args', spec_id => -1",
        "'manifest_args', sort_by => array('id')",
        "'manifest_args', unknown => true",
        "'manifest_args', table => 'manifest_args'",
    ],
)
def test_manifest_rewrite_rejects_invalid_arguments_without_changes(spark, tmp_path, arguments):
    name = "manifest_args"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"INSERT INTO {name} VALUES (2)")
        before = _find_latest_metadata(path)
        artifacts = _payloads((path / "metadata").iterdir())
        with pytest.raises(Exception, match=r"(?i)(argument|parameter|spec|identifier|table)"):
            spark.sql(f"CALL system.rewrite_manifests({arguments})").collect()
        assert _find_latest_metadata(path) == before
        assert _payloads((path / "metadata").iterdir()) == artifacts
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    "argument",
    [
        "true",
        "false",
        "NULL",
        "CAST(NULL AS BOOLEAN)",
        "CAST(1 AS BOOLEAN)",
        "1 = 1",
        "'true'",
    ],
)
def test_manifest_rewrite_accepts_boolean_arguments(spark, tmp_path, argument):
    name = "manifest_boolean_args"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        before = _find_latest_metadata(path)
        # Explicit casts, Boolean expressions, and untyped NULL are still valid.
        # The numeric spec_id parameter must keep its existing conversion behavior.
        assert _rewrite(spark, name, f", use_caching => {argument}, spec_id => 0") == (0, 0)
        assert _find_latest_metadata(path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_failed_read_does_not_publish_or_stage_files(spark, tmp_path):
    name = "manifest_failed_read"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"INSERT INTO {name} VALUES (2)")
        manifest = _local_file_path(_manifests(path)[-1]["manifest-path"])
        original = manifest.read_bytes()
        manifest.write_bytes(b"not an avro manifest")
        before = _find_latest_metadata(path)
        artifacts = _payloads((path / "metadata").iterdir())
        try:
            with pytest.raises(Exception, match=r"(?i)(avro|manifest|header|magic)"):
                _rewrite(spark, name)
            assert _find_latest_metadata(path) == before
            assert _payloads((path / "metadata").iterdir()) == artifacts
        finally:
            manifest.write_bytes(original)
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_preserves_historical_schema_metrics(spark, tmp_path):
    name = "manifest_schema_history"
    path = tmp_path / name
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, old STRING) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")
        spark.sql(f"INSERT INTO {name} VALUES (2, 'b')")
        old_paths = {manifest["manifest-path"] for manifest in _manifests(path)}
        spark.sql(f"ALTER TABLE {name} DROP COLUMN old")
        spark.sql(f"INSERT INTO {name} VALUES (3)")
        retained = [manifest for manifest in _manifests(path) if manifest["manifest-path"] not in old_paths]
        retained_payloads = _payloads(manifest["manifest-path"] for manifest in retained)
        before = _file_state(path)
        data_payloads = _payloads(before)
        old_field_id = 2
        assert any(
            old_field_id in (entry.data_file.lower_bounds or {})
            for entry in _current_manifest_entries(path, ManifestContent.DATA)
        )
        assert _rewrite(spark, name) == (2, 1)
        assert all(manifest in _manifests(path) for manifest in retained)
        assert _payloads(retained_payloads) == retained_payloads
        assert _file_state(path) == before
        assert _payloads(before) == data_payloads
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2, 3]
        metadata = _find_latest_metadata(path)
        snapshot = _current_snapshot(metadata)
        assert snapshot["summary"]["entries-processed"] == "2"
        manifests = _manifests(path)
        artifacts = _payloads((path / "metadata").iterdir())
        assert _rewrite(spark, name) == (0, 0)
        assert _find_latest_metadata(path) == metadata
        assert _current_snapshot(_find_latest_metadata(path)) == snapshot
        assert _manifests(path) == manifests
        assert _payloads((path / "metadata").iterdir()) == artifacts
        assert _file_state(path) == before
        assert _payloads(before) == data_payloads
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2, 3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_rejects_zero_target_without_changes(spark, tmp_path):
    name = "manifest_zero_target"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"INSERT INTO {name} VALUES (2)")
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('commit.manifest.target-size-bytes'='0')")
        before = _find_latest_metadata(path)
        with pytest.raises(Exception, match="must be positive"):
            _rewrite(spark, name)
        assert _find_latest_metadata(path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_manifest_rewrite_defers_unassigned_v3_lineage(spark, tmp_path):
    name = "manifest_upgrade"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"INSERT INTO {name} VALUES (2)")
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        before = _find_latest_metadata(path)
        with pytest.raises(Exception, match="requires assigned row lineage"):
            _rewrite(spark, name)
        assert _find_latest_metadata(path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
