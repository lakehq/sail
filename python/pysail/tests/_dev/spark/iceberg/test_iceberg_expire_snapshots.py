# ruff: noqa: S608
# Semantic cases: Apache Iceberg 1.12 TestRemoveSnapshots and TestExpireSnapshotsProcedure.
# NULL IDs and missing-file counts confirmed with Spark 4.1.4 + Iceberg 1.12.0.

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_merge import _local_file_path
from pysail.tests._dev.spark.iceberg.test_iceberg_procedures import _edit_latest_metadata
from pysail.tests._dev.spark.iceberg.test_iceberg_rewrite_data_files import _data_entries, _delete_files, _rewrite

FIELDS = [
    "deleted_data_files_count",
    "deleted_position_delete_files_count",
    "deleted_equality_delete_files_count",
    "deleted_manifest_files_count",
    "deleted_manifest_lists_count",
    "deleted_statistics_files_count",
]
ZERO = (0,) * len(FIELDS)
FUTURE = "TIMESTAMP '2999-01-01 00:00:00'"


def expire(spark, name, args=""):
    frame = spark.sql(f"CALL system.expire_snapshots(table => '{name}'{args})")
    assert frame.schema == StructType([StructField(name, LongType(), True) for name in FIELDS])
    return tuple(frame.first())


@pytest.fixture
def table(spark, tmp_path):
    name = "expire_snapshots_table"
    path = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("missing", [False, True])
def test_expiration_overwrite_inventory_and_reopen(spark, table, version, missing):
    name, path = table
    if version != 2:
        # New empty table may be upgraded, but cannot be downgraded from v2.
        spark.sql(f"DROP TABLE {name}")
        path = path.parent / f"v{version}"
        spark.sql(
            f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}')"
        )
    for value in [1, 2, 3]:
        spark.sql(f"INSERT OVERWRITE {name} VALUES ({value})")
    before = _find_latest_metadata(path)
    metadata_json = {p: p.read_bytes() for p in (path / "metadata").glob("*.json")}
    current_files = {_local_file_path(row.file_path) for row in spark.table(f"{name}.files").collect()}
    all_files = set(path.rglob("*.parquet"))
    obsolete = all_files - current_files
    assert obsolete
    if missing:
        next(iter(obsolete)).unlink()
    counts = expire(
        spark, name, f", older_than => {FUTURE}, retain_last => 1, max_concurrent_deletes => 2, stream_results => true"
    )
    assert counts[0] == len(obsolete)
    assert counts[1:3] == (0, 0)
    assert counts[4] == len(before["snapshots"]) - 1
    assert counts[5] == 0
    assert all(not file.exists() for file in obsolete)
    assert all(file.exists() for file in current_files)
    after = _find_latest_metadata(path)
    assert after["snapshots"] == [before["snapshots"][-1]]
    assert after["current-snapshot-id"] == before["current-snapshot-id"]
    assert after.get("last-sequence-number") == before.get("last-sequence-number")
    assert after.get("next-row-id") == before.get("next-row-id")
    assert all(p.read_bytes() == payload for p, payload in metadata_json.items())
    assert expire(spark, name, f", older_than => {FUTURE}") == ZERO
    assert [row.id for row in spark.read.format("iceberg").load(str(path)).collect()] == [3]
    spark.sql(f"INSERT INTO {name} VALUES (4)")
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == [3, 4]


def test_defaults_null_arrays_explicit_ids_and_head_atomicity(spark, table):
    name, path = table
    assert expire(spark, name) == ZERO
    for value in [1, 2, 3]:
        spark.sql(f"INSERT INTO {name} VALUES ({value})")
    before = _find_latest_metadata(path)
    ids = [snapshot["snapshot-id"] for snapshot in before["snapshots"]]
    for args in [
        "",
        ", older_than => NULL, retain_last => NULL, max_concurrent_deletes => NULL, stream_results => NULL, snapshot_ids => NULL, clean_expired_metadata => NULL",
        ", snapshot_ids => array(NULL, NULL)",
        ", snapshot_ids => array()",
        ", snapshot_ids => array(9223372036854775806L)",
    ]:
        assert expire(spark, name, args) == ZERO
        assert _find_latest_metadata(path) == before
    with pytest.raises(Exception, match="Still referenced"):
        expire(spark, name, f", snapshot_ids => array({ids[0]}L, {ids[-1]}L)")
    assert _find_latest_metadata(path) == before
    counts = expire(spark, name, f", retain_last => 100, snapshot_ids => array({ids[0]}L, NULL, {ids[0]}L)")
    assert counts[0] == 0  # Appends share all prior data.
    assert counts[4] == 1
    assert [s["snapshot-id"] for s in _find_latest_metadata(path)["snapshots"]] == ids[1:]
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2, 3]


@pytest.mark.parametrize(
    "args",
    [
        ", retain_last => 0",
        ", max_concurrent_deletes => 0",
        ", max_concurrent_deletes => 257",
        ", unknown => 1",
        ", stream_results => 1",
        ", snapshot_ids => 1",
    ],
)
def test_invalid_arguments_preserve_state(spark, table, args):
    name, path = table
    before = _find_latest_metadata(path)
    with pytest.raises(Exception):
        expire(spark, name, args)
    assert _find_latest_metadata(path) == before


def test_gc_disabled_rejects_empty_noop(spark, table):
    name, path = table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('gc.enabled'='false')")
    before = _find_latest_metadata(path)
    with pytest.raises(Exception, match="GC is disabled"):
        expire(spark, name)
    assert _find_latest_metadata(path) == before


def test_refs_retention_and_history_suffix(spark, table):
    name, path = table
    for value in [1, 2, 3]:
        spark.sql(f"INSERT INTO {name} VALUES ({value})")
    ids = [s["snapshot-id"] for s in _find_latest_metadata(path)["snapshots"]]
    spark.sql(f"ALTER TABLE {name} CREATE TAG audit AS OF VERSION {ids[0]}")
    spark.sql(f"ALTER TABLE {name} CREATE BRANCH review AS OF VERSION {ids[1]}")
    with pytest.raises(Exception, match="Still referenced"):
        expire(spark, name, f", snapshot_ids => array({ids[0]}L)")
    assert expire(spark, name, f", older_than => {FUTURE}") == ZERO
    spark.sql(f"ALTER TABLE {name} DROP TAG audit")
    counts = expire(spark, name, f", older_than => {FUTURE}")
    assert counts[4] == 1
    metadata = _find_latest_metadata(path)
    assert [s["snapshot-id"] for s in metadata["snapshots"]] == ids[1:]
    assert all(entry["snapshot-id"] != ids[0] for entry in metadata["snapshot-log"])
    spark.sql(f"ALTER TABLE {name} DROP BRANCH review")
    expire(spark, name, f", older_than => {FUTURE}")
    assert [s["snapshot-id"] for s in _find_latest_metadata(path)["snapshots"]] == ids[-1:]


@pytest.mark.parametrize("clean", [False, True])
def test_schema_cleanup_and_statistics_paths(spark, table, clean):
    name, path = table
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    old_id = _find_latest_metadata(path)["current-snapshot-id"]
    spark.sql(f"ALTER TABLE {name} ADD COLUMN value STRING")
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2, 'new')")
    shared = path / "metadata" / "shared.puffin"
    expired = path / "metadata" / "expired.puffin"
    shared.write_bytes(b"statistics")
    expired.write_bytes(b"statistics")

    def add_stats(metadata):
        def stats(snapshot, file):
            return {
                "snapshot-id": snapshot,
                "statistics-path": str(file),
                "file-size-in-bytes": 10,
                "file-footer-size-in-bytes": 0,
                "blob-metadata": [],
            }

        metadata["statistics"] = [stats(old_id, shared), stats(metadata["current-snapshot-id"], shared)]
        metadata["partition-statistics"] = [
            {"snapshot-id": old_id, "statistics-path": str(expired), "file-size-in-bytes": 10}
        ]

    _edit_latest_metadata(path, add_stats)
    before = _find_latest_metadata(path)
    counts = expire(spark, name, f", older_than => {FUTURE}, clean_expired_metadata => {str(clean).lower()}")
    assert counts[5] == 1
    assert shared.exists() and not expired.exists()
    after = _find_latest_metadata(path)
    assert len(after["schemas"]) == (1 if clean else len(before["schemas"]))
    assert len(after["statistics"]) == 1
    assert not after.get("partition-statistics")
    assert [tuple(row) for row in spark.table(name).collect()] == [(2, "new")]


@pytest.mark.parametrize("version", [2, 3])
def test_delete_paths_remain_reachable_from_retained_snapshot(spark, table, version):
    name, path = table
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read')"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1), (2), (3)")
    spark.sql(f"DELETE FROM {name} WHERE id = 1").collect()
    snapshot_id = _find_latest_metadata(path)["current-snapshot-id"]
    deletes = {_local_file_path(file["file_path"]) for file in _delete_files(path)}
    assert deletes
    spark.sql(f"ALTER TABLE {name} CREATE TAG audit AS OF VERSION {snapshot_id}")
    spark.sql(f"INSERT OVERWRITE {name} VALUES (4)")
    assert expire(spark, name, f", older_than => {FUTURE}")[1] == 0
    assert all(file.exists() for file in deletes)
    assert [
        row.id
        for row in spark.read.format("iceberg")
        .option("snapshot-id", snapshot_id)
        .load(str(path))
        .orderBy("id")
        .collect()
    ] == [2, 3]
    spark.sql(f"ALTER TABLE {name} DROP TAG audit")
    assert expire(spark, name, f", older_than => {FUTURE}")[1] == len(deletes)
    assert all(not file.exists() for file in deletes)
    assert [row.id for row in spark.table(name).collect()] == [4]


def test_expiration_preserves_shared_dv_container_until_last_blob_is_unreachable(spark, table):
    name, path = table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read')")
    starts, total = [], 0
    for size in range(1, 10):
        starts.append(total)
        end = total + size * 100
        spark.sql(f"INSERT INTO {name} SELECT /*+ COALESCE(1) */ id FROM range({total}, {end})")
        total = end
    spark.sql(f"DELETE FROM {name} WHERE id IN ({','.join(map(str, starts))})").collect()
    files = _data_entries(path)
    deletes = _delete_files(path)
    sizes = {}
    for file in deletes:
        sizes.setdefault(file["file_path"], set()).add(files[file["referenced_data_file"]].data_file.file_size_in_bytes)
    cutoff = max(next(group for group in sizes.values() if len(group) > 1))
    selected = {name for name, entry in files.items() if entry.data_file.file_size_in_bytes < cutoff}
    selected_paths = {file["file_path"] for file in deletes if file["referenced_data_file"] in selected}
    retained_paths = {file["file_path"] for file in deletes if file["referenced_data_file"] not in selected}
    assert selected_paths & retained_paths
    payloads = {file: _local_file_path(file).read_bytes() for file in retained_paths}
    _rewrite(
        spark,
        name,
        {
            "min-file-size-bytes": str(cutoff),
            "target-file-size-bytes": str(cutoff + 1),
            "max-file-size-bytes": str(cutoff * 10),
            "min-input-files": "1",
        },
    )
    assert expire(spark, name, f", older_than => {FUTURE}")[1] == len(selected_paths - retained_paths)
    assert all(_local_file_path(file).read_bytes() == payload for file, payload in payloads.items())
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == [i for i in range(total) if i not in starts]
    _rewrite(spark, name)
    assert expire(spark, name, f", older_than => {FUTURE}")[1] == len(retained_paths)
    assert all(not _local_file_path(file).exists() for file in retained_paths)
    assert spark.table(name).count() == total - len(starts)


@pytest.mark.parametrize("invalid", ["foreign", "inline", "encrypted"])
def test_preflight_rejections_preserve_metadata_and_files(spark, table, invalid):
    name, path = table
    for value in [1, 2]:
        spark.sql(f"INSERT OVERWRITE {name} VALUES ({value})")

    def corrupt(metadata):
        if invalid == "foreign":
            metadata["snapshots"][0]["manifest-list"] = "s3://wrong-bucket/metadata/list.avro"
        elif invalid == "inline":
            metadata["snapshots"][0]["manifests"] = []
        else:
            metadata["snapshots"][0]["key-id"] = "encrypted"

    _edit_latest_metadata(path, corrupt)
    before = {str(p): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    with pytest.raises(Exception, match="object store|inline|unencrypted"):
        expire(spark, name, f", older_than => {FUTURE}")
    assert {str(p): p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
