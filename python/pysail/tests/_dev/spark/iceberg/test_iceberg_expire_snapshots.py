# ruff: noqa: S608
# Semantic cases: Apache Iceberg 1.12 TestRemoveSnapshots and TestExpireSnapshotsProcedure.
# NULL IDs and missing-file counts confirmed with Spark 4.1.4 + Iceberg 1.12.0.

import copy
from urllib.parse import urlparse

import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    ManifestEntry,
    ManifestEntryStatus,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.table import StaticTable
from pyspark.errors import PySparkException
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
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
DEFAULT_FORMAT_VERSION = 2


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


def _install_escaped_data_aliases(metadata_location, file_io, spelling):
    """Give two snapshots one reader-visible file, using URI and non-URI spellings."""
    table = StaticTable.from_metadata(metadata_location, properties=file_io.properties)
    snapshots = table.metadata.snapshots
    assert len(snapshots) == 2  # noqa: PLR2004
    current = table.current_snapshot()
    entries = [entry for manifest in current.manifests(file_io) for entry in manifest.fetch_manifest_entry(file_io)]
    assert len(entries) == 1
    shared = f"{table.location().rstrip('/')}/data/shared%253A.parquet"
    with file_io.new_input(entries[0].data_file.file_path).open() as source:
        content = source.read()
    with file_io.new_output(shared).create() as output:
        output.write(content)
    plain = urlparse(shared).path.replace("%253A", "%3A")
    retained = plain if spelling == "absolute" else "data/shared%3A.parquet"
    for snapshot in snapshots:
        data_file = copy.deepcopy(entries[0].data_file)
        data_file[1] = retained if snapshot.snapshot_id == current.snapshot_id else shared
        manifest_path = f"{table.location().rstrip('/')}/metadata/alias-{snapshot.snapshot_id}.avro"
        with write_manifest(
            2, table.spec(), table.schema(), file_io.new_output(manifest_path), snapshot.snapshot_id, "null"
        ) as writer:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=snapshot.snapshot_id,
                    sequence_number=snapshot.sequence_number,
                    file_sequence_number=snapshot.sequence_number,
                    data_file=data_file,
                )
            )
        manifest = writer.to_manifest_file()
        with write_manifest_list(
            2,
            file_io.new_output(snapshot.manifest_list),
            snapshot.snapshot_id,
            snapshot.parent_snapshot_id,
            snapshot.sequence_number,
            "null",
        ) as writer:
            writer.add_manifests([manifest])
    return snapshots[0].snapshot_id, shared, content


@pytest.mark.parametrize("spelling", ["absolute", "relative"])
def test_expiration_rejects_escaped_data_alias_before_publication(spark, table, spelling):
    from pysail.testing._dev.spark.steps.iceberg import _latest_metadata_path

    name, path = table
    for value in [1, 2]:
        spark.sql(f"INSERT OVERWRITE {name} VALUES ({value}), ({value + 10})")
    expired, shared, content = _install_escaped_data_aliases(
        _latest_metadata_path(path).as_uri(), PyArrowFileIO(), spelling
    )
    # Read an expression over the actual data, not a metadata-only count or bound.
    assert sorted(row.value for row in spark.table(name).selectExpr("id + 100 AS value").collect()) == [102, 112]
    before = {p: p.read_bytes() for p in path.rglob("*") if p.is_file()}
    metadata = _find_latest_metadata(path)
    try:
        with pytest.raises(PySparkException, match="ambiguous non-URI path"):
            expire(spark, name, f", snapshot_ids => array(CAST({expired} AS BIGINT))")
    finally:
        # Run even when the refusal assertion fails, to expose destructive regressions.
        with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
            reopened = sessions.create().read.format("iceberg").load(str(path))
            assert sorted(row.value for row in reopened.selectExpr("id + 100 AS value").collect()) == [102, 112]
        with PyArrowFileIO().new_input(shared).open() as source:
            assert source.read() == content
    assert _find_latest_metadata(path) == metadata
    assert {p: p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
    with PyArrowFileIO().new_input(shared).open() as source:
        assert source.read() == content


def test_expiration_preserves_encoded_partition_uris(spark, tmp_path):
    name = "expire_encoded_partition"
    path = tmp_path / name
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, part STRING) USING iceberg PARTITIONED BY (part) "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        for value in [1, 2]:
            spark.sql(f"INSERT OVERWRITE {name} VALUES ({value}, 'a/b'), ({value + 10}, 'a/b')")
        uris = [row.file_path for row in spark.table(f"{name}.files").collect()]
        assert uris and all("%" in uri and uri.startswith("file:") for uri in uris)
        assert expire(spark, name, f", older_than => {FUTURE}")[0] == 1
        with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
            rows = sessions.create().read.format("iceberg").load(str(path)).orderBy("id").collect()
            assert [tuple(row) for row in rows] == [(2, "a/b"), (12, "a/b")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_pointer_authoritative_memory_registration_rejects_expiration(spark, table):
    name, path = table
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2)")
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('table_type'='ICEBERG')")
    before = {p: p.read_bytes() for p in path.rglob("*") if p.is_file()}
    properties = spark.sql(f"SHOW TBLPROPERTIES {name}").collect()
    with pytest.raises(PySparkException, match="pointer-authoritative registrations"):
        expire(spark, name, f", older_than => {FUTURE}")
    assert {p: p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
    assert spark.sql(f"SHOW TBLPROPERTIES {name}").collect() == properties
    assert [row.id for row in spark.table(name).collect()] == [2]


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("missing", [False, True])
def test_expiration_overwrite_inventory_and_reopen(spark, table, version, missing):
    name, path = table
    if version != DEFAULT_FORMAT_VERSION:
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
    with pytest.raises(PySparkException):
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
    assert shared.exists()
    assert not expired.exists()
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
    with pytest.raises(Exception, match=r"object store|inline|unencrypted"):
        expire(spark, name, f", older_than => {FUTURE}")
    assert {str(p): p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
