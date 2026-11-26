# SPDX-License-Identifier: Apache-2.0

from contextlib import contextmanager

import pyarrow as pa
import pytest
from pyspark.errors import PySparkException
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_procedures import _edit_latest_metadata
from pysail.tests._dev.spark.iceberg.test_iceberg_rewrite_data_files import _data_entries

# Apache Iceberg 1.12.0 TestCherrypickSnapshotProcedure, TestPublishChangesProcedure,
# TestSnapshotManager, TestWapWorkflow, TestSequenceNumberForV2Table, TestRowLineageAssignment and TestRemoveSnapshots.


@pytest.fixture
def target(spark, tmp_path, request):
    table = "pick_snapshot"
    path = tmp_path / table
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{getattr(request, 'param', 2)}')"
    )
    try:
        yield table, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def write(spark, table, identifier, partition, *, wap=None, overwrite=False):
    writer = spark.sql(f"SELECT {identifier}L AS id, {partition} AS p").writeTo(table)
    if wap is not None:
        writer = writer.option("snapshot-property.wap.id", wap)
    if overwrite:
        writer.overwritePartitions()
    else:
        writer.append()


def rollback(spark, table, snapshot_id):
    spark.sql(f"CALL system.rollback_to_snapshot('{table}', {snapshot_id})").collect()


@pytest.mark.parametrize("procedure", ["cherrypick_snapshot", "publish_changes"])
@pytest.mark.parametrize("diverged", [False, True], ids=["fast-forward", "replay"])
@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_publish_append(spark, target, procedure, diverged):
    table, path = target
    write(spark, table, 1, 0)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    wap = "audit" if procedure == "publish_changes" else None
    write(spark, table, 2, 0, wap=wap)
    source = _current_snapshot(_find_latest_metadata(path))
    source_id = source["snapshot-id"]
    rollback(spark, table, base)
    if diverged:
        write(spark, table, 3, 0)
    before = _find_latest_metadata(path)
    call = (
        f"CALL system.publish_changes(wap_id => 'audit', table => '{table}')"
        if wap
        else f"CALL system.cherrypick_snapshot('{table}', {source_id})"
    )
    result = spark.sql(call)
    assert result.schema == StructType(
        [StructField("source_snapshot_id", LongType(), False), StructField("current_snapshot_id", LongType(), False)]
    )
    row = result.first()
    after = _find_latest_metadata(path)
    current = _current_snapshot(after)
    assert tuple(row) == (source_id, current["snapshot-id"])
    assert source in after["snapshots"]
    if after["format-version"] > 1:
        expected_sequence = 4 if diverged else 2
        assert current["sequence-number"] == after["last-sequence-number"] == expected_sequence
        entries = [entry for entry in _data_entries(path).values() if entry.snapshot_id == current["snapshot-id"]]
        assert [(entry.sequence_number, entry.file_sequence_number) for entry in entries] == [
            (expected_sequence, expected_sequence)
        ]
    assert len(after["snapshots"]) == len(before["snapshots"]) + int(diverged)
    if diverged:
        assert current["parent-snapshot-id"] == before["current-snapshot-id"]
        assert current["summary"]["source-snapshot-id"] == str(source_id)
        assert current["summary"].get("published-wap-id") == wap
        assert current["summary"]["total-records"] == "3"
        assert current["snapshot-id"] != source_id
    else:
        assert current == source
    assert [r.id for r in spark.table(table).orderBy("id").collect()] == ([1, 2, 3] if diverged else [1, 2])
    error = "published already" if wap else ("already picked" if diverged else "already an ancestor")
    with pytest.raises(Exception, match=error):
        spark.sql(call).collect()
    assert _find_latest_metadata(path) == after


@pytest.mark.parametrize("concurrent", ["disjoint", "same_partition", "missing_file"])
@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_pick_dynamic_overwrite(spark, target, concurrent):
    table, path = target
    write(spark, table, 10, 1)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 20, 1, overwrite=True)
    source = _find_latest_metadata(path)["current-snapshot-id"]
    rollback(spark, table, base)
    if concurrent == "missing_file":
        spark.sql(f"DELETE FROM {table} WHERE p = 1").collect()  # noqa: S608
    else:
        write(spark, table, 30, 1 if concurrent == "same_partition" else 2)
    call = f"CALL system.cherrypick_snapshot(table => '{table}', snapshot_id => {source})"
    before = _find_latest_metadata(path)
    if concurrent != "disjoint":
        error = "changed partition" if concurrent == "same_partition" else "missing required files"
        with pytest.raises(Exception, match=error):
            spark.sql(call).collect()
        assert _find_latest_metadata(path) == before
    else:
        result = spark.sql(call).first()
        current = _current_snapshot(_find_latest_metadata(path))
        assert tuple(result) == (source, current["snapshot-id"])
        assert current["parent-snapshot-id"] == before["current-snapshot-id"]
        assert current["summary"]["deleted-data-files"] == current["summary"]["added-data-files"] == "1"
        assert current["summary"]["operation"] == "overwrite"
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [20, 30]


def test_publish_requires_unique_staged_id(spark, target):
    table, path = target
    for identifier in (1, 2):
        write(spark, table, identifier, 0, wap="audit")
    before = _find_latest_metadata(path)
    for args, error in [
        (f"publish_changes('{table}', 'audit')", "non-unique WAP ID"),
        (f"publish_changes('{table}', 'Audit')", "unknown WAP ID"),
        (f"cherrypick_snapshot('{table}', -1)", "unknown snapshot ID"),
    ]:
        with pytest.raises(Exception, match=error):
            spark.sql(f"CALL system.{args}").collect()
    assert _find_latest_metadata(path) == before


def test_pick_delete_requires_fast_forward(spark, target):
    table, path = target
    write(spark, table, 1, 1)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    spark.sql(f"DELETE FROM {table} WHERE p = 1").collect()  # noqa: S608
    deleted = _current_snapshot(_find_latest_metadata(path))
    assert deleted["summary"]["operation"] == "delete"
    rollback(spark, table, base)
    source = deleted["snapshot-id"]
    call = f"CALL system.cherrypick_snapshot('{table}', {source})"
    assert tuple(spark.sql(call).first()) == (source, source)
    assert spark.table(table).count() == 0
    rollback(spark, table, base)
    write(spark, table, 2, 2)
    before = _find_latest_metadata(path)
    with pytest.raises(Exception, match="not append, dynamic overwrite, or fast-forward"):
        spark.sql(call).collect()
    assert _find_latest_metadata(path) == before


def test_pick_dynamic_overwrite_removes_dangling_deletion_vector(spark, target):
    table, path = target
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')")
    spark.sql("SELECT id, 1 AS p FROM range(3)").coalesce(1).writeTo(table).append()
    base = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 20, 1, overwrite=True)
    source = _find_latest_metadata(path)["current-snapshot-id"]
    rollback(spark, table, base)
    spark.sql(f"DELETE FROM {table} WHERE id = 1").collect()  # noqa: S608
    before = _current_snapshot(_find_latest_metadata(path))
    assert before["summary"]["total-delete-files"] == "1"
    spark.sql(f"CALL system.cherrypick_snapshot('{table}', {source})").collect()
    current = _current_snapshot(_find_latest_metadata(path))
    assert spark.table(f"{table}.files").select("first_row_id").first().first_row_id == current["first-row-id"]
    assert current["summary"]["removed-dvs"] == "1"
    assert current["summary"]["total-delete-files"] == "0"
    assert [r.id for r in spark.table(table).collect()] == [20]


@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_pick_dynamic_overwrite_requires_ancestor_even_for_disjoint_partitions(spark, target):
    table, path = target
    write(spark, table, 1, 1)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 2, 2)
    write(spark, table, 20, 1, overwrite=True)
    source = _find_latest_metadata(path)["current-snapshot-id"]
    rollback(spark, table, base)
    write(spark, table, 3, 3)
    before = _find_latest_metadata(path)
    with pytest.raises(Exception, match="not based on an ancestor"):
        spark.sql(f"CALL system.cherrypick_snapshot('{table}', {source})").collect()
    assert _find_latest_metadata(path) == before


@pytest.mark.parametrize("target", [1], indirect=True)
def test_pick_from_v1_inline_manifests(spark, target):
    table, path = target
    write(spark, table, 1, 0)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 2, 0)
    source = _find_latest_metadata(path)["current-snapshot-id"]
    paths = {source: [r.path for r in spark.table(f"{table}.manifests").select("path").collect()]}
    rollback(spark, table, base)
    write(spark, table, 3, 0)
    parent = _find_latest_metadata(path)["current-snapshot-id"]
    paths[parent] = [r.path for r in spark.table(f"{table}.manifests").select("path").collect()]

    def use_inline_manifests(metadata):
        for snapshot in metadata["snapshots"]:
            if snapshot["snapshot-id"] in paths:
                del snapshot["manifest-list"]
                snapshot["manifests"] = paths[snapshot["snapshot-id"]]

    _edit_latest_metadata(path, use_inline_manifests)
    result = spark.sql(f"CALL system.cherrypick_snapshot('{table}', {source})").first()
    current = _current_snapshot(_find_latest_metadata(path))
    assert tuple(result) == (source, current["snapshot-id"])
    assert current["parent-snapshot-id"] == parent
    assert [r.id for r in spark.table(table).orderBy("id").collect()] == [1, 2, 3]
    for snapshot in _find_latest_metadata(path)["snapshots"]:
        if snapshot["snapshot-id"] in paths:
            assert "manifest-list" not in snapshot
            assert snapshot["manifests"] == paths[snapshot["snapshot-id"]]

    rollback(spark, table, base)
    assert tuple(spark.sql(f"CALL system.cherrypick_snapshot('{table}', {source})").first()) == (source, source)
    assert [r.id for r in spark.table(table).orderBy("id").collect()] == [1, 2]
    write(spark, table, 4, 0)
    assert [r.id for r in spark.table(table).orderBy("id").collect()] == [1, 2, 4]


@pytest.fixture(params=[1, 2])
def staged_table(sql_catalog, tmp_path, request):
    name = "external_staged"
    path = tmp_path / name
    table = sql_catalog.create_table(
        f"default.{name}",
        pa.schema([("id", pa.int64())]),
        location=path.as_uri(),
        properties={"format-version": str(request.param), "write.wap.enabled": "true"},
    )
    return table, path


@contextmanager
def register_staged_table(spark, table):
    name = table.name()[-1]
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(table.location())}'")
    try:
        yield name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("procedure", ["cherrypick_snapshot", "publish_changes"])
@pytest.mark.parametrize("named", [False, True], ids=["positional", "named"])
def test_publish_first_staged_snapshot(spark, staged_table, procedure, named):
    table, path = staged_table
    table.append(pa.table({"id": [1]}), snapshot_properties={"wap.id": "one"}, branch=None)
    assert table.current_snapshot() is None
    (source,) = table.snapshots()
    with register_staged_table(spark, table) as name:
        assert spark.table(name).collect() == []
        key, value = (
            ("wap_id", "'one'") if procedure == "publish_changes" else ("snapshot_id", f"{source.snapshot_id}L")
        )
        args = f"{key} => {value}, table => '{name}'" if named else f"'{name}', {value}"
        assert tuple(spark.sql(f"CALL system.{procedure}({args})").first()) == (source.snapshot_id, source.snapshot_id)
        assert [row.id for row in spark.table(name).collect()] == [1]
        metadata = _find_latest_metadata(path)
        assert [entry["snapshot-id"] for entry in metadata["snapshot-log"]] == [source.snapshot_id]
        assert len(metadata["snapshots"]) == 1


@pytest.mark.parametrize("intervening_append", [False, True])
def test_publish_sibling_staged_snapshots(spark, staged_table, intervening_append):
    table, path = staged_table
    table.append(pa.table({"id": [1]}))
    base = table.current_snapshot().snapshot_id
    for value, wap in [(2, "first"), (3, "second")]:
        table.append(pa.table({"id": [value]}), snapshot_properties={"wap.id": wap}, branch=None)
    if intervening_append:
        table.append(pa.table({"id": [4]}))
    first, second = table.snapshots()[1:3]
    assert first.parent_snapshot_id == second.parent_snapshot_id == base
    with register_staged_table(spark, table) as name:
        before = _find_latest_metadata(path)
        for wap, snapshot in [("first", first), ("second", second)]:
            source, published = spark.sql(f"CALL system.publish_changes('{name}', '{wap}')").first()
            assert source == snapshot.snapshot_id
            assert (source == published) == (wap == "first" and not intervening_append)
            after = _find_latest_metadata(path)
            assert after["current-snapshot-id"] == published
            assert _current_snapshot(after)["parent-snapshot-id"] == before["current-snapshot-id"]
            assert len(after["snapshot-log"]) == len(before["snapshot-log"]) + 1
            assert len(after["snapshots"]) == len(before["snapshots"]) + (source != published)
            before = after
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == (
            [1, 2, 3, 4] if intervening_append else [1, 2, 3]
        )


def test_cherrypick_sibling_snapshots_with_same_wap_id(spark, staged_table):
    table, path = staged_table
    for value in (1, 2):
        table.append(pa.table({"id": [value]}), snapshot_properties={"wap.id": "same"}, branch=None)
    first, second = table.snapshots()
    with register_staged_table(spark, table) as name:
        before = _find_latest_metadata(path)
        with pytest.raises(PySparkException, match="non-unique WAP ID"):
            spark.sql(f"CALL system.publish_changes('{name}', 'same')").collect()
        assert _find_latest_metadata(path) == before
        assert tuple(spark.sql(f"CALL system.cherrypick_snapshot('{name}', {first.snapshot_id})").first()) == (
            first.snapshot_id,
            first.snapshot_id,
        )
        after = _find_latest_metadata(path)
        with pytest.raises(PySparkException, match="published already"):
            spark.sql(f"CALL system.cherrypick_snapshot('{name}', {second.snapshot_id})").collect()
        assert _find_latest_metadata(path) == after
        assert [row.id for row in spark.table(name).collect()] == [1]


@pytest.mark.parametrize(
    ("procedure", "parameter", "value"),
    [("cherrypick_snapshot", "snapshot_id", "1L"), ("publish_changes", "wap_id", "'one'")],
)
def test_publish_argument_errors(spark, target, procedure, parameter, value):
    table, path = target
    before = _find_latest_metadata(path)
    cases = [
        (f"system.{procedure}('n', table => '{table}', {value})", "UNEXPECTED_POSITIONAL_ARGUMENT"),
        (f"custom.{procedure}('n', '{table}', {value})", "Procedure not found"),
        (f"system.{procedure}('{table}')", f"Missing required argument '{parameter}'"),
        (f"system.{procedure}('', {value})", "identifier"),
    ]
    if procedure == "cherrypick_snapshot":
        cases.append((f"system.{procedure}('{table}', '2.2')", "Cannot cast"))
    for call, error in cases:
        with pytest.raises(PySparkException, match=error):
            spark.sql(f"CALL {call}").collect()
    assert _find_latest_metadata(path) == before


@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_reverse_cherrypick_after_rollback(spark, target):
    table, path = target
    for value in (1, 2, 3):
        write(spark, table, value, 0)
    a, b, c = [snap["snapshot-id"] for snap in _find_latest_metadata(path)["snapshots"]]
    rollback(spark, table, a)
    for source, expected in [(c, [1, 3]), (b, [1, 2, 3])]:
        original, published = spark.sql(f"CALL system.cherrypick_snapshot('{table}', {source})").first()
        assert original == source != published
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == expected
    after = _find_latest_metadata(path)
    assert len(after["snapshots"]) == 5
    with pytest.raises(PySparkException, match="already an ancestor"):
        spark.sql(f"CALL system.cherrypick_snapshot('{table}', {a})").collect()
    assert _find_latest_metadata(path) == after


@pytest.mark.parametrize("target", [3], indirect=True)
def test_cherrypick_reassigns_row_ids_and_preserves_references(spark, target):
    table, path = target
    write(spark, table, 1, 1)
    a = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 2, 2)
    b = _find_latest_metadata(path)["current-snapshot-id"]
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit AS OF VERSION {b}")
    spark.sql(f"ALTER TABLE {table} CREATE TAG baseline AS OF VERSION {a}")
    rollback(spark, table, a)
    write(spark, table, 3, 3)
    before = _find_latest_metadata(path)
    spark.sql(f"CALL system.cherrypick_snapshot('{table}', {b})").collect()
    after = _find_latest_metadata(path)
    assert before["next-row-id"] == 3
    assert after["next-row-id"] == 4
    assert _current_snapshot(after)["first-row-id"] == 3
    for ref in ("audit", "baseline"):
        assert after["refs"][ref] == before["refs"][ref]
    files = spark.table(f"{table}.files").select("partition.p", "first_row_id").collect()
    assert {file.p: file.first_row_id for file in files} == {1: 0, 2: 3, 3: 2}


@pytest.mark.parametrize("diverged", [False, True], ids=["fast-forward", "replay"])
@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_expiration_after_cherrypick_preserves_live_files(spark, target, diverged):
    table, path = target
    write(spark, table, 1, 1)
    a = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 2, 1, overwrite=not diverged)
    b = _find_latest_metadata(path)["current-snapshot-id"]
    if diverged:
        rollback(spark, table, a)
    write(spark, table, 3, 2)
    c = _find_latest_metadata(path)["current-snapshot-id"]
    if not diverged:
        rollback(spark, table, a)
    source, picked = spark.sql(f"CALL system.cherrypick_snapshot('{table}', {b})").first()
    assert source == b
    assert (picked != b) == diverged
    if diverged:
        counts = spark.sql(f"CALL system.expire_snapshots('{table}', snapshot_ids => array({b}L))").first()
        assert counts.deleted_data_files_count == 0
        assert b not in {snap["snapshot-id"] for snap in _find_latest_metadata(path)["snapshots"]}
    else:
        spark.sql(f"CALL system.set_current_snapshot('{table}', {c})").collect()
    count = spark.sql(
        f"CALL system.expire_snapshots('{table}', older_than => TIMESTAMP '2100-01-01', retain_last => 1)"
    ).first()
    # The overwritten file from A is eligible for deletion on the fast-forward branch.
    assert count.deleted_data_files_count == (0 if diverged else 1)
    assert [row.id for row in spark.table(table).orderBy("id").collect()] == ([1, 2, 3] if diverged else [2, 3])


@pytest.mark.parametrize("target", [1, 2, 3], indirect=True)
def test_cherrypick_overwrite_requires_fast_forward(spark, target):
    table, path = target
    write(spark, table, 1, 1)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    spark.sql(f"INSERT OVERWRITE TABLE {table} VALUES (2, 1)")  # noqa: S608
    source = _current_snapshot(_find_latest_metadata(path))
    assert source["summary"]["operation"] == "overwrite"
    assert "replace-partitions" not in source["summary"]
    rollback(spark, table, base)
    call = f"CALL system.cherrypick_snapshot('{table}', {source['snapshot-id']})"
    assert tuple(spark.sql(call).first()) == (source["snapshot-id"], source["snapshot-id"])
    assert [row.id for row in spark.table(table).collect()] == [2]
    rollback(spark, table, base)
    write(spark, table, 3, 2)
    before = _find_latest_metadata(path)
    with pytest.raises(PySparkException, match="not append, dynamic overwrite, or fast-forward"):
        spark.sql(call).collect()
    assert _find_latest_metadata(path) == before
    assert [row.id for row in spark.table(table).orderBy("id").collect()] == [1, 3]


def test_cherrypick_dynamic_overwrite_without_parent(spark, sql_catalog, tmp_path):
    path = tmp_path / "root_overwrite"
    table = sql_catalog.create_table("default.root_overwrite", pa.schema([("id", pa.int64())]), location=path.as_uri())
    with table.update_spec() as spec:
        spec.add_identity("id")
    table.append(pa.table({"id": [1]}), branch=None)
    (other_root,) = table.snapshots()
    with register_staged_table(spark, table) as name:
        spark.sql("SELECT 2L AS id").writeTo(name).overwritePartitions()
        source = _current_snapshot(_find_latest_metadata(path))
        assert source.get("parent-snapshot-id") is None
        assert source["summary"]["operation"] == "overwrite"
        assert source["summary"]["replace-partitions"] == "true"
        spark.sql(f"CALL system.set_current_snapshot('{name}', {other_root.snapshot_id})").collect()
        original, published = spark.sql(f"CALL system.cherrypick_snapshot('{name}', {source['snapshot-id']})").first()
        assert original == source["snapshot-id"] != published
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2]


@pytest.mark.parametrize("limit", [0, 1])
def test_replayed_partition_summary(spark, target, limit):
    table, path = target
    write(spark, table, 1, 0)
    base = _find_latest_metadata(path)["current-snapshot-id"]
    write(spark, table, 2, 1, wap="audit")
    rollback(spark, table, base)
    write(spark, table, 3, 2)
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.summary.partition-limit'='{limit}')")
    spark.sql(f"CALL system.publish_changes('{table}', 'audit')").collect()
    summary = _current_snapshot(_find_latest_metadata(path))["summary"]
    assert summary["changed-partition-count"] == "1"
    if limit:
        assert summary["partition-summaries-included"] == "true"
        assert (
            summary["partitions.p=1"]
            == f"added-data-files=1,added-records=1,added-files-size={summary['added-files-size']}"
        )
    else:
        assert "partition-summaries-included" not in summary
        assert "partitions.p=1" not in summary
