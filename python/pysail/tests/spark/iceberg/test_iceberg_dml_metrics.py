# ruff: noqa: S608
"""Iceberg action summaries count affected rows separately from storage records."""

import pytest
from pyspark.errors import UnsupportedOperationException
from pyspark.sql.types import StructType

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

SCHEMA = "id INT, dep STRING, salary INT"
BASE = ((1, "hr", 100), (2, "software", 200), (3, "finance", 300))
PREFIXES = ("spark.merge-into.", "spark.insert.", "spark.update.", "spark.delete.")


@pytest.fixture(
    params=[
        (1, "copy-on-write"),
        (2, "copy-on-write"),
        (3, "copy-on-write"),
        (2, "merge-on-read"),
        (3, "merge-on-read"),
    ],
    ids=["v1-cow", "v2-cow", "v3-cow", "v2-mor", "v3-mor"],
)
def metrics_table(spark, tmp_path, request):
    version, mode = request.param
    table = "iceberg_action_metrics"
    spark.sql(f"""
        CREATE TABLE {table} ({SCHEMA}) USING iceberg
        LOCATION '{escape_sql_string_literal(str(tmp_path))}' TBLPROPERTIES (
          'format-version'='{version}', 'write.delete.mode'='{mode}',
          'write.update.mode'='{mode}', 'write.merge.mode'='{mode}',
          'write.distribution-mode'='none')
    """)
    try:
        yield table, mode
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _merge_counts(
    *,
    copied=0,
    updated=0,
    deleted=0,
    inserted=0,
    matched_updated=0,
    matched_deleted=0,
    target_only_updated=0,
    target_only_deleted=0,
):
    return {
        "copied": copied,
        "updated": updated,
        "deleted": deleted,
        "inserted": inserted,
        "matched-updated": matched_updated,
        "matched-deleted": matched_deleted,
        "not-matched-by-source-updated": target_only_updated,
        "not-matched-by-source-deleted": target_only_deleted,
    }


def _metrics(counts):
    return {f"spark.merge-into.num-target-rows-{key}": str(value) for key, value in counts.items()}


def _seed(spark, table, path, rows=BASE):
    spark.createDataFrame(rows, SCHEMA).coalesce(1).writeTo(table).append()
    files = spark.table(f"{table}.files").select("content", "record_count").collect()
    assert files == ([(0, len(rows))] if rows else [])
    summary = _current_snapshot(_find_latest_metadata(path))["summary"]
    assert {k: v for k, v in summary.items() if k.startswith(PREFIXES)} == {
        "spark.insert.num-inserted-rows": str(len(rows))
    }


def _inventory(path):
    return {p.relative_to(path): p.read_bytes() for p in path.rglob("*") if p.is_file()}


def _files_by_path(spark, table):
    files = spark.table(f"{table}.files").collect()
    records = {row.file_path: row for row in files}
    assert len(records) == len(files)
    return records


def _check(spark, table, path, sql, rows, metrics, *, changed=True, operation=None):
    before = _find_latest_metadata(path)
    schema = spark.table(table).schema
    inventory = _inventory(path) if not changed else None
    files = _files_by_path(spark, table) if metrics and set(metrics.values()) == {"0"} else None
    result = spark.sql(sql)
    assert result.schema == StructType([])
    assert result.collect() == []
    assert spark.table(table).schema == schema
    after = _find_latest_metadata(path)
    assert spark.table(table).orderBy("id", "dep", "salary").collect() == sorted(rows)
    assert (before.get("current-snapshot-id") != after.get("current-snapshot-id")) == changed
    if not changed:
        assert after == before
        assert _inventory(path) == inventory
        return _current_snapshot(after)["summary"] if after.get("snapshots") else {}
    assert len(after["snapshots"]) == len(before.get("snapshots", [])) + 1
    assert after["snapshots"][:-1] == before.get("snapshots", [])
    current = _current_snapshot(after)
    parent = _current_snapshot(before)["snapshot-id"] if before.get("snapshots") else None
    assert current.get("parent-snapshot-id") == parent
    if after["format-version"] >= 2:  # noqa: PLR2004
        assert after["last-sequence-number"] == before["last-sequence-number"] + 1
    summary = current["summary"]
    assert {k: v for k, v in summary.items() if k.startswith(PREFIXES)} == metrics
    if operation is not None:
        assert summary["operation"] == operation
    if files is not None:
        assert _files_by_path(spark, table) == files
        if after["format-version"] == 3:  # noqa: PLR2004
            assert after["next-row-id"] == before["next-row-id"]
            assert current["added-rows"] == 0
    return summary


# Each nonempty target starts with one live data file; only COW copies untouched rows.
MERGES = [
    pytest.param(
        BASE[:2],
        ((1, "updated", 150), (3, "new", 300)),
        "WHEN MATCHED THEN UPDATE SET dep=s.dep",
        ((1, "updated", 100), BASE[1]),
        _merge_counts(copied=1, updated=1, matched_updated=1),
        "overwrite",
        "overwrite",
        id="matched-update",
    ),
    pytest.param(
        BASE,
        BASE[:2],
        "WHEN MATCHED THEN DELETE",
        BASE[2:],
        _merge_counts(copied=1, deleted=2, matched_deleted=2),
        "overwrite",
        "delete",
        id="matched-delete",
    ),
    pytest.param(
        BASE,
        ((1, "hr-updated", 150), (2, "delete", 250), (4, "new", 400)),
        "WHEN MATCHED AND s.dep='delete' THEN DELETE WHEN MATCHED THEN UPDATE SET dep=s.dep "
        "WHEN NOT MATCHED THEN INSERT *",
        ((1, "hr-updated", 100), BASE[2], (4, "new", 400)),
        _merge_counts(copied=1, updated=1, deleted=1, inserted=1, matched_updated=1, matched_deleted=1),
        "overwrite",
        "overwrite",
        id="mixed-with-copy",
    ),
    pytest.param(
        BASE,
        ((1, "updated", 150),),
        "WHEN MATCHED THEN UPDATE SET dep=s.dep WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep='orphaned'",
        ((1, "updated", 100), (2, "orphaned", 200), (3, "orphaned", 300)),
        _merge_counts(updated=3, matched_updated=1, target_only_updated=2),
        "overwrite",
        "overwrite",
        id="target-only-update",
    ),
    pytest.param(
        BASE,
        ((1, "updated", 150),),
        "WHEN MATCHED THEN UPDATE SET dep=s.dep WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ((1, "updated", 100),),
        _merge_counts(updated=1, deleted=2, matched_updated=1, target_only_deleted=2),
        "overwrite",
        "overwrite",
        id="target-only-delete",
    ),
    pytest.param(
        (*BASE, (4, "marketing", 400), (5, "sales", 500)),
        ((1, "hr", 150), (2, "software", 250), (3, "finance", 350), (6, "new", 600), (7, "newer", 700)),
        "WHEN MATCHED AND t.salary<200 THEN DELETE WHEN MATCHED THEN UPDATE SET salary=s.salary "
        "WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE AND t.salary>400 THEN DELETE "
        "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep='orphaned'",
        ((2, "software", 250), (3, "finance", 350), (4, "orphaned", 400), (6, "new", 600), (7, "newer", 700)),
        _merge_counts(
            updated=3,
            deleted=2,
            inserted=2,
            matched_updated=2,
            matched_deleted=1,
            target_only_updated=1,
            target_only_deleted=1,
        ),
        "overwrite",
        "overwrite",
        id="all-five-actions",
    ),
    pytest.param(
        BASE,
        (BASE[0], (4, "new", 400)),
        "WHEN NOT MATCHED THEN INSERT *",
        (*BASE, (4, "new", 400)),
        _merge_counts(inserted=1),
        "append",
        "append",
        id="insert-only",
    ),
    pytest.param(
        BASE,
        BASE[:1],
        "WHEN NOT MATCHED THEN INSERT *",
        BASE,
        _merge_counts(),
        "append",
        "append",
        id="insert-only-noop",
    ),
    pytest.param(
        BASE,
        (),
        "WHEN MATCHED THEN UPDATE SET dep=s.dep WHEN NOT MATCHED THEN INSERT *",
        BASE,
        _merge_counts(),
        "overwrite",
        "overwrite",
        id="empty-source",
    ),
    pytest.param(
        BASE,
        BASE,
        "WHEN MATCHED AND s.salary<0 THEN UPDATE SET dep=s.dep",
        BASE,
        _merge_counts(),
        "overwrite",
        "overwrite",
        id="conditional-noop",
    ),
    pytest.param(
        BASE,
        (),
        "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT * "
        "WHEN NOT MATCHED BY SOURCE AND t.id=1 THEN DELETE "
        "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET dep='orphaned'",
        ((2, "orphaned", 200), (3, "orphaned", 300)),
        _merge_counts(updated=2, deleted=1, target_only_updated=2, target_only_deleted=1),
        "overwrite",
        "overwrite",
        id="empty-source-target-actions",
    ),
    pytest.param(
        (),
        BASE[:2],
        "WHEN MATCHED THEN UPDATE SET dep=s.dep WHEN NOT MATCHED THEN INSERT *",
        BASE[:2],
        _merge_counts(inserted=2),
        "append",
        "append",
        id="empty-target",
    ),
    pytest.param(
        (),
        (),
        "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT *",
        (),
        _merge_counts(),
        "overwrite",
        "overwrite",
        id="both-empty",
    ),
    pytest.param(
        (BASE[0], BASE[0], BASE[2]),
        (BASE[0], BASE[0], (4, "new", 400), (4, "new", 400)),
        "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT *",
        (BASE[2], (4, "new", 400), (4, "new", 400)),
        _merge_counts(copied=1, deleted=2, inserted=2, matched_deleted=2),
        "overwrite",
        "overwrite",
        id="duplicate-delete-and-insert",
    ),
]


@pytest.mark.parametrize(("initial", "source", "clauses", "rows", "counts", "cow_operation", "mor_operation"), MERGES)
def test_merge_action_metrics(
    spark,
    tmp_path,
    metrics_table,
    initial,
    source,
    clauses,
    rows,
    counts,
    cow_operation,
    mor_operation,
):
    table, mode = metrics_table
    _seed(spark, table, tmp_path, initial)
    spark.createDataFrame(source, SCHEMA).createOrReplaceTempView("iceberg_metrics_source")
    try:
        counts = {**counts, "copied": counts["copied"] if mode == "copy-on-write" else 0}
        operation = cow_operation if mode == "copy-on-write" else mor_operation
        summary = _check(
            spark,
            table,
            tmp_path,
            f"MERGE INTO {table} t USING iceberg_metrics_source s ON t.id=s.id {clauses}",
            rows,
            _metrics(counts),
            operation=operation,
        )
        assert int(summary.get("added-records", "0")) == counts["copied"] + counts["updated"] + counts["inserted"]
        removed = len(initial) if mode == "copy-on-write" and (counts["updated"] or counts["deleted"]) else 0
        assert int(summary.get("deleted-records", "0")) == removed
        if operation == "append":
            assert summary["total-delete-files"] == "0"
            assert summary.get("deleted-data-files", "0") == "0"
    finally:
        spark.catalog.dropTempView("iceberg_metrics_source")


@pytest.mark.parametrize(
    "command", ["insert", "empty-insert", "merge", "empty-merge", "update", "general-merge", "metadata-delete"]
)
def test_first_snapshot_action_metrics(spark, tmp_path, metrics_table, command):
    table, _ = metrics_table
    assert not _find_latest_metadata(tmp_path).get("snapshots")
    empty = command.startswith("empty") or command in ("update", "general-merge", "metadata-delete")
    source = "SELECT 1 AS id, 'hr' AS dep, 100 AS salary" + (" WHERE false" if empty else "")
    rows = () if empty else BASE[:1]
    operation = "append"
    if command.endswith("insert"):
        sql = f"INSERT INTO {table} {source}"
        metrics = {"spark.insert.num-inserted-rows": str(len(rows))}
    elif command == "update":
        sql = f"UPDATE {table} SET dep=dep WHERE id=99"
        metrics = {"spark.update.num-updated-rows": "0", "spark.update.num-copied-rows": "0"}
        operation = "overwrite"
    elif command == "metadata-delete":
        sql = f"DELETE FROM {table} WHERE id=99"
        metrics = {}
        operation = "delete"
    else:
        clauses = "WHEN NOT MATCHED THEN INSERT *"
        if command == "general-merge":
            clauses = "WHEN MATCHED THEN UPDATE SET dep=s.dep " + clauses
        sql = f"MERGE INTO {table} t USING ({source}) s ON t.id=s.id {clauses}"
        metrics = _metrics(_merge_counts(inserted=len(rows)))
        if command == "general-merge":
            operation = "overwrite"
    summary = _check(spark, table, tmp_path, sql, rows, metrics, operation=operation)
    if empty:
        assert summary["total-data-files"] == "0"
        assert summary["total-records"] == "0"


# NULL predicates, including bare NULL, select no rows and must not commit.
@pytest.mark.parametrize("populated", [False, True], ids=["snapshotless", "populated"])
@pytest.mark.parametrize("predicate", ["false", "NOT true", "1=0", "false AND id=1", "NULL", "CAST(NULL AS BOOLEAN)"])
def test_delete_never_matching_predicate_preserves_all_artifacts(spark, tmp_path, metrics_table, predicate, populated):
    table, _ = metrics_table
    if populated:
        _seed(spark, table, tmp_path)
    else:
        assert not _find_latest_metadata(tmp_path).get("snapshots")
    _check(
        spark, table, tmp_path, f"DELETE FROM {table} WHERE {predicate}", BASE if populated else (), {}, changed=False
    )


@pytest.mark.parametrize(
    ("assignment", "predicate", "rows", "updated", "copied"),
    [
        pytest.param("salary+1", "id=1", ((1, "hr", 101), *BASE[1:]), 1, 2, id="update"),
        pytest.param("salary", "id=1", BASE, 1, 2, id="same-value-update"),
        pytest.param("salary", "id=99", BASE, 0, 0, id="no-match-update"),
    ],
)
def test_update_action_metrics(spark, tmp_path, metrics_table, assignment, predicate, rows, updated, copied):
    table, mode = metrics_table
    _seed(spark, table, tmp_path)
    _check(
        spark,
        table,
        tmp_path,
        f"UPDATE {table} SET salary={assignment} WHERE {predicate}",
        rows,
        {
            "spark.update.num-updated-rows": str(updated),
            "spark.update.num-copied-rows": str(copied if mode == "copy-on-write" else 0),
        },
        operation="overwrite",
    )


@pytest.mark.parametrize(
    ("predicate", "rows", "deleted", "copied", "cow_operation", "mor_operation"),
    [
        pytest.param("length(dep)=2", BASE[1:], 1, 2, "overwrite", "delete", id="scanned-delete"),
        pytest.param("length(dep)=999", BASE, 0, 0, "overwrite", "overwrite", id="no-match-scanned-delete"),
        pytest.param("length(dep)>0", (), 3, 0, "delete", "delete", id="removal-only-scanned-delete"),
    ],
)
def test_scanned_delete_action_metrics(
    spark,
    tmp_path,
    metrics_table,
    predicate,
    rows,
    deleted,
    copied,
    cow_operation,
    mor_operation,
):
    table, mode = metrics_table
    _seed(spark, table, tmp_path)
    _check(
        spark,
        table,
        tmp_path,
        f"DELETE FROM {table} WHERE {predicate}",
        rows,
        {
            "spark.delete.num-deleted-rows": str(deleted),
            "spark.delete.num-copied-rows": str(copied if mode == "copy-on-write" else 0),
        },
        operation=cow_operation if mode == "copy-on-write" else mor_operation,
    )


@pytest.mark.parametrize(
    ("predicate", "rows"),
    [
        pytest.param("true", (), id="metadata-delete"),
        pytest.param("id=99", BASE, id="no-match-metadata-delete"),
        pytest.param("id=1 AND id>20", BASE, id="contradictory-metadata-delete"),
    ],
)
def test_metadata_delete_action_metrics(spark, tmp_path, metrics_table, predicate, rows):
    table, _ = metrics_table
    _seed(spark, table, tmp_path)
    _check(spark, table, tmp_path, f"DELETE FROM {table} WHERE {predicate}", rows, {}, operation="delete")


@pytest.mark.parametrize(
    ("sql", "rows", "metrics", "operation"),
    [
        pytest.param(
            "INSERT INTO {table} SELECT 4, 'new', 400 WHERE false",
            BASE,
            {"spark.insert.num-inserted-rows": "0"},
            "append",
            id="empty-insert",
        ),
        pytest.param(
            "INSERT OVERWRITE {table} SELECT 4, 'new', 400",
            ((4, "new", 400),),
            {},
            "overwrite",
            id="overwrite",
        ),
        pytest.param(
            "INSERT OVERWRITE {table} SELECT 4, 'new', 400 WHERE false",
            (),
            {},
            "overwrite",
            id="empty-overwrite",
        ),
    ],
)
def test_insert_action_metrics(spark, tmp_path, metrics_table, sql, rows, metrics, operation):
    table, _ = metrics_table
    _seed(spark, table, tmp_path)
    _check(spark, table, tmp_path, sql.format(table=table), rows, metrics, operation=operation)


def test_copies_exclude_previously_deleted_rows(spark, tmp_path, metrics_table):
    table, mode = metrics_table
    _seed(spark, table, tmp_path)
    _check(
        spark,
        table,
        tmp_path,
        f"DELETE FROM {table} WHERE length(dep)=2",
        BASE[1:],
        {"spark.delete.num-deleted-rows": "1", "spark.delete.num-copied-rows": "2" if mode == "copy-on-write" else "0"},
    )
    # COW must count only the two remaining live rows, even after MOR tombstones.
    if mode == "merge-on-read":
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.update.mode'='copy-on-write')")
    _check(
        spark,
        table,
        tmp_path,
        f"UPDATE {table} SET salary=salary WHERE id=2",
        BASE[1:],
        {"spark.update.num-updated-rows": "1", "spark.update.num-copied-rows": "1"},
    )


def test_metrics_accumulate_multiple_writer_partitions(spark, tmp_path, metrics_table):
    table, _ = metrics_table
    source = spark.range(41, numPartitions=4).selectExpr(
        "cast(id AS INT) AS id", "concat('dep', id) AS dep", "cast(id * 10 AS INT) AS salary"
    )
    source.createOrReplaceTempView("iceberg_partitioned_source")
    try:
        rows = tuple((i, f"dep{i}", i * 10) for i in range(41))
        _check(
            spark,
            table,
            tmp_path,
            f"INSERT INTO {table} SELECT * FROM iceberg_partitioned_source",
            rows,
            {"spark.insert.num-inserted-rows": "41"},
        )
        files = spark.table(f"{table}.files").select("record_count").collect()
        assert len(files) > 1
        assert sum(row.record_count for row in files) == len(rows)
        _check(
            spark,
            table,
            tmp_path,
            f"UPDATE {table} SET salary=salary+1",
            tuple((i, dep, salary + 1) for i, dep, salary in rows),
            {"spark.update.num-updated-rows": "41", "spark.update.num-copied-rows": "0"},
        )
    finally:
        spark.catalog.dropTempView("iceberg_partitioned_source")


@pytest.mark.parametrize("rows", [(), BASE])
def test_new_path_append_summary(spark, tmp_path, rows):
    spark.createDataFrame(rows, SCHEMA).coalesce(1).write.format("iceberg").mode("append").save(str(tmp_path))
    metadata = _find_latest_metadata(tmp_path)
    assert len(metadata["snapshots"]) == 1
    summary = _current_snapshot(metadata)["summary"]
    assert {k: v for k, v in summary.items() if k.startswith(PREFIXES)} == {
        "spark.insert.num-inserted-rows": str(len(rows))
    }
    assert spark.read.format("iceberg").load(str(tmp_path)).orderBy("id").collect() == list(rows)


def test_rewrite_procedure_does_not_publish_action_metrics(spark, tmp_path, metrics_table):
    table, _ = metrics_table
    _seed(spark, table, tmp_path)
    statement = f"CALL system.rewrite_data_files('{table}', options => map('rewrite-all', 'true'))"
    before = _find_latest_metadata(tmp_path)
    if before["format-version"] == 3:  # noqa: PLR2004
        # v3 rewriting is explicitly unsupported; rejection must not publish a summary.
        files = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
        with pytest.raises(UnsupportedOperationException, match="v3 row-lineage preservation"):
            spark.sql(statement)
        assert _find_latest_metadata(tmp_path) == before
        assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == files
        assert spark.table(table).orderBy("id").collect() == list(BASE)
        return
    result = spark.sql(statement).first()
    assert result.rewritten_data_files_count == 1
    metadata = _find_latest_metadata(tmp_path)
    summary = _current_snapshot(metadata)["summary"]
    assert summary["operation"] == "replace"
    assert not {k: v for k, v in summary.items() if k.startswith(PREFIXES)}
    assert spark.table(table).orderBy("id").collect() == list(BASE)
    result = spark.sql(f"CALL system.rewrite_data_files('{table}')").first()
    assert tuple(result) == (0, 0, 0, 0, 0)
    assert _find_latest_metadata(tmp_path) == metadata
