# ruff: noqa: S608, PLR2004
"""Delta SQL results report affected rows and remain stable across repeated collection."""

import json
from contextlib import contextmanager

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.delta.test_delta_metadata_grouping import _write_partitioned_table


@contextmanager
def _table(spark, path, *, deletion_vectors=False, cdf=False):
    table = "delta_dml_results"
    spark.sql(
        f"CREATE TABLE {table} (id INT, value INT) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ("
        f"'delta.enableDeletionVectors'='{str(deletion_vectors).lower()}', "
        f"'delta.enableChangeDataFeed'='{str(cdf).lower()}')"
    )
    try:
        yield table
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _result(spark, sql, path, expected=None):
    frame = spark.sql(sql)
    names = [] if expected is None else ["num_affected_rows"]
    if expected is not None and len(expected) == 4:
        names += ["num_updated_rows", "num_deleted_rows", "num_inserted_rows"]
    assert frame.schema == StructType([StructField(name, LongType(), True) for name in names])
    rows = [] if expected is None else [expected]
    assert frame.collect() == rows
    commits = sorted((path / "_delta_log").glob("*.json"))
    assert frame.collect() == rows
    assert sorted((path / "_delta_log").glob("*.json")) == commits


def _rows(spark, table):
    return spark.table(table).orderBy("id").collect()


def _metrics(path):
    latest = max((path / "_delta_log").glob("*.json"))
    return next(
        json.loads(line)["commitInfo"]["operationMetrics"]
        for line in latest.read_text().splitlines()
        if "commitInfo" in json.loads(line)
    )


def _seed(spark, path):
    # One physical file makes copied rows distinguishable from affected rows.
    spark.createDataFrame([(1, 10), (2, None), (3, 30), (9, 90)], "id INT, value INT").coalesce(1).write.format(
        "delta"
    ).mode("append").save(str(path))


def test_delta_insert_results(spark, tmp_path):
    with _table(spark, tmp_path) as table:
        _result(spark, f"INSERT INTO {table} SELECT 1, 10 WHERE false", tmp_path)
        assert _rows(spark, table) == []
        _result(spark, f"INSERT INTO {table} VALUES (1, 10), (2, 20)", tmp_path)
        assert _rows(spark, table) == [(1, 10), (2, 20)]
        _result(spark, f"INSERT INTO {table} SELECT id + 2, value FROM {table}", tmp_path)
        assert _rows(spark, table) == [(1, 10), (2, 20), (3, 10), (4, 20)]
        _result(spark, f"INSERT OVERWRITE {table} VALUES (5, 50)", tmp_path)
        assert _rows(spark, table) == [(5, 50)]
        _result(spark, f"INSERT OVERWRITE {table} SELECT 1, 10 WHERE false", tmp_path)
        assert _rows(spark, table) == []


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "dv"])
@pytest.mark.parametrize("cdf", [False, True], ids=["no-cdf", "cdf"])
def test_delta_update_delete_results(spark, tmp_path, deletion_vectors, cdf):
    with _table(spark, tmp_path, deletion_vectors=deletion_vectors, cdf=cdf) as table:
        _seed(spark, tmp_path)
        _result(spark, f"UPDATE {table} SET value = 0 WHERE id = 99", tmp_path, (0,))
        assert _rows(spark, table) == [(1, 10), (2, None), (3, 30), (9, 90)]
        if cdf:
            # Empty tagged streams still report exact zeros in the CDC-only commit.
            metrics = _metrics(tmp_path)
            assert [int(metrics[name]) for name in ("numUpdatedRows", "numCopiedRows", "numOutputRows")] == [0, 0, 0]
        _result(spark, f"UPDATE {table} SET value = value WHERE id = 1", tmp_path, (1,))
        assert int(_metrics(tmp_path)["numUpdatedRows"]) == 1
        _result(spark, f"UPDATE {table} SET value = value + 1 WHERE id = 3", tmp_path, (1,))
        assert _rows(spark, table) == [(1, 10), (2, None), (3, 31), (9, 90)]
        _result(spark, f"DELETE FROM {table} WHERE value < 20", tmp_path, (1,))
        assert int(_metrics(tmp_path)["numDeletedRows"]) == 1
        assert _rows(spark, table) == [(2, None), (3, 31), (9, 90)]
        _result(spark, f"DELETE FROM {table} WHERE id = 99", tmp_path, (0,))
        _result(spark, f"UPDATE {table} SET value = id", tmp_path, (3,))
        assert _rows(spark, table) == [(2, 2), (3, 3), (9, 9)]
        _result(spark, f"DELETE FROM {table} WHERE id = 3", tmp_path, (1,))
        _result(spark, f"DELETE FROM {table}", tmp_path, (2,))
        assert _rows(spark, table) == []
        _result(spark, f"DELETE FROM {table}", tmp_path, (0,))
        _result(spark, f"UPDATE {table} SET value = 0", tmp_path, (0,))


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "dv"])
@pytest.mark.parametrize("cdf", [False, True], ids=["no-cdf", "cdf"])
def test_delta_merge_results(spark, tmp_path, deletion_vectors, cdf):
    with _table(spark, tmp_path, deletion_vectors=deletion_vectors, cdf=cdf) as table:
        _seed(spark, tmp_path)
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (
                WITH source AS (SELECT * FROM VALUES (1, 11), (2, 20), (4, 40) AS s(id, value))
                SELECT * FROM source
            ) s ON t.id = s.id
            WHEN MATCHED AND s.id = 2 THEN DELETE
            WHEN MATCHED THEN UPDATE SET value = s.value
            WHEN NOT MATCHED THEN INSERT *
        """,
            tmp_path,
            (3, 1, 1, 1),
        )
        assert _rows(spark, table) == [(1, 11), (3, 30), (4, 40), (9, 90)]
        metrics = _metrics(tmp_path)
        assert [
            int(metrics[name]) for name in ("numTargetRowsUpdated", "numTargetRowsDeleted", "numTargetRowsInserted")
        ] == [1, 1, 1]
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (SELECT 0 AS id WHERE false) s ON t.id = s.id
            WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET value = value + 1
            WHEN NOT MATCHED BY SOURCE AND t.id = 9 THEN DELETE
        """,
            tmp_path,
            (2, 1, 1, 0),
        )
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (SELECT 5 AS id, 50 AS value) s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT *
        """,
            tmp_path,
            (1, 0, 0, 1),
        )
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (SELECT 99 AS id) s ON t.id = s.id
            WHEN MATCHED THEN DELETE
        """,
            tmp_path,
            (0, 0, 0, 0),
        )
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (SELECT * FROM VALUES (1), (1) AS s(id)) s ON t.id = s.id
            WHEN MATCHED THEN DELETE
        """,
            tmp_path,
            (1, 0, 1, 0),
        )
        assert _rows(spark, table) == [(3, 31), (4, 40), (5, 50)]


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "dv"])
@pytest.mark.parametrize("cdf", [False, True], ids=["no-cdf", "cdf"])
def test_delta_merge_delete_counts_physical_targets_once(spark, tmp_path, deletion_vectors, cdf):
    # Duplicate matches delete each physical target once; inserts retain source duplicates.
    with _table(spark, tmp_path, deletion_vectors=deletion_vectors, cdf=cdf) as table:
        spark.createDataFrame([(1, 10), (1, 10), (2, 20), (2, 20), (9, 90)], "id INT, value INT").coalesce(
            1
        ).write.format("delta").mode("append").save(str(tmp_path))
        _result(
            spark,
            f"""
            MERGE INTO {table} t USING (SELECT * FROM VALUES (1), (1) AS source(id)) s
            ON t.id = s.id WHEN MATCHED THEN DELETE
        """,
            tmp_path,
            (2, 0, 2, 0),
        )
        assert _rows(spark, table) == [(2, 20), (2, 20), (9, 90)]
        assert int(_metrics(tmp_path)["numTargetRowsDeleted"]) == 2
        if cdf:
            changes = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", 2)
                .option("endingVersion", 2)
                .load(str(tmp_path))
            )
            assert changes.select("id", "value", "_change_type").collect() == [(1, 10, "delete"), (1, 10, "delete")]
        _result(
            spark,
            f"""
            MERGE INTO {table} t
            USING (SELECT * FROM VALUES (2, 200), (2, 200), (3, 30), (3, 30) AS source(id, value)) s
            ON t.id = s.id WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED AND s.value = 30 THEN INSERT *
            WHEN NOT MATCHED BY SOURCE AND t.id = 9 THEN UPDATE SET value = t.value + 1
        """,
            tmp_path,
            (5, 1, 2, 2),
        )
        assert _rows(spark, table) == [(3, 30), (3, 30), (9, 91)]
        metrics = _metrics(tmp_path)
        assert [
            int(metrics[name]) for name in ("numTargetRowsUpdated", "numTargetRowsDeleted", "numTargetRowsInserted")
        ] == [1, 2, 2]
        if cdf:
            changes = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", 3)
                .option("endingVersion", 3)
                .load(str(tmp_path))
            )
            assert sorted(changes.select("id", "value", "_change_type").collect()) == [
                (2, 20, "delete"),
                (2, 20, "delete"),
                (3, 30, "insert"),
                (3, 30, "insert"),
                (9, 90, "update_preimage"),
                (9, 91, "update_postimage"),
            ]


@pytest.mark.parametrize("known_stats", [False, True], ids=["missing-stats", "mixed-stats"])
@pytest.mark.parametrize(("predicate", "expected"), [("v = 2", 1), ("p = 'a'", -1), ("true", -1)])
def test_delta_delete_missing_metadata_counts(spark, tmp_path, known_stats, predicate, expected):
    # Scans count deleted rows; metadata-only removal reports -1 for missing counts.
    # A known count in another partition must not hide unknowns.
    path = tmp_path / "missing_stats"
    _write_partitioned_table(path, [("a", 1, [1, 2, 3], False), ("b", 1, [4, 5, 6], known_stats)])
    target = f"delta.`{path}`"
    _result(spark, f"DELETE FROM {target} WHERE {predicate}", path, (expected,))
    metrics = _metrics(path)
    if expected == -1:
        assert "numDeletedRows" not in metrics
    else:
        assert int(metrics["numDeletedRows"]) == 1
    remaining = (
        []
        if predicate == "true"
        else ([(4,), (5,), (6,)] if predicate == "p = 'a'" else [(1,), (3,), (4,), (5,), (6,)])
    )
    assert spark.sql(f"SELECT v FROM {target} ORDER BY v").collect() == remaining
