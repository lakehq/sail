# ruff: noqa: PLR2004

import json
import os
from datetime import UTC, datetime
from urllib.parse import unquote

import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import LongType, StringType, TimestampType


def _actions(path, version):
    return [json.loads(line) for line in (path / "_delta_log" / f"{version:020}.json").read_text().splitlines()]


def _feed(spark, path, start=0, end=None, **options):
    reader = spark.read.format("delta").option("readChangeFeed", "true")
    if start is not None:
        reader = reader.option("startingVersion", start)
    if end is not None:
        reader = reader.option("endingVersion", end)
    return reader.options(**options).load(str(path))


def _changes(spark, path, start, end=None):
    return sorted(tuple(row) for row in _feed(spark, path, start, end).select("id", "value", "_change_type").collect())


def test_cdf_append_overwrite_and_metadata(spark, tmp_path):
    path = tmp_path / "cdf"
    spark.range(3, numPartitions=1).write.format("delta").option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.range(3, 5).write.format("delta").mode("append").save(str(path))
    spark.range(7, 9).write.format("delta").mode("overwrite").save(str(path))
    rows = (
        _feed(spark, path)
        .selectExpr("id", "_change_type", "_commit_version", "unix_micros(_commit_timestamp) AS commit_micros")
        .collect()
    )
    assert sorted(tuple(row)[:3] for row in rows) == sorted(
        [(i, "insert", 0) for i in range(3)]
        + [(i, "insert", 1) for i in range(3, 5)]
        + [(i, "delete", 2) for i in range(5)]
        + [(i, "insert", 2) for i in range(7, 9)]
    )
    for row in rows:
        actions = _actions(path, row["_commit_version"])
        timestamp = next(
            (a["commitInfo"]["inCommitTimestamp"] for a in actions if "inCommitTimestamp" in a.get("commitInfo", {})),
            None,
        )
        if timestamp is None:
            timestamp = (path / "_delta_log" / f"{row['_commit_version']:020}.json").stat().st_mtime_ns // 1_000_000
        assert row.commit_micros == timestamp * 1000
    schema = _feed(spark, path).schema
    assert schema["_change_type"].dataType == StringType()
    assert schema["_commit_version"].dataType == LongType()
    assert schema["_commit_timestamp"].dataType == TimestampType()
    assert _feed(spark, path, 1, 1).count() == 2
    assert _feed(spark, path).where("_commit_version = 2 AND _change_type = 'delete'").select("id").count() == 5
    assert len(_feed(spark, path).select("_commit_version").limit(1).collect()) == 1
    assert spark.read.format("delta").load(str(path)).columns == ["id"]
    assert sorted(row.id for row in spark.read.format("delta").load(str(path)).collect()) == [7, 8]


@pytest.mark.parametrize("deletion_vectors", [False, True])
@pytest.mark.parametrize("column_mapping", ["none", "name", "id"])
def test_cdf_update_delete_merge(spark, tmp_path, deletion_vectors, column_mapping):
    path = tmp_path / "cdf_dml"
    writer = (
        spark.createDataFrame(
            [(1, "a", "x"), (2, "b", None), (3, "c", "x"), (9, "keep", "x")], "id LONG, value STRING, p STRING"
        )
        .coalesce(1)
        .write.format("delta")
        .partitionBy("p")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.enableDeletionVectors", str(deletion_vectors).lower())
        .option("delta.columnMapping.mode", column_mapping)
    )
    writer.save(str(path))
    target = f"delta.`{path}`"
    spark.sql(f"UPDATE {target} SET value = 'updated', p = 'a% /b' WHERE id = 1")  # noqa: S608
    assert _changes(spark, path, 1, 1) == [(1, "a", "update_preimage"), (1, "updated", "update_postimage")]
    assert sorted((row.p, row["_change_type"]) for row in _feed(spark, path, 1, 1).collect()) == [
        ("a% /b", "update_postimage"),
        ("x", "update_preimage"),
    ]
    spark.sql(
        f"""MERGE INTO {target} t
        USING (SELECT * FROM VALUES (2L, 'merged'), (3L, 'delete'), (4L, 'new') AS v(id, value)) s
        ON t.id = s.id
        WHEN MATCHED AND s.value = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT (id, value, p) VALUES (s.id, s.value, NULL)
        """  # noqa: S608
    )
    assert _changes(spark, path, 2, 2) == [
        (2, "b", "update_preimage"),
        (2, "merged", "update_postimage"),
        (3, "c", "delete"),
        (4, "new", "insert"),
    ]
    spark.sql(f"DELETE FROM {target} WHERE id = 2")  # noqa: S608
    assert _changes(spark, path, 3, 3) == [(2, "merged", "delete")]
    assert sorted((row.id, row.value) for row in spark.read.format("delta").load(str(path)).collect()) == [
        (1, "updated"),
        (4, "new"),
        (9, "keep"),
    ]
    for version in [1, 2, 3]:
        cdc = [a["cdc"] for a in _actions(path, version) if "cdc" in a]
        assert cdc
        for action in cdc:
            assert action["dataChange"] is False
            assert action["path"].startswith("_change_data/")
            filename = path / unquote(action["path"])
            assert filename.stat().st_size == action["size"]
            assert "_change_type" in pq.ParquetFile(filename).schema_arrow.names


def test_cdf_noop_rewrite_and_replace_where(spark, tmp_path):
    path = tmp_path / "cdf_replace"
    spark.createDataFrame([(1, "a"), (9, "b"), (None, "null")], "id LONG, value STRING").coalesce(1).write.format(
        "delta"
    ).option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id = 5")  # noqa: S608
    assert _feed(spark, path, 1, 1).collect() == []
    assert spark.read.format("delta").load(str(path)).count() == 3
    spark.createDataFrame([(9, "replacement")], "id LONG, value STRING").write.format("delta").mode("overwrite").option(
        "replaceWhere", "id = 9"
    ).save(str(path))
    assert _changes(spark, path, 2, 2) == [(9, "b", "delete"), (9, "replacement", "insert")]
    assert {(row.id, row.value) for row in spark.read.format("delta").load(str(path)).collect()} == {
        (1, "a"),
        (9, "replacement"),
        (None, "null"),
    }


def test_cdf_enable_disable_and_range_validation(spark, tmp_path):
    path = tmp_path / "cdf_enable"
    spark.range(1).write.format("delta").save(str(path))
    spark.sql(f"CREATE TABLE cdf_enable USING DELTA LOCATION '{path}'")
    spark.sql("ALTER TABLE cdf_enable SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
    spark.range(1, 2).write.format("delta").mode("append").save(str(path))
    spark.sql("ALTER TABLE cdf_enable SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')")
    spark.sql("DROP TABLE cdf_enable")
    assert [row.id for row in _feed(spark, path, 1, 2).collect()] == [1]
    for start, end, message in [
        (0, 2, "not enabled"),
        (1, 3, "not enabled"),
        (-1, 2, "Invalid"),
        (3, 2, "Invalid"),
        (10, None, "Invalid"),
    ]:
        with pytest.raises(Exception, match=message):
            _feed(spark, path, start, end).collect()
    for options, message in [
        ({}, "requires startingVersion"),
        ({"startingVersion": "1", "startingTimestamp": "2025-01-01 00:00:00"}, "only one"),
        ({"startingVersion": "1", "endingVersion": "2", "endingTimestamp": "2025-01-01 00:00:00"}, "only one"),
        ({"startingVersion": "1", "versionAsOf": "2"}, "time travel"),
    ]:
        with pytest.raises(Exception, match=message):
            _feed(spark, path, None, **options).collect()


def test_cdf_timestamp_bounds_and_missing_commit(spark, tmp_path):
    path = tmp_path / "cdf_time"
    spark.range(1).write.format("delta").option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.range(1, 2).write.format("delta").mode("append").save(str(path))
    spark.range(2, 3).write.format("delta").mode("append").save(str(path))
    for version in range(3):
        timestamp = 1_700_000_000 + version * 10
        os.utime(path / "_delta_log" / f"{version:020}.json", (timestamp, timestamp))

    def timestamp(seconds):
        return datetime.fromtimestamp(seconds, UTC).isoformat()

    rows = _feed(
        spark, path, None, startingTimestamp=timestamp(1_700_000_005), endingTimestamp=timestamp(1_700_000_015)
    ).collect()
    assert [(row.id, row["_commit_version"]) for row in rows] == [(1, 1)]
    (path / "_delta_log" / "00000000000000000001.json").unlink()
    with pytest.raises(Exception, match=r"(?i)(commit|version|missing)"):
        _feed(spark, path, 0, 2).collect()


@pytest.mark.parametrize("column", ["_change_type", "_commit_version", "_commit_timestamp"])
def test_cdf_reserved_columns(spark, tmp_path, column):
    with pytest.raises(Exception, match="reserves column"):
        spark.range(1).selectExpr(f"id AS {column}").write.format("delta").option(
            "delta.enableChangeDataFeed", "true"
        ).save(str(tmp_path / column))


def test_cdf_additive_schema(spark, tmp_path):
    path = tmp_path / "cdf_schema"
    spark.createDataFrame([(0,)], "id LONG").write.format("delta").option("delta.enableChangeDataFeed", "true").save(
        str(path)
    )
    spark.createDataFrame([(1, "new")], "id LONG, value STRING").write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(path))
    assert sorted((row.id, row.value) for row in _feed(spark, path).collect()) == [(0, None), (1, "new")]


def test_cdf_inferred_deletion_vectors(spark, tmp_path):
    path = tmp_path / "cdf_inferred_dv"
    spark.range(8, numPartitions=1).write.format("delta").option("delta.enableChangeDataFeed", "true").option(
        "delta.enableDeletionVectors", "true"
    ).save(str(path))
    for version, predicate in [(1, "id IN (1, 4)"), (2, "id IN (2, 6)")]:
        spark.sql(f"DELETE FROM delta.`{path}` WHERE {predicate}")  # noqa: S608
        # The protocol permits inferring a DV replacement from the add/remove pair.
        actions = [action for action in _actions(path, version) if "cdc" not in action]
        assert any("deletionVector" in action.get("add", {}) for action in actions)
        (path / "_delta_log" / f"{version:020}.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions)
        )
    rows = _feed(spark, path, 1, 2).select("id", "_change_type", "_commit_version").collect()
    assert sorted(tuple(row) for row in rows) == [
        (1, "delete", 1),
        (2, "delete", 2),
        (4, "delete", 1),
        (6, "delete", 2),
    ]


def test_cdf_column_mapping_historical_schema(spark, tmp_path):
    path = tmp_path / "cdf_rename"
    spark.createDataFrame([(1, "old")], "id LONG, value STRING").write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("delta.columnMapping.mode", "name").save(str(path))
    # A legal mapping rename preserves physical names and field IDs.
    metadata = next(action["metaData"] for action in _actions(path, 0) if "metaData" in action)
    schema = json.loads(metadata["schemaString"])
    schema["fields"][1]["name"] = "renamed"
    metadata["schemaString"] = json.dumps(schema)
    (path / "_delta_log" / "00000000000000000001.json").write_text(json.dumps({"metaData": metadata}) + "\n")
    spark.sql(f"UPDATE delta.`{path}` SET renamed = 'new' WHERE id = 1")  # noqa: S608
    assert [(row.id, row.value) for row in _feed(spark, path, 0, 0).collect()] == [(1, "old")]
    assert sorted((row.renamed, row["_change_type"]) for row in _feed(spark, path, 2, 2).collect()) == [
        ("new", "update_postimage"),
        ("old", "update_preimage"),
    ]
    with pytest.raises(Exception, match="incompatible schema"):
        _feed(spark, path, 0, 2).collect()


def test_cdf_merge_duplicate_deletes_and_by_source(spark, tmp_path):
    path = tmp_path / "cdf_merge_clauses"
    spark.createDataFrame([(1, "a"), (1, "a"), (2, "b"), (3, "c")], "id LONG, value STRING").coalesce(1).write.format(
        "delta"
    ).option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.sql(
        f"MERGE INTO delta.`{path}` t USING (SELECT 1L id UNION ALL SELECT 1L) s ON t.id = s.id WHEN MATCHED THEN DELETE"
    )
    assert _changes(spark, path, 1, 1) == [(1, "a", "delete"), (1, "a", "delete")]
    spark.sql(
        f"MERGE INTO delta.`{path}` t USING (SELECT 2L id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'"
    )
    assert _changes(spark, path, 2, 2) == [(3, "c", "update_preimage"), (3, "orphan", "update_postimage")]
    spark.sql(
        f"MERGE INTO delta.`{path}` t USING (SELECT 4L id, 'new' value) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
    )
    assert _changes(spark, path, 3, 3) == [(4, "new", "insert")]


def test_cdf_in_commit_timestamps(spark, tmp_path):
    path = tmp_path / "cdf_ict"
    spark.range(1).write.format("delta").option("delta.enableChangeDataFeed", "true").option(
        "delta.enableInCommitTimestamps", "true"
    ).save(str(path))
    spark.range(1, 2).write.format("delta").mode("append").save(str(path))
    timestamps = {}
    for version in range(2):
        timestamps[version] = next(
            action["commitInfo"]["inCommitTimestamp"] for action in _actions(path, version) if "commitInfo" in action
        )
        os.utime(path / "_delta_log" / f"{version:020}.json", (1, 1))
    for row in (
        _feed(spark, path).selectExpr("_commit_version", "unix_micros(_commit_timestamp) AS commit_micros").collect()
    ):
        assert row.commit_micros == timestamps[row["_commit_version"]] * 1000
    start = datetime.fromtimestamp(timestamps[1] / 1000, UTC).isoformat()
    assert [row.id for row in _feed(spark, path, None, startingTimestamp=start).collect()] == [1]


@pytest.mark.parametrize("operation", ["update", "merge", "replace"])
def test_cdf_rejects_volatile_rewrites_before_commit(spark, tmp_path, operation):
    path = tmp_path / "cdf_volatile"
    spark.createDataFrame([(1, "old")], "id LONG, value STRING").write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).save(str(path))

    def rewrite():
        if operation == "update":
            spark.sql(f"UPDATE delta.`{path}` SET value = uuid() WHERE id = 1")  # noqa: S608
        elif operation == "merge":
            spark.sql(
                f"MERGE INTO delta.`{path}` t USING (SELECT 1L id, uuid() value) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value"
            )
        else:
            spark.range(1, 2).selectExpr("id", "uuid() AS value").write.format("delta").mode("overwrite").option(
                "replaceWhere", "id = 1"
            ).save(str(path))

    with pytest.raises(Exception, match="non-deterministic expressions"):
        rewrite()
    assert not (path / "_delta_log" / "00000000000000000001.json").exists()
    assert [(row.id, row.value) for row in spark.read.format("delta").load(str(path)).collect()] == [(1, "old")]


def test_cdf_ignores_reorganization_actions(spark, tmp_path):
    path = tmp_path / "cdf_reorganize"
    spark.range(2).write.format("delta").option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.range(2).write.format("delta").mode("overwrite").save(str(path))
    actions = _actions(path, 1)
    for action in actions:
        for name in ["add", "remove"]:
            if name in action:
                action[name]["dataChange"] = False
    (path / "_delta_log" / "00000000000000000001.json").write_text(
        "".join(json.dumps(action) + "\n" for action in actions)
    )
    assert _feed(spark, path, 1, 1).collect() == []
    assert sorted(row.id for row in _feed(spark, path, 0, 100).collect()) == [0, 1]


def test_cdf_generated_columns(spark, tmp_path):
    path = tmp_path / "cdf_generated"
    spark.sql(f"""CREATE TABLE cdf_generated (id BIGINT, doubled BIGINT GENERATED ALWAYS AS (id * 2))
        USING DELTA LOCATION '{path}' TBLPROPERTIES ('delta.enableChangeDataFeed'='true')""")
    try:
        spark.sql("INSERT INTO cdf_generated (id) VALUES (1), (2)")
        spark.sql("UPDATE cdf_generated SET id = 3 WHERE id = 1")
        assert sorted(
            tuple(row) for row in _feed(spark, path, 2, 2).select("id", "doubled", "_change_type").collect()
        ) == [(1, 2, "update_preimage"), (3, 6, "update_postimage")]
    finally:
        spark.sql("DROP TABLE cdf_generated")
