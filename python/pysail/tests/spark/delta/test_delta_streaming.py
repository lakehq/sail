# ruff: noqa: S608

import time
from collections import Counter

import pytest
from pyspark.sql import functions as sf

from pysail.testing.spark.utils.common import is_jvm_spark

if is_jvm_spark():
    pytest.skip("Sail dynamic-table streaming extensions", allow_module_level=True)


def _stream(spark, path, *, start=0, end=None, apply_changes=True):
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    if apply_changes:
        reader = reader.option("applyChanges", "true")
    if start is not None:
        reader = reader.option("startingVersion", start)
    if end is not None:
        reader = reader.option("endingVersion", end)
    return reader.load(str(path))


def _aggregate(df, *, grouped=True):
    groups = df.groupBy("g") if grouped else df.groupBy()
    return groups.agg(
        sf.count("*").alias("rows"),
        sf.count("v").alias("values"),
        sf.sum("v").alias("sum"),
        sf.avg("v").alias("avg"),
        sf.min("v").alias("min"),
        sf.max("v").alias("max"),
        sf.count_distinct("v").alias("distinct"),
    )


def _assert_snapshot(spark, path, version, *, start=0, grouped=True):
    actual = _aggregate(_stream(spark, path, start=start, end=version), grouped=grouped)
    expected = _aggregate(spark.read.format("delta").option("versionAsOf", version).load(str(path)), grouped=grouped)
    assert actual.schema == expected.schema
    assert Counter(map(tuple, actual.collect())) == Counter(map(tuple, expected.collect()))


@pytest.mark.parametrize("deletion_vectors", [False, True])
@pytest.mark.parametrize("column_mapping", ["none", "name"])
def test_delta_streaming_aggregate_changes(spark, tmp_path, deletion_vectors, column_mapping):
    path = tmp_path / "aggregate"
    (
        spark.createDataFrame(
            [(1, "a", 10), (2, "a", 20), (3, "b", None), (4, "b", 5), (5, "b", 5), (6, None, 7)],
            "id LONG, g STRING, v LONG",
        )
        .repartition(2)
        .write.format("delta")
        .partitionBy("g")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.enableDeletionVectors", str(deletion_vectors).lower())
        .option("delta.columnMapping.mode", column_mapping)
        .save(str(path))
    )
    target = f"delta.`{path}`"
    statements = [
        f"UPDATE {target} SET g = 'b', v = 30 WHERE id = 1",
        f"DELETE FROM {target} WHERE id = 4",
        f"DELETE FROM {target} WHERE id = 5",
        f"DELETE FROM {target} WHERE id = 2",
        None,
        f"MERGE INTO {target} t USING (SELECT 7L id) s ON t.id = s.id WHEN MATCHED THEN DELETE",
    ]
    _assert_snapshot(spark, path, 0)
    for version, statement in enumerate(statements, start=1):
        if statement is None:
            spark.createDataFrame([(7, "a", 11), (7, "a", 11)], "id LONG, g STRING, v LONG").write.format("delta").mode(
                "append"
            ).save(str(path))
        else:
            spark.sql(statement)
        _assert_snapshot(spark, path, version)
    _assert_snapshot(spark, path, len(statements), start=2)
    _assert_snapshot(spark, path, len(statements), start=None)
    _assert_snapshot(spark, path, len(statements), grouped=False)
    # Projection and filtering must preserve the hidden change flags.
    actual = _stream(spark, path, end=len(statements)).where("v > 6").selectExpr("g", "v * 2 AS v")
    expected = spark.read.format("delta").load(str(path)).where("v > 6").selectExpr("g", "v * 2 AS v")
    assert Counter(map(tuple, _aggregate(actual).collect())) == Counter(map(tuple, _aggregate(expected).collect()))
    filtered_sum = sf.expr("sum(v) FILTER (WHERE v > 10)").alias("total")
    assert Counter(map(tuple, actual.groupBy("g").agg(filtered_sum).collect())) == Counter(
        map(tuple, expected.groupBy("g").agg(filtered_sum).collect())
    )
    # A marker has placeholder zeroes for non-nullable aggregate results.
    # Data expressions must not evaluate those placeholders.
    actual_counts = actual.groupBy("g").count().where("10 / count > 0").selectExpr("g", "10 / count AS ratio")
    expected_counts = expected.groupBy("g").count().where("10 / count > 0").selectExpr("g", "10 / count AS ratio")
    assert Counter(map(tuple, actual_counts.collect())) == Counter(map(tuple, expected_counts.collect()))


def test_delta_streaming_empty_and_duplicate_rows(spark, tmp_path):
    path = tmp_path / "empty"
    spark.createDataFrame([], "g STRING, v LONG").write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).save(str(path))
    _assert_snapshot(spark, path, 0)
    _assert_snapshot(spark, path, 0, grouped=False)
    spark.createDataFrame([("a", 1), ("a", 1), ("a", None)], "g STRING, v LONG").write.format("delta").mode(
        "append"
    ).save(str(path))
    expected_rows = [("a", 1), ("a", 1), ("a", None)]
    assert _stream(spark, path, end=1).count() == len(expected_rows)
    assert Counter(map(tuple, _stream(spark, path, end=1).collect())) == Counter(expected_rows)
    spark.sql(f"DELETE FROM delta.`{path}`")
    _assert_snapshot(spark, path, 2)
    _assert_snapshot(spark, path, 2, grouped=False)
    assert _stream(spark, path, end=2).collect() == []
    assert _stream(spark, path, end=2).count() == 0


def test_delta_streaming_cdf_events_remain_append_only(spark, tmp_path):
    path = tmp_path / "events"
    spark.range(3).write.format("delta").option("delta.enableChangeDataFeed", "true").save(str(path))
    spark.sql(f"UPDATE delta.`{path}` SET id = 10 WHERE id = 1")
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id = 2")
    batch = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(str(path))
    stream = _stream(spark, path, end=2, apply_changes=False)
    assert stream.schema == batch.schema
    assert Counter(map(tuple, stream.collect())) == Counter(map(tuple, batch.collect()))
    assert Counter(map(tuple, stream.groupBy("_change_type").count().collect())) == Counter(
        map(tuple, batch.groupBy("_change_type").count().collect())
    )
    assert sorted(row.id for row in _stream(spark, path, start=None, end=2, apply_changes=False).collect()) == [0, 10]


def test_delta_streaming_rejects_unsafe_operations(spark, tmp_path):
    path = tmp_path / "invalid"
    spark.range(2).write.format("delta").option("delta.enableChangeDataFeed", "true").save(str(path))
    with pytest.raises(Exception, match="applyChanges requires a streaming"):
        spark.read.format("delta").option("applyChanges", "true").load(str(path)).collect()
    with pytest.raises(Exception, match="LIMIT is not supported on retractable streams"):
        _stream(spark, path, end=0).limit(1).collect()
    with pytest.raises(Exception, match="volatile projections"):
        _stream(spark, path, end=0).select(sf.rand()).collect()
    with pytest.raises(Exception, match="version boundaries"):
        spark.readStream.format("delta").option("readChangeFeed", "true").option(
            "startingTimestamp", "2026-01-01"
        ).load(str(path)).collect()


def test_delta_streaming_tails_new_commits(spark, tmp_path, capfd):
    path = tmp_path / "tail"
    spark.createDataFrame([("a", 4242)], "g STRING, v LONG").write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).save(str(path))
    query = _stream(spark, path, start=None).groupBy("g").sum("v").writeStream.format("console").start()
    output = ""

    def wait_for(text):
        nonlocal output
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            output += capfd.readouterr().out
            if text in output:
                return
            assert query.isActive, str(query.exception())
            time.sleep(0.05)
        pytest.fail(f"stream did not emit {text!r}: {output}")

    try:
        wait_for("4242")
        output = ""
        spark.sql(f"UPDATE delta.`{path}` SET v = 8484")
        wait_for("8484")
        assert "true" in output  # The old aggregate is retracted.
        assert "4242" in output
        output = ""
        spark.sql(f"DELETE FROM delta.`{path}`")
        wait_for("8484")
        assert "true" in output
    finally:
        query.stop()
