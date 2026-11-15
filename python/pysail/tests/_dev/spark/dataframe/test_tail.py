"""`DataFrame.tail`, which returns the last rows of one evaluation of the input
(Spark's `CollectTailExec`)."""

import random

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import functions as sf


@pytest.mark.parametrize(
    ("num", "expected"),
    [(0, []), (3, [7, 8, 9]), (20, list(range(10))), (2147483647, list(range(10)))],
    ids=["zero", "some", "more-than-rows", "max-int"],
)
def test_tail(spark, num, expected):
    assert [r.id for r in spark.range(0, 10, 1, 1).tail(num)] == expected
    assert [r.id for r in spark.range(0, 10, 1, 3).orderBy("id").tail(num)] == expected


@pytest.mark.parametrize("num", [3, 20])
def test_tail_of_partitions(spark, num):
    rows = [r.id for r in spark.range(0, 10, 1, 3).tail(num)]
    assert len(rows) == min(num, 10)
    assert set(rows) <= set(range(10))


def test_negative_tail(spark):
    with pytest.raises(PySparkException, match="must be equal to or greater than 0"):
        spark.range(10).tail(-1)


def test_tail_of_empty_input(spark):
    assert spark.range(0).tail(3) == []
    assert spark.range(0, 10, 1, 3).where("id < 0").tail(3) == []


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark Connect evaluates scalar subqueries for tail(0)")
def test_zero_tail_does_not_evaluate_subqueries(spark):
    # This matches `limit(0)` and the classic Spark API (`executeTail(0)`). Spark Connect
    # still prepares the scalar subqueries of the input of a zero tail and fails.
    df = spark.sql("SELECT id, (SELECT id FROM range(2)) AS x FROM range(3)")
    assert df.tail(0) == []
    with pytest.raises(PySparkException, match=r"(?i)more than one row"):
        df.tail(1)


def test_tail_of_sorted_input(spark):
    df = spark.range(0, 1000, 1, 4).orderBy("id", ascending=False)
    assert [r.id for r in df.tail(3)] == [2, 1, 0]


@pytest.mark.parametrize("partitions", [1, 2, 4])
@pytest.mark.parametrize("projection", ["drop", "alias", "reorder", "computed", "chain", "filter"])
def test_tail_preserves_sort_with_projected_key(spark, partitions, projection):
    df = spark.sql(
        "SELECT * FROM VALUES (0, 'a', 'left'), (1, 'b', 'middle'), (2, 'c', 'right') AS t(k, payload, marker)"
    )
    df = df.repartition(partitions)
    ordered = df.orderBy("k", ascending=False)
    if projection == "drop":
        projected = ordered.select("payload")
        columns = ["payload"]
        expected = [("c",), ("b",), ("a",)]
    elif projection == "alias":
        projected = ordered.selectExpr("payload AS value")
        columns = ["value"]
        expected = [("c",), ("b",), ("a",)]
    elif projection == "reorder":
        projected = ordered.select("marker", "payload")
        columns = ["marker", "payload"]
        expected = [("right", "c"), ("middle", "b"), ("left", "a")]
    elif projection == "computed":
        projected = (
            df.selectExpr("k * k + 1 AS sort_key", "payload")
            .orderBy("sort_key", ascending=False)
            .selectExpr("upper(payload) AS value")
        )
        columns = ["value"]
        expected = [("C",), ("B",), ("A",)]
    elif projection == "filter":
        projected = ordered.where("payload != 'b'").select("payload")
        columns = ["payload"]
        expected = [("c",), ("a",)]
    else:
        projected = (
            ordered.selectExpr("k AS hidden", "payload AS first", "marker")
            .selectExpr("marker", "concat(first, '!') AS second")
            .selectExpr("second AS value")
        )
        columns = ["value"]
        expected = [("c!",), ("b!",), ("a!",)]
    assert projected.columns == columns
    for count in [1, 2, 4]:
        assert [tuple(row) for row in projected.tail(count)] == expected[-count:]


@pytest.mark.parametrize("partitions", [1, 4])
def test_tail_preserves_hidden_sort_over_multiple_batches(spark, partitions):
    # This exceeds the default batch size so projection parallelization cannot
    # be tested only on inputs that fit in one batch.
    df = (
        spark.range(0, 20000, 1, partitions)
        .orderBy("id", ascending=False)
        .selectExpr("concat('row-', cast(id as string)) AS payload")
    )
    for count in [17, 9000]:
        assert [row.payload for row in df.tail(count)] == [f"row-{i}" for i in range(count - 1, -1, -1)]


@pytest.mark.parametrize("partitions", [1, 4])
def test_tail_preserves_hidden_tie_breaker_over_multiple_batches(spark, partitions):
    # The rows that tie on the remaining sort key keep the order of the hidden one.
    df = (
        spark.range(0, 20000, 1, partitions)
        .withColumn("k", sf.col("id") % 3)
        .orderBy("k", sf.col("id").desc())
        .selectExpr("k", "cast(id as string) AS payload")
    )
    expected = sorted(((i % 3, i) for i in range(20000)), key=lambda row: (row[0], -row[1]))
    for count in [5, 9000]:
        assert [(row.k, int(row.payload)) for row in df.tail(count)] == expected[-count:]


@pytest.mark.parametrize("partitions", [1, 4])
def test_tail_preserves_hidden_sort_expression_and_limit(spark, partitions):
    df = spark.range(0, 20000, 1, partitions)
    by_expression = df.orderBy((df.id % 1000).desc(), df.id.asc()).selectExpr("cast(id as string) AS payload")
    assert [row.payload for row in by_expression.tail(3)] == ["17000", "18000", "19000"]
    limited = df.orderBy(df.id.desc()).limit(15000).selectExpr("cast(id as string) AS payload")
    assert [row.payload for row in limited.tail(3)] == ["5002", "5001", "5000"]


@pytest.mark.parametrize("partitions", [1, 4])
def test_tail_preserves_hidden_random_sort(spark, partitions):
    ordered = spark.range(0, 20000, 1, partitions).orderBy(sf.rand(7))
    expected = [str(row.id) for row in ordered.collect()][-17:]
    df = ordered.selectExpr("cast(id as string) AS payload")
    assert [row.payload for row in df.tail(17)] == expected


def _row_local_path(spark, path):
    # The inputs exceed the default batch size, so that the operators between the
    # order and Tail could be parallelized.
    ordered = spark.range(0, 20000, 1, 4).orderBy(sf.col("id").desc())
    if path == "single-partition":
        return spark.range(0, 20000, 1, 1)
    if path == "sort-within-partitions":
        return spark.range(0, 20000, 1, 1).sortWithinPartitions(sf.col("id").desc())
    if path == "rand":
        return ordered.withColumn("r", sf.rand(7)).where("r < 2")
    if path == "monotonic-id":
        return ordered.withColumn("m", sf.monotonically_increasing_id())
    if path == "partition-id":
        return ordered.withColumn("m", sf.spark_partition_id())
    if path == "explode":
        return ordered.select(sf.explode(sf.array("id", "id")).alias("id"))
    if path == "left-semi-join":
        return ordered.join(spark.range(0, 20000, 1, 3).where("id % 2 = 0"), "id", "left_semi")
    if path == "offset":
        return ordered.limit(15000).offset(5)
    ordered.createOrReplaceTempView("tail_ordered")
    return spark.sql("SELECT * FROM (SELECT * FROM tail_ordered ORDER BY id DESC) AS s")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("single-partition", ["19996", "19997", "19998", "19999"]),
        ("sort-within-partitions", ["3", "2", "1", "0"]),
        ("rand", ["3", "2", "1", "0"]),
        ("monotonic-id", ["3", "2", "1", "0"]),
        ("partition-id", ["3", "2", "1", "0"]),
        ("explode", ["1", "1", "0", "0"]),
        ("left-semi-join", ["6", "4", "2", "0"]),
        ("offset", ["5003", "5002", "5001", "5000"]),
        ("subquery", ["3", "2", "1", "0"]),
    ],
)
def test_tail_preserves_order_through_row_local_operators(spark, path, expected):
    df = _row_local_path(spark, path).selectExpr("cast(id as string) AS payload")
    assert [row.payload for row in df.tail(4)] == expected


def test_tail_preserves_order_through_sample(spark):
    ordered = spark.range(0, 20000, 1, 4).orderBy(sf.col("id").desc()).sample(fraction=0.5, seed=3)
    expected = [str(row.id) for row in ordered.tail(17)]
    df = ordered.selectExpr("cast(id as string) AS payload")
    assert [row.payload for row in df.tail(17)] == expected


def test_tail_evaluates_projected_rows_outside_the_suffix(spark):
    # The failing row is outside tail(1), so postponing this projection until
    # after Tail would incorrectly suppress its error. One partition also makes
    # Spark's take-from-end execution read the entire child partition.
    df = (
        spark.range(0, 20000, 1, 1)
        .orderBy("id", ascending=False)
        .selectExpr(
            "CASE WHEN id = 10000 THEN raise_error('tail projection evaluated') ELSE cast(id as string) END AS payload"
        )
    )
    with pytest.raises(PySparkException, match="tail projection evaluated"):
        df.tail(1)


def test_tail_of_sorted_files(spark, tmp_path):
    path = str(tmp_path / "data")
    spark.range(0, 40000, 1, 4).write.parquet(path)
    df = spark.read.parquet(path).orderBy("id")
    assert [r.id for r in df.tail(5)] == list(range(39995, 40000))


@pytest.mark.parametrize("seed", [1729, 9001, 20261113])
def test_tail_of_seeded_nested_rows(spark, seed):
    """A stable total order gives a Python oracle for nullable, nested payloads after
    repartitioning. Counts cut both inside and beyond the retained rows."""
    rng = random.Random(seed)  # noqa: S311
    rows = [
        (
            i,
            rng.choice([None, -2, -1, 0, 1, 2]),
            None if i % 7 == 0 else (rng.choice([None, "", "é", "x" * 80]), [i, None, -i]),
            None if i % 5 == 0 else {"a": i, "b": None},
            None if i % 3 == 0 else bytes([i, 255 - i]),
        )
        for i in range(37)
    ]
    rng.shuffle(rows)
    df = spark.createDataFrame(
        rows,
        "id INT, k INT, payload STRUCT<label:STRING,items:ARRAY<INT>>, tags MAP<STRING,INT>, raw BINARY",
    ).repartition(3)
    ordered = df.orderBy(df.k.asc_nulls_last(), df.id.desc())
    ordered_rows = sorted(rows, key=lambda row: (row[1] is None, row[1] or 0, -row[0]))
    expected = ordered.collect()
    assert [row.id for row in expected] == [row[0] for row in ordered_rows]
    projected = ordered.select("raw", "payload", "tags")
    projected_expected = [
        {
            "raw": row[4],
            "payload": None if row[2] is None else {"label": row[2][0], "items": row[2][1]},
            "tags": row[3],
        }
        for row in ordered_rows
    ]
    # Keeping only the first sort key must also preserve the hidden tie-breaker
    # within duplicate and null-key groups.
    prefix_projected = ordered.select("k", "raw", "payload", "tags")
    prefix_expected = [{"k": row[1], **projected_expected[index]} for index, row in enumerate(ordered_rows)]
    for count in [0, 1, 17, len(rows), len(rows) + 1]:
        assert ordered.tail(count) == (expected[-count:] if count else [])
        assert [row.asDict(recursive=True) for row in projected.tail(count)] == (
            projected_expected[-count:] if count else []
        )
        assert [row.asDict(recursive=True) for row in prefix_projected.tail(count)] == (
            prefix_expected[-count:] if count else []
        )


def test_tail_returns_the_last_rows_when_the_row_count_is_random(spark):
    # The window columns and rows come from the same evaluation, so the last rows
    # must be numbered total - 2 through total even when the input size changes.
    df = spark.sql(
        "SELECT id, row_number() OVER (ORDER BY id) AS rn, count(*) OVER () AS total "
        "FROM range(100) WHERE uuid() < '8' ORDER BY id"
    )
    for _ in range(5):
        for candidate in [df, df.select("rn", "total")]:
            rows = candidate.tail(3)
            assert len(rows) == 3, rows  # noqa: PLR2004
            total = rows[0].total
            assert [r.rn for r in rows] == [total - 2, total - 1, total], rows
