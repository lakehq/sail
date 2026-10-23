import pyspark.sql.functions as F  # noqa: N812
import pytest


def test_monotonically_increasing_id_smoke(spark):
    rows = spark.sql("SELECT monotonically_increasing_id() AS id").collect()
    assert len(rows) == 1
    value = rows[0]["id"]
    assert isinstance(value, int)
    assert value >= 0


def test_monotonically_increasing_id_increases_within_partition(spark):
    # 10 rows in one partition (most likely single partition for this small query),
    # ids should be strictly increasing in output order.
    rows = spark.sql(
        """
        SELECT monotonically_increasing_id() AS id
        FROM range(0, 10)
        ORDER BY id
        """
    ).collect()
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


def test_monotonically_increasing_id_uses_input_partition_ids(spark):
    df = spark.range(0, 10, 1, 2).select(
        "*",
        F.spark_partition_id(),
        F.monotonically_increasing_id(),
    )

    assert df.columns == ["id", "SPARK_PARTITION_ID()", "monotonically_increasing_id()"]
    expected = [(i, 0, i) for i in range(5)] + [(i, 1, (1 << 33) + i - 5) for i in range(5, 10)]
    assert sorted((row[0], row[1], row[2]) for row in df.collect()) == expected


def test_monotonically_increasing_id_same_when_called_twice_in_select(spark):
    rows = spark.sql(
        """
        SELECT
          monotonically_increasing_id() AS id1,
          monotonically_increasing_id() AS id2
        FROM range(0, 10)
        """
    ).collect()
    assert rows, "expected non-empty result"
    for r in rows:
        assert r["id1"] == r["id2"]


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
def test_monotonically_increasing_id_in_aggregate_projection(spark):
    rows = spark.sql(
        """
        SELECT
          id,
          max(monotonically_increasing_id()) AS id1,
          max(monotonically_increasing_id()) AS id2
        FROM range(0, 10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["id1"], int)
        assert row["id1"] >= 0
        assert row["id1"] == row["id2"]


def test_monotonically_increasing_id_in_grouping_expression_projection(spark):
    rows = spark.sql(
        """
        SELECT max(id), monotonically_increasing_id() AS group_id
        FROM range(10)
        GROUP BY monotonically_increasing_id()
        ORDER BY group_id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    max_ids = sorted(row[0] for row in rows)
    group_ids = [row[1] for row in rows]

    assert max_ids == list(range(10))
    assert group_ids == sorted(group_ids)
    assert len(set(group_ids)) == len(group_ids)


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
def test_monotonically_increasing_id_in_aggregate_with_group_by_id(spark):
    rows = spark.sql(
        """
        SELECT id, max(monotonically_increasing_id())
        FROM range(10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    for index, row in enumerate(rows):
        assert row[0] == index
        assert isinstance(row[1], int)
        assert row[1] >= 0


def test_monotonically_increasing_id_inside_lateral_view_expression(spark):
    rows = spark.sql(
        """
        SELECT id, v
        FROM range(0, 10)
        LATERAL VIEW explode(array(monotonically_increasing_id())) AS v
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["v"], int)
        assert row["v"] >= 0


def test_monotonically_increasing_id_in_filter(spark):
    limit = 5
    df = spark.range(40, numPartitions=4)
    result = df.filter(F.monotonically_increasing_id() < limit)
    assert result.columns == ["id"]
    assert [row.id for row in result.collect()] == list(range(limit))


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("id % 2 = 0 AND monotonically_increasing_id() < 3", [0, 2, 4]),
        ("monotonically_increasing_id() < 3 AND id % 2 = 0", [0, 2]),
        ("id % 2 = 0 OR monotonically_increasing_id() < 3", [0, 1, 2, 3, 4, 5, 6, 8]),
        ("monotonically_increasing_id() < 3 OR id % 2 = 0", [0, 1, 2, 4, 6, 8]),
        ("(CASE WHEN id % 2 = 0 THEN NULL ELSE true END) AND monotonically_increasing_id() < 3", [1, 3, 5]),
        ("(CASE WHEN id % 2 = 0 THEN NULL ELSE false END) OR monotonically_increasing_id() < 3", [0, 1, 2]),
        (
            "(CASE WHEN id % 2 = 0 THEN monotonically_increasing_id() ELSE monotonically_increasing_id() END) < 3",
            [0, 1, 2, 3, 4, 5],
        ),
        (
            "monotonically_increasing_id() = (CASE WHEN id % 2 = 0 THEN monotonically_increasing_id() ELSE 100 END)",
            [0],
        ),
        ("coalesce(CASE WHEN id % 2 = 0 THEN NULL ELSE 100 END, monotonically_increasing_id()) < 3", [0, 2, 4]),
    ],
    ids=[
        "and",
        "and-reversed",
        "or",
        "or-reversed",
        "and-null",
        "or-null",
        "independent-branches",
        "independent-calls",
        "coalesce",
    ],
)
def test_monotonically_increasing_id_conditional_filter(spark, predicate, expected):
    result = spark.range(10, numPartitions=1).filter(predicate)
    assert [row.id for row in result.collect()] == expected


def test_monotonically_increasing_id_filter_state_across_batches_and_executions(spark):
    size, partitions = 80000, 4
    per_partition = size // partitions
    limit = 9000
    counter = F.monotonically_increasing_id() - F.spark_partition_id().cast("long") * (1 << 33)
    result = spark.range(size, numPartitions=partitions).filter(
        F.when(F.col("id") % 2 == 0, counter).otherwise(limit) < limit
    )
    expected = [partition * per_partition + 2 * i for partition in range(partitions) for i in range(limit)]
    assert sorted(row.id for row in result.collect()) == expected
    assert sorted(row.id for row in result.collect()) == expected


@pytest.mark.parametrize("global_sort", [False, True], ids=["local-sort", "global-sort"])
def test_monotonically_increasing_id_filter_keeps_input_sort(spark, global_sort):
    limit = 3
    df = spark.range(20000, numPartitions=1)
    ordered = df.orderBy(F.col("id").desc()) if global_sort else df.sortWithinPartitions(F.col("id").desc())
    result = ordered.filter(F.when(F.col("id") % 2 == 0, F.monotonically_increasing_id()).otherwise(100) < limit)
    assert [row.id for row in result.orderBy("id").collect()] == [19994, 19996, 19998]


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("id IN (SELECT id FROM range(0, 10, 2, 1)) AND monotonically_increasing_id() >= 0", [0, 2, 4, 6, 8]),
        ("id IN (SELECT id FROM range(0, 10, 2, 1)) OR monotonically_increasing_id() < 0", [0, 2, 4, 6, 8]),
        ("EXISTS (SELECT 1 FROM range(1)) AND monotonically_increasing_id() < 3", [0, 1, 2]),
        (
            "EXISTS (SELECT 1 FROM range(0, 10, 2, 1) u WHERE u.id = t.id) AND monotonically_increasing_id() >= 0",
            [0, 2, 4, 6, 8],
        ),
        ("monotonically_increasing_id() < (SELECT count(*) FROM range(3))", [0, 1, 2]),
    ],
    ids=["in", "in-or", "exists", "correlated-exists", "scalar"],
)
def test_monotonically_increasing_id_filter_with_subquery(spark, predicate, expected):
    result = spark.sql(f"SELECT id FROM range(0, 10, 1, 1) t WHERE {predicate}")  # noqa: S608
    assert sorted(row.id for row in result.collect()) == expected


def test_monotonically_increasing_id_separate_filters_keep_their_input(spark):
    inner_limit, outer_limit = 5, 2
    result = (
        spark.range(20, numPartitions=1)
        .filter(F.monotonically_increasing_id() < inner_limit)
        .filter(F.col("id") % 2 == 0)
        .filter(F.monotonically_increasing_id() < outer_limit)
    )
    assert [row.id for row in result.collect()] == [0, 2]


@pytest.mark.parametrize("sql", [False, True], ids=["dataframe", "sql"])
def test_monotonically_increasing_id_in_grouping(spark, sql):
    if sql:
        result = spark.sql(
            "SELECT monotonically_increasing_id() % 2 AS m, count(*) AS n "
            "FROM range(0, 40, 1, 4) GROUP BY monotonically_increasing_id() % 2"
        )
    else:
        df = spark.range(40, numPartitions=4)
        result = df.groupBy((F.monotonically_increasing_id() % 2).alias("m")).count()
    assert sorted(tuple(row) for row in result.collect()) == [(0, 20), (1, 20)]


def test_monotonically_increasing_id_in_sort(spark):
    df = spark.range(40, numPartitions=4)
    result = df.orderBy(F.monotonically_increasing_id().desc()).limit(5)
    assert result.columns == ["id"]
    assert [row.id for row in result.collect()] == list(range(39, 34, -1))


@pytest.mark.parametrize("global_sort", [False, True], ids=["local-sort", "global-sort"])
def test_monotonically_increasing_id_keeps_input_sort(spark, global_sort):
    size = 20000
    df = spark.range(size, numPartitions=4).select(((F.col("id") * 7919) % size).alias("k")).repartition(1)
    ordered = df.orderBy("k") if global_sort else df.sortWithinPartitions("k")
    indexed = ordered.withColumn("i", F.monotonically_increasing_id())
    assert indexed.filter(F.col("i") != F.col("k")).count() == 0
    assert indexed.agg(F.sum(F.when(F.col("i") == F.col("k"), 1).otherwise(0))).first()[0] == size


def test_monotonically_increasing_id_in_repartition(spark):
    df = spark.range(40, numPartitions=4).repartition(4, F.monotonically_increasing_id())
    assert df.columns == ["id"]
    assert sorted(row.id for row in df.collect()) == list(range(40))


def test_monotonically_increasing_id_keeps_sort_in_each_partition(spark):
    size, partitions = 20000, 4
    per_partition = size // partitions
    indexed = (
        spark.range(size, numPartitions=partitions)
        .sortWithinPartitions(F.col("id").desc())
        .select("id", F.spark_partition_id().alias("p"), F.monotonically_increasing_id().alias("i"))
    )
    expected = F.col("p") * (1 << 33) + per_partition - 1 - (F.col("id") % per_partition)
    matches = indexed.agg(F.sum(F.when(F.col("i") == expected, 1).otherwise(0))).first()[0]
    assert matches == size
