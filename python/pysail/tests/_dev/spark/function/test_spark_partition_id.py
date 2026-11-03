import pyspark.sql.functions as F  # noqa: N812
import pytest


def test_spark_partition_id_smoke(spark):
    rows = spark.sql("SELECT spark_partition_id() AS pid").collect()
    assert len(rows) == 1
    value = rows[0]["pid"]
    assert isinstance(value, int)
    assert value >= 0


def test_spark_partition_id_constant_within_single_partition(spark):
    # `range(..., numPartitions=1)` forces one partition; all rows must
    # share the same partition id (which is 0).
    rows = spark.range(0, 10, numPartitions=1).selectExpr("id", "spark_partition_id() AS pid").collect()
    assert len(rows) == 10  # noqa: PLR2004
    pids = {row["pid"] for row in rows}
    assert pids == {0}


def test_spark_partition_id_distinct_across_partitions(spark):
    # With multiple partitions, `spark_partition_id` should produce
    # all partition indices in `[0, numPartitions)`.
    num_partitions = 4
    rows = spark.range(0, 100, numPartitions=num_partitions).selectExpr("spark_partition_id() AS pid").collect()
    pids = {row["pid"] for row in rows}
    assert pids == set(range(num_partitions))


def test_spark_partition_id_same_when_called_twice_in_select(spark):
    rows = spark.sql(
        """
        SELECT
          spark_partition_id() AS pid1,
          spark_partition_id() AS pid2
        FROM range(0, 10)
        """
    ).collect()
    assert rows, "expected non-empty result"
    for r in rows:
        assert r["pid1"] == r["pid2"]


def test_spark_partition_id_inside_lateral_view_expression(spark):
    rows = spark.sql(
        """
        SELECT id, v
        FROM range(0, 10)
        LATERAL VIEW explode(array(spark_partition_id())) AS v
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["v"], int)
        assert row["v"] >= 0


def test_spark_partition_id_return_type_is_int(spark):
    df = spark.sql("SELECT spark_partition_id() AS pid")
    [field] = df.schema.fields
    assert field.name == "pid"
    # Spark's `spark_partition_id` returns IntegerType (32-bit signed int).
    assert field.dataType.typeName() == "integer"


def test_spark_partition_id_in_filter(spark):
    partition = 2
    df = spark.range(40, numPartitions=4)
    filtered = df.filter(F.spark_partition_id() == partition)
    assert filtered.columns == ["id"]
    assert sorted(row.id for row in filtered.collect()) == list(range(20, 30))


@pytest.mark.parametrize("sql", [False, True], ids=["dataframe", "sql"])
def test_spark_partition_id_in_grouping(spark, sql):
    if sql:
        result = spark.sql(
            "SELECT spark_partition_id() + 10 AS p, count(*) AS n "
            "FROM range(0, 40, 1, 4) GROUP BY spark_partition_id() + 10"
        )
    else:
        df = spark.range(40, numPartitions=4)
        result = df.groupBy((F.spark_partition_id() + 10).alias("p")).count()
    assert sorted(tuple(row) for row in result.collect()) == [(i + 10, 10) for i in range(4)]


@pytest.mark.parametrize("global_sort", [False, True], ids=["local-sort", "global-sort"])
def test_spark_partition_id_in_sort(spark, global_sort):
    df = spark.range(40, numPartitions=4)
    sort = df.orderBy if global_sort else df.sortWithinPartitions
    result = sort(F.spark_partition_id().desc(), F.col("id").desc())
    assert result.columns == ["id"]
    rows = [row.id for row in result.collect()]
    assert sorted(rows) == list(range(40))
    if global_sort:
        assert rows == list(range(39, -1, -1))


def test_spark_partition_id_in_repartition(spark):
    df = spark.range(40, numPartitions=4).repartition(4, F.spark_partition_id())
    assert df.columns == ["id"]
    assert sorted(row.id for row in df.collect()) == list(range(40))


def test_spark_partition_id_grouping_reuses_value_in_having(spark):
    result = spark.sql(
        "SELECT spark_partition_id() AS p, count(*) AS n "
        "FROM range(0, 40, 1, 4) GROUP BY spark_partition_id() HAVING p >= 2"
    )
    assert sorted(tuple(row) for row in result.collect()) == [(2, 10), (3, 10)]


@pytest.mark.parametrize("function", ["spark_partition_id", "monotonically_increasing_id"])
def test_partition_function_in_having(spark, function):
    result = spark.sql(f"SELECT count(*) FROM range(40) HAVING {function}() = 0")  # noqa: S608
    assert result.collect() == [(40,)]


@pytest.mark.parametrize("function", [F.spark_partition_id, F.monotonically_increasing_id])
def test_partition_function_rejected_inside_aggregate(spark, function):
    with pytest.raises(Exception, match=r"(?i)non.?deterministic"):
        spark.range(40).agg(F.max(function())).collect()
