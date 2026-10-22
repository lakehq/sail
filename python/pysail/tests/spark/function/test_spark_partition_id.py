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
