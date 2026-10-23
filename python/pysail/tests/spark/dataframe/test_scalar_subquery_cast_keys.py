import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize(
    ("boundary", "counts"),
    [
        ("alias", (2, 1, 0)),
        ("double_alias", (2, 1, 0)),
        ("cross", (6, 3, 0)),
        ("inner", (2, 0, 0)),
        ("left", (2, 1, 0)),
        ("left_semi", (2, 0, 0)),
        ("left_anti", (0, 1, 0)),
        ("right_inner", (2, 0, 0)),
        ("right", (2, 1, 0)),
        ("right_cross", (6, 3, 0)),
    ],
)
def test_cast_correlation_through_alias_and_join(spark, boundary, counts):
    outer = spark.sql("SELECT * FROM VALUES (0L), (0L), (1L), (2L) AS o(id)").alias("o")
    inner = spark.sql("SELECT * FROM VALUES ('0', 2), ('00', 3), ('1', 4) AS i(k, v)").alias("i")
    matching = inner.where(F.col("i.k").cast("long") == F.col("o.id").outer())
    if boundary == "alias":
        matching = matching.alias("a")
    elif boundary == "double_alias":
        matching = matching.alias("a").alias("b")
    else:
        other = spark.sql("SELECT * FROM VALUES (2), (3), (7) AS j(w)")
        condition = F.col("v") == F.col("w")
        if boundary == "cross":
            matching = matching.crossJoin(other)
        elif boundary == "right_cross":
            matching = other.crossJoin(matching)
        elif boundary in ("right_inner", "right"):
            matching = other.join(matching, condition, "inner" if boundary == "right_inner" else "right")
        else:
            matching = matching.join(other, condition, boundary)
    result = outer.select("id", matching.select(F.count("*")).scalar().alias("n")).orderBy("id")
    assert [tuple(row) for row in result.collect()] == [(0, counts[0]), (0, counts[0]), (1, counts[1]), (2, counts[2])]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize("cast_below_window", [True, False])
def test_cast_correlation_keeps_aliased_window_partition_keys(spark, cast_below_window):
    outer = spark.sql("SELECT * FROM VALUES (0L, 10), (0L, 20), (1L, 10), (2L, 10) AS o(id, g)").alias("o")
    inner = spark.sql(
        "SELECT * FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5) AS i(k, g, v)"
    ).alias("i")
    matching = inner.where(F.col("i.g") == F.col("o.g").outer())
    if cast_below_window:
        matching = matching.where(F.col("i.k").cast("long") == F.col("o.id").outer())
    matching = matching.alias("a").withColumn("w", F.row_number().over(Window.orderBy("v")))
    if not cast_below_window:
        matching = matching.where(F.col("a.k").cast("long") == F.col("o.id").outer())
    result = outer.select("id", "g", matching.select(F.max("w")).scalar().alias("n")).orderBy("id", "g")
    assert [tuple(row) for row in result.collect()] == [
        (0, 10, 2),
        (0, 20, 2),
        (1, 10, 1 if cast_below_window else 3),
        (2, 10, None),
    ]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
def test_cast_correlation_keeps_shared_dataframe_aliases_distinct(spark):
    source = spark.sql("SELECT * FROM VALUES ('0', 1), ('00', 2), ('1', 3) AS t(k, v)")
    outer = source.select(F.col("k").cast("long").alias("id")).alias("o")
    matching = source.alias("i").where(F.col("i.k").cast("long") == F.col("o.id").outer()).alias("a")
    matching = matching.withColumn("w", F.row_number().over(Window.orderBy("v")))
    result = outer.select("id", matching.select(F.max("w")).scalar().alias("n")).orderBy("id")
    assert result.columns == ["id", "n"]
    assert [tuple(row) for row in result.collect()] == [(0, 2), (0, 2), (1, 1)]
