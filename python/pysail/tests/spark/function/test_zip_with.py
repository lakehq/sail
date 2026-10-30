from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812


def test_zip_with_pads_the_shorter_array_with_nulls(spark):
    df = spark.createDataFrame([(1, [1, 3, 5, 8], [0, 2, 4, 6])], ("id", "xs", "ys"))
    actual = df.select(F.zip_with("xs", "ys", lambda x, y: x**y).alias("powers")).collect()
    assert actual == [Row(powers=[1.0, 9.0, 625.0, 262144.0])]

    df = spark.createDataFrame([(1, ["foo", "bar"], [1, 2, 3])], ("id", "xs", "ys"))
    actual = df.select(F.zip_with("xs", "ys", lambda x, y: F.concat_ws("_", x, y)).alias("xs_ys")).collect()
    assert actual == [Row(xs_ys=["foo_1", "bar_2", "3"])]


def test_zip_with_captures_columns_and_uses_any_parameter(spark):
    df = spark.createDataFrame([(10, [1, 2], [3, 4, 5]), (20, [6], [])], ("id", "xs", "ys"))
    actual = df.select(F.zip_with("xs", "ys", lambda x, _y: x + F.col("id")).alias("v")).collect()
    assert actual == [Row(v=[11, 12, None]), Row(v=[26])]
    actual = spark.sql("SELECT zip_with(array(1, 2), array(3, 4, 5), (x, y) -> y) AS v").collect()
    assert actual == [Row(v=[3, 4, 5])]


def test_zip_with_null_array(spark):
    actual = spark.sql("SELECT zip_with(CAST(NULL AS ARRAY<INT>), array(1), (x, y) -> y) AS v").collect()
    assert actual == [Row(v=None)]


def test_map_zip_with_merges_the_keys_of_both_maps(spark):
    df = spark.createDataFrame(
        [(1, {"IT": 24.0, "SALES": 12.00}, {"IT": 2.0, "SALES": 1.4})],
        ("id", "base", "ratio"),
    )
    actual = df.select(F.map_zip_with("base", "ratio", lambda _k, v1, v2: F.round(v1 * v2, 2)).alias("data")).collect()
    assert actual == [Row(data={"IT": 48.0, "SALES": 16.8})]

    query = """
        SELECT map_keys(m) AS k, map_values(m) AS v FROM (
            SELECT map_zip_with(map(1, 'a', 2, 'b'), map(3, 'y', 1, 'x'), (k, v1, v2) -> concat_ws('+', v1, v2)) AS m
        )
    """
    assert spark.sql(query).collect() == [Row(k=[1, 2, 3], v=["a+x", "b", "y"])]


def test_map_zip_with_null_map(spark):
    query = "SELECT map_zip_with(CAST(NULL AS MAP<INT, INT>), map(1, 2), (k, v1, v2) -> v2) AS m"
    assert spark.sql(query).collect() == [Row(m=None)]
