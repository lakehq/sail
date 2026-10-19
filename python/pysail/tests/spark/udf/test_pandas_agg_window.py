import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Window
from pyspark.sql.functions import PandasUDFType, pandas_udf

from pysail.testing.spark.utils.common import is_jvm_spark


def test_registered_pandas_grouped_agg_udf_as_window_function(spark):
    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v):
        return v.mean()

    spark.udf.register("mean_udf", mean_udf)
    df = spark.createDataFrame(
        [(1, 10.0), (1, 20.0), (2, 30.0), (2, 50.0)],
        schema="key long, value double",
    )
    df.createOrReplaceTempView("t_window_udaf")

    actual = spark.sql(
        "SELECT key, value, mean_udf(value) OVER (PARTITION BY key) AS m FROM t_window_udaf ORDER BY key, value"
    ).toPandas()
    expected = pd.DataFrame(
        {
            "key": [1, 1, 2, 2],
            "value": [10.0, 20.0, 30.0, 50.0],
            "m": [15.0, 15.0, 40.0, 40.0],
        }
    )
    assert_frame_equal(actual, expected)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail only: error message differs from JVM Spark")
def test_registered_pandas_scalar_udf_rejected_as_window_function(spark):
    @pandas_udf("string", PandasUDFType.SCALAR)
    def my_upper(s):
        return s.str.upper()

    spark.udf.register("my_upper_scalar", my_upper)
    df = spark.createDataFrame([("alice", 1)], schema="name string, dept long")
    df.createOrReplaceTempView("t_window_scalar")

    with pytest.raises(Exception, match="unknown window function"):
        spark.sql("SELECT my_upper_scalar(name) OVER (PARTITION BY dept) FROM t_window_scalar").collect()


@pytest.mark.parametrize("frame_type", ["rows", "range"])
def test_pandas_window_retraction_preserves_nulls_and_multiple_arguments(spark, frame_type):
    from pyspark.sql import functions as F  # noqa: N812

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def paired_sum(a, b):
        return float((a.fillna(0) + b.fillna(0)).sum())

    df = spark.createDataFrame(
        [(0, 1.0, 3.0), (1, None, 2.0), (2, 1.0, None), (8, 4.0, 5.0), (9, None, 6.0)],
        "id long, a double, b double",
    )
    window = Window.orderBy("id")
    window = window.rowsBetween(-1, 1) if frame_type == "rows" else window.rangeBetween(-1, 1)
    actual = df.select("id", paired_sum("a", "b").over(window).alias("value")).orderBy("id").collect()
    expected = (
        df.select("id", F.sum(F.coalesce("a", F.lit(0)) + F.coalesce("b", F.lit(0))).over(window).alias("value"))
        .orderBy("id")
        .collect()
    )
    assert actual == expected


@pytest.mark.parametrize("invocation", ["inline", "registered"])
def test_pandas_window_retracts_to_an_empty_range(spark, invocation):
    from pyspark.sql import functions as F  # noqa: N812

    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def frame_size(values):
        return len(values)

    df = spark.createDataFrame([(0,), (1,), (8,), (9,)], "id long")
    window = Window.orderBy("id").rangeBetween(-2, -1)
    if invocation == "registered":
        spark.udf.register("pandas_frame_size", frame_size)
        value = F.expr("pandas_frame_size(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING)")
    else:
        value = frame_size("id").over(window)
    actual = df.select("id", value.alias("n")).orderBy("id").collect()
    expected = df.select("id", F.count("id").over(window).alias("n")).orderBy("id").collect()
    assert actual == expected
