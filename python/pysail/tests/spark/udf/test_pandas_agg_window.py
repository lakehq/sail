import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
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
