import math

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType

# Runs alongside the higher-order function suite (`-m "transform or exists"`),
# which is where the name-shadowing regression lives.
pytestmark = [pytest.mark.transform, pytest.mark.exists]


def test_registered_udf_shadows_higher_order_function(spark):
    """A user-registered scalar UDF whose name collides with a built-in
    higher-order function (e.g. ``transform``) must shadow the built-in.

    ``transform(1, 2)`` has two non-lambda arguments, so the higher-order
    resolution path must not intercept it and reject it as a malformed
    lambda call; the registered UDF is invoked instead.

    The same UDF then runs per row over a column of ten records that mixes
    NULL arguments (propagated to NULL) with Int32 overflow/underflow (which
    wraps on the cast back to ``IntegerType``), both through the object
    reference and through the shadowed SQL name.
    """

    @udf(IntegerType())
    def transform(a, b):
        if a is None or b is None:
            return None
        return a + b

    spark.udf.register("transform", transform)

    assert spark.sql("SELECT transform(1, 2) AS r").collect() == [Row(r=3)]

    rows = [
        Row(id=0, a=1, b=2),
        Row(id=1, a=10, b=-3),
        Row(id=2, a=None, b=5),  # null argument
        Row(id=3, a=7, b=None),  # null argument
        Row(id=4, a=None, b=None),  # both null
        Row(id=5, a=0, b=0),
        Row(id=6, a=2147483647, b=1),  # Int32 overflow wraps
        Row(id=7, a=-2147483648, b=-1),  # Int32 underflow wraps
        Row(id=8, a=100, b=200),
        Row(id=9, a=-50, b=50),
    ]
    expected = [3, 7, None, None, None, 0, -2147483648, 2147483647, 300, 0]
    df = spark.createDataFrame(rows, schema="id int, a int, b int")

    by_object = df.select(col("id"), transform(col("a"), col("b")).alias("r")).sort("id").collect()
    assert [row.r for row in by_object] == expected

    df.createOrReplaceTempView("shadow_overflow_null")
    by_sql = spark.sql("SELECT transform(a, b) AS r FROM shadow_overflow_null ORDER BY id").collect()
    assert [row.r for row in by_sql] == expected


def test_shadowing_udf_runs_over_a_column_with_null_and_nan(spark):
    """The shadowing UDF is a real scalar function: it runs per row over a
    column that contains NULL and NaN, both through the object reference and
    through the SQL name (which is what the resolver's shadowing gate covers).
    """

    @udf(StringType())
    def exists(x):
        if x is None:
            return "null"
        if math.isnan(x):
            return "nan"
        return f"{x:g}"

    spark.udf.register("exists", exists)

    values = [1.0, 2.5, 3.0, float("nan"), None, -5.0, 0.0, 100.0, float("nan"), None]
    expected = ["1", "2.5", "3", "nan", "null", "-5", "0", "100", "nan", "null"]
    df = spark.createDataFrame(
        [Row(id=i, v=v) for i, v in enumerate(values)],
        schema="id int, v double",
    )

    by_object = df.select(col("id"), exists(col("v")).alias("r")).sort("id").collect()
    assert [row.r for row in by_object] == expected

    df.createOrReplaceTempView("shadow_nan_null")
    by_sql = spark.sql("SELECT exists(v) AS r FROM shadow_nan_null ORDER BY id").collect()
    assert [row.r for row in by_sql] == expected
