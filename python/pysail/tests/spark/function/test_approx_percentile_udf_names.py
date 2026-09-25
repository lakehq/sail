from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812


def test_percentile_named_scalar_udf_does_not_require_foldable_parameters(spark):
    # The module-scoped session isolates these registrations from built-in tests.
    spark.udf.register("percentile_approx", lambda value, percentages: value + len(percentages), "int")
    result = spark.sql("SELECT percentile_approx(1, array_compact(array(0D, NULL, 1D))) AS p")
    expected = 3
    assert result.first().p == expected


def test_percentile_named_window_udaf_does_not_require_foldable_parameters(spark):
    @F.pandas_udf("long", F.PandasUDFType.GROUPED_AGG)
    def sum_values(values, _percentages):
        return values.sum()

    spark.udf.register("approx_percentile", sum_values)
    percentages = F.array_compact(F.array(F.lit(0.0), F.lit(None).cast("double"), F.lit(1.0)))
    aggregate = F.call_udf("approx_percentile", F.col("value"), percentages).over(Window.partitionBy())
    result = spark.createDataFrame([(1,), (2,)], ["value"]).select("value", aggregate.alias("p"))
    assert [row.p for row in result.orderBy("value").collect()] == [3, 3]
