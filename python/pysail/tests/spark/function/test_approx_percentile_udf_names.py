import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

# Local Spark teardown removes SPARK_REMOTE before later module fixtures run.
_PREFER_REGISTERED_FUNCTIONS = is_jvm_spark() and pyspark_version() >= (4, 2)


@pytest.fixture(scope="module", autouse=True)
def _prefer_registered_functions(spark):
    # Spark 4.2 defaults to resolving built-ins before temporary functions.
    # These tests exercise user functions registered with built-in names.
    if _PREFER_REGISTERED_FUNCTIONS:
        spark.conf.set("spark.sql.path.enabled", "true")
        spark.sql("SET PATH = system.session, system.builtin, current_schema").collect()


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
