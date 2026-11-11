import pytest
from pyspark.sql.functions import PandasUDFType, pandas_udf, udf
from pyspark.sql.types import DoubleType

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def df(spark):
    return spark.createDataFrame([(g, float(i)) for g in range(5) for i in range(4)], schema="g long, v double")


def test_nondeterministic_aggregate_udf_calls_are_evaluated_separately(spark, df):
    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def noise(v):
        return v.sum() + __import__("random").random()

    noise = noise.asNondeterministic()
    rows = df.groupBy("g").agg(noise("v").alias("a"), noise("v").alias("b")).collect()
    assert len(rows) == 5  # noqa: PLR2004
    assert all(row.a != row.b for row in rows)

    spark.udf.register("noise_agg", noise)
    df.createOrReplaceTempView("nondeterministic_agg_input")
    rows = spark.sql(
        "SELECT g, noise_agg(v) AS a, noise_agg(v) AS b FROM nondeterministic_agg_input GROUP BY g"
    ).collect()
    assert len(rows) == 5  # noqa: PLR2004
    assert all(row.a != row.b for row in rows)


def test_deterministic_aggregate_udf_calls_are_equal(df):
    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def total(v):
        return v.sum()

    rows = df.groupBy("g").agg(total("v").alias("a"), total("v").alias("b")).collect()
    assert sorted((row.g, row.a, row.b) for row in rows) == [(g, 6.0, 6.0) for g in range(5)]


@pytest.mark.skipif(is_jvm_spark(), reason="Spark evaluates equal deterministic Python UDF calls once")
def test_deterministic_udf_calls_are_evaluated_separately(spark, df):
    # The calls are not merged even though the UDF claims to be deterministic, as Ibis expects
    # for its impure UDFs (`test_impure_uncorrelated_*`).
    noise = udf(lambda v: v + __import__("random").random(), DoubleType(), useArrow=False)
    rows = df.select(noise("v").alias("a"), noise("v").alias("b")).collect()
    assert any(row.a != row.b for row in rows)

    spark.udf.register("deterministic_noise", noise)
    df.createOrReplaceTempView("deterministic_udf_input")
    rows = spark.sql(
        "SELECT deterministic_noise(v) AS a, deterministic_noise(v) AS b FROM deterministic_udf_input"
    ).collect()
    assert any(row.a != row.b for row in rows)
