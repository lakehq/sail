import pytest
from pyspark.sql.functions import PandasUDFType, pandas_udf


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
