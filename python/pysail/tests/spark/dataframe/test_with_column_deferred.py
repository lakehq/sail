"""Pre-existing gaps measured on Spark 4.2, main, HEAD and the staged PR."""

import pytest
from pyspark.sql import functions as F

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

@pytest.fixture(params=["true", "false"], ids=["ansi", "non-ansi"])
def explicit_column_settings(spark, request):
    settings = {
        "spark.sql.ansi.enabled": request.param,
        "spark.sql.caseSensitive": "false",
        "spark.sql.analyzer.strictDataFrameColumnResolution": "true",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
            assert spark.conf.get(key) == value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="Non-strict column resolution requires Spark 4.2")
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail does not implement non-strict fallback for a shadowed DataFrame column",
    strict=True,
)
def test_with_column_non_strict_shadowed_reference(spark, explicit_column_settings):
    spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "false")
    original = spark.sql("SELECT 123 AS c")
    result = original.withColumn("c", F.col("c").cast("string")).select(original.c)

    assert result._show_string(truncate=False) == "+---+\n|c  |\n+---+\n|123|\n+---+\n"
    assert result.schema.simpleString() == "struct<c:string>"


@pytest.mark.skipif(pyspark_version() < (4, 0), reason="DataFrame scalar subqueries require Spark 4")
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail cannot decorrelate outer references in a scalar-subquery projection",
    strict=True,
)
def test_with_column_correlated_scalar_projection(spark, explicit_column_settings):
    original = spark.sql("SELECT 1 AS c1, 2 AS c2")
    scalar = spark.range(1).select(F.col("c1").outer() + F.col("c2").outer()).scalar()
    result = original.withColumn("scalar", scalar)

    assert result._show_string(truncate=False) == (
        "+---+---+------+\n"
        "|c1 |c2 |scalar|\n"
        "+---+---+------+\n"
        "|1  |2  |3     |\n"
        "+---+---+------+\n"
    )
