import pytest


@pytest.mark.spark(cross_version=True)
@pytest.mark.parametrize(
    "qualifier",
    [
        "year",
        "month",
        "year to month",
        "day",
        "hour",
        "minute",
        "second",
        "day to hour",
        "day to minute",
        "day to second",
        "hour to minute",
        "hour to second",
        "minute to second",
    ],
)
def test_print_schema_interval_qualifiers(spark, capsys, qualifier):
    df = spark.sql(f"SELECT CAST(NULL AS INTERVAL {qualifier}) AS value")

    df.printSchema()

    assert capsys.readouterr().out.strip() == f"root\n |-- value: interval {qualifier} (nullable = true)"
