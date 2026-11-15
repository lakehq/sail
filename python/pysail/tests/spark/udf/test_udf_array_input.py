import pytest
from pyspark import Row
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("offset", [0, 10])
def test_non_arrow_array_udf_after_offset(spark, offset):
    identity = F.udf(lambda values: values, "array<long>", useArrow=False)
    df = spark.range(0, 1000, 1, 1).select("id", F.array("id", F.col("id") + 1000, F.col("id") + 2000).alias("values"))
    rows = df.offset(offset).select("id", identity("values").alias("actual")).collect()

    expected_ids = range(offset, 1000)
    assert len(rows) == len(expected_ids)
    for row, i in zip(sorted(rows, key=lambda row: row.id), expected_ids, strict=True):
        assert row == Row(id=i, actual=[i, i + 1000, i + 2000])


def test_non_arrow_array_udf_preserves_nulls_after_offset(spark):
    identity = F.udf(lambda values: values, "array<long>", useArrow=False)
    values = (
        F.when(F.col("id") == 0, F.array(F.lit(90), F.lit(91)))
        .when(F.col("id") == 1, F.lit(None).cast("array<long>"))
        .when(F.col("id") == 2, F.array().cast("array<long>"))  # noqa: PLR2004
        .when(F.col("id") == 3, F.array(F.lit(7), F.lit(None).cast("long")))  # noqa: PLR2004
        .otherwise(F.array(F.lit(9)))
        .cast("array<long>")
    )
    df = spark.range(0, 5, 1, 1).select("id", values.alias("values")).offset(1)
    rows = df.select("id", identity("values").alias("actual")).collect()

    assert sorted(rows, key=lambda row: row.id) == [
        Row(id=1, actual=None),
        Row(id=2, actual=[]),
        Row(id=3, actual=[7, None]),
        Row(id=4, actual=[9]),
    ]
