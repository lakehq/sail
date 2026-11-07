"""`min` and `max` of floating point columns in Parquet files, which may be answered from file statistics.

Parquet statistics leave out NaN, which is the largest floating point value in Spark.
"""

import math

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

# `max` from file statistics leaves out NaN, which is the largest value in Spark.
max_statistics_nan_bug = pytest.mark.xfail(
    not is_jvm_spark(), reason="Known Sail bug: `max` from file statistics leaves out NaN", strict=True
)


@max_statistics_nan_bug
@pytest.mark.parametrize("data_type", ["DOUBLE", "FLOAT"])
def test_min_max_of_parquet_floats_with_nan(spark, tmp_path, data_type):
    path = str(tmp_path / "data")
    spark.sql(
        f"SELECT CAST(x AS {data_type}) AS x FROM VALUES (1.0D), (CAST('NaN' AS DOUBLE)), (-2.0D) AS t(x)"  # noqa: S608
    ).coalesce(1).write.parquet(path)
    row = spark.read.parquet(path).selectExpr("min(x)", "max(x)").first()
    assert row[0] == -2.0  # noqa: PLR2004
    assert math.isnan(row[1])


@pytest.mark.parametrize("data_type", ["DOUBLE", "FLOAT"])
@pytest.mark.parametrize("nan", ["CAST('NaN' AS DOUBLE)", "-CAST('NaN' AS DOUBLE)"], ids=["nan", "negative_nan"])
def test_parquet_floats_with_nan_and_one_other_value_read_back(spark, tmp_path, data_type, nan):
    # The bounds of this file are equal, but its values are not.
    path = str(tmp_path / "data")
    spark.sql(f"SELECT id, CAST(x AS {data_type}) AS x FROM VALUES (0, {nan}), (1, 1.0D) AS t(id, x)").coalesce(  # noqa: S608
        1
    ).write.parquet(path)
    df = spark.read.parquet(path)
    [first, second] = df.orderBy("id").collect()
    assert math.isnan(first.x)
    assert second.x == 1.0
    assert [row.id for row in df.where("x = 1.0D").collect()] == [1]
