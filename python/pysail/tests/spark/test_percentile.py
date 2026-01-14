"""Tests for percentile_cont aggregate function."""

import pandas as pd


def test_percentile_cont(spark):
    """Tests percentile_cont with WITHIN GROUP (ORDER BY ...) syntax."""
    df = spark.createDataFrame(
        [(10,), (15,), (7,), (20,), (10,), (3,), (5,), (8,)],
        ["quantity"],
    )
    df.createOrReplaceTempView("test_quantities")

    actual = spark.sql("""
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY quantity) AS median
        FROM test_quantities
    """).toPandas()

    # Sorted: [3, 5, 7, 8, 10, 10, 15, 20]
    # 50th percentile (interpolated) = 9.0
    expected = pd.DataFrame({"median": [9.0]})
    pd.testing.assert_frame_equal(actual, expected)
