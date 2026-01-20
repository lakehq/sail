"""Tests for percentile_cont and percentile_disc aggregate functions."""

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


def test_percentile_disc_basic(spark):
    """Tests percentile_disc returns an actual value from the dataset (no interpolation)."""
    df = spark.createDataFrame(
        [(10,), (20,), (30,), (40,), (50,)],
        ["val"],
    )
    df.createOrReplaceTempView("test_disc_basic")

    actual = spark.sql("""
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) AS median
        FROM test_disc_basic
    """).toPandas()

    # Sorted: [10, 20, 30, 40, 50]
    # 50th percentile (discrete) = 30 (actual value, not interpolated)
    expected = pd.DataFrame({"median": [30]})
    pd.testing.assert_frame_equal(actual, expected)


def test_percentile_disc_with_group_by(spark):
    """Tests percentile_disc with GROUP BY clause."""
    df = spark.createDataFrame(
        [
            ("A", 10),
            ("A", 20),
            ("A", 30),
            ("B", 100),
            ("B", 200),
            ("B", 300),
            ("B", 400),
        ],
        ["category", "val"],
    )
    df.createOrReplaceTempView("test_disc_grouped")

    actual = spark.sql("""
        SELECT
            category,
            percentile_disc(0.5) WITHIN GROUP (ORDER BY val) AS median
        FROM test_disc_grouped
        GROUP BY category
        ORDER BY category
    """).toPandas()

    # Group A: [10, 20, 30] -> median = 20
    # Group B: [100, 200, 300, 400] -> median = 200
    expected = pd.DataFrame({"category": ["A", "B"], "median": [20, 200]})
    pd.testing.assert_frame_equal(actual, expected)


def test_percentile_disc_edge_cases(spark):
    """Tests percentile_disc edge cases: 0th and 100th percentiles."""
    df = spark.createDataFrame(
        [(5,), (10,), (15,), (20,)],
        ["val"],
    )
    df.createOrReplaceTempView("test_disc_edges")

    actual = spark.sql("""
        SELECT
            percentile_disc(0.0) WITHIN GROUP (ORDER BY val) AS p0,
            percentile_disc(1.0) WITHIN GROUP (ORDER BY val) AS p100
        FROM test_disc_edges
    """).toPandas()

    # 0th percentile = min = 5
    # 100th percentile = max = 20
    expected = pd.DataFrame({"p0": [5], "p100": [20]})
    pd.testing.assert_frame_equal(actual, expected)


def test_percentile_disc_with_nulls(spark):
    """Tests percentile_disc correctly ignores null values."""
    df = spark.createDataFrame(
        [(10,), (None,), (20,), (None,), (30,), (40,), (None,), (50,)],
        ["val"],
    )
    df.createOrReplaceTempView("test_disc_nulls")

    actual = spark.sql("""
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) AS median
        FROM test_disc_nulls
    """).toPandas()

    # Non-null values: [10, 20, 30, 40, 50]
    # 50th percentile (discrete) = 30
    expected = pd.DataFrame({"median": [30]})
    pd.testing.assert_frame_equal(actual, expected)
