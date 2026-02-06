"""Tests for the histogram_numeric aggregate function."""

import pandas as pd


def test_histogram_numeric_basic(spark):
    """Each value gets its own bin when fewer values than bins."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist FROM VALUES (0), (1), (2), (10) AS tab(col)"
    ).toPandas()

    expected = pd.DataFrame(
        {"hist": [[{"x": 0, "y": 1.0}, {"x": 1, "y": 1.0}, {"x": 2, "y": 1.0}, {"x": 10, "y": 1.0}]]}
    )
    pd.testing.assert_frame_equal(actual, expected)


def test_histogram_numeric_repeated_values(spark):
    """Duplicate values accumulate into the same bin."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist FROM VALUES (1), (1), (1), (2), (3) AS tab(col)"
    ).toPandas()

    expected = pd.DataFrame({"hist": [[{"x": 1, "y": 3.0}, {"x": 2, "y": 1.0}, {"x": 3, "y": 1.0}]]})
    pd.testing.assert_frame_equal(actual, expected)


def test_histogram_numeric_merging(spark):
    """Bins are merged when there are more distinct values than requested bins."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 2) AS hist FROM VALUES (1), (2), (3), (100) AS tab(col)"
    ).toPandas()

    hist = actual["hist"].iloc[0]
    expected_bins = 2
    expected_total = 4.0
    assert len(hist) == expected_bins
    total_count = sum(entry["y"] for entry in hist)
    assert total_count == expected_total


def test_histogram_numeric_with_nulls(spark):
    """Null values are ignored."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist FROM VALUES (1), (NULL), (2), (NULL), (3) AS tab(col)"
    ).toPandas()

    expected = pd.DataFrame({"hist": [[{"x": 1, "y": 1.0}, {"x": 2, "y": 1.0}, {"x": 3, "y": 1.0}]]})
    pd.testing.assert_frame_equal(actual, expected)
