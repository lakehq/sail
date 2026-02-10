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


def test_histogram_numeric_all_nulls(spark):
    """All-NULL input returns a null list."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist "
        "FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col)"
    ).toPandas()

    assert actual["hist"].iloc[0] is None


def test_histogram_numeric_negative_values(spark):
    """Negative values are handled correctly."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist FROM VALUES (-3), (-1), (0), (2), (4) AS tab(col)"
    ).toPandas()

    hist = actual["hist"].iloc[0]
    xs = [entry["x"] for entry in hist]
    expected_total = 5.0
    assert xs == sorted(xs)
    total_count = sum(entry["y"] for entry in hist)
    assert total_count == expected_total


def test_histogram_numeric_float_values(spark):
    """Float column input is supported."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist "
        "FROM VALUES (1.5), (2.5), (3.5) AS tab(col)"
    ).toPandas()

    expected = pd.DataFrame(
        {"hist": [[{"x": 1.5, "y": 1.0}, {"x": 2.5, "y": 1.0}, {"x": 3.5, "y": 1.0}]]}
    )
    pd.testing.assert_frame_equal(actual, expected)


def test_histogram_numeric_single_value(spark):
    """A single non-null value produces one bin."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist FROM VALUES (42) AS tab(col)"
    ).toPandas()

    expected = pd.DataFrame({"hist": [[{"x": 42, "y": 1.0}]]})
    pd.testing.assert_frame_equal(actual, expected)


def test_histogram_numeric_large_values(spark):
    """Large values do not cause precision issues."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 3) AS hist "
        "FROM VALUES (1e15), (2e15), (3e15) AS tab(col)"
    ).toPandas()

    hist = actual["hist"].iloc[0]
    expected_bins = 3
    expected_total = 3.0
    assert len(hist) == expected_bins
    total_count = sum(entry["y"] for entry in hist)
    assert total_count == expected_total


def test_histogram_numeric_bigint(spark):
    """BIGINT column input is supported."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist "
        "FROM VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)) AS tab(col)"
    ).toPandas()

    hist = actual["hist"].iloc[0]
    expected_bins = 3
    expected_total = 3.0
    assert len(hist) == expected_bins
    total_count = sum(entry["y"] for entry in hist)
    assert total_count == expected_total


def test_histogram_numeric_decimal(spark):
    """DECIMAL column input is supported."""
    actual = spark.sql(
        "SELECT histogram_numeric(col, 5) AS hist "
        "FROM VALUES (CAST(1.1 AS DECIMAL(10,2))), (CAST(2.2 AS DECIMAL(10,2))), "
        "(CAST(3.3 AS DECIMAL(10,2))) AS tab(col)"
    ).toPandas()

    hist = actual["hist"].iloc[0]
    expected_bins = 3
    expected_total = 3.0
    assert len(hist) == expected_bins
    total_count = sum(entry["y"] for entry in hist)
    assert total_count == expected_total
