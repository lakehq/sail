"""Tests for the format_number function."""

import pandas as pd


def test_format_number_decimal_places(spark):
    """Formats a number with comma grouping and specified decimal places."""
    actual = spark.sql("SELECT format_number(12332.123456, 4) AS result").toPandas()

    expected = pd.DataFrame({"result": ["12,332.1235"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_pattern(spark):
    """Formats a number using a DecimalFormat pattern string."""
    actual = spark.sql("SELECT format_number(12332.123456, '##################.###') AS result").toPandas()

    expected = pd.DataFrame({"result": ["12332.123"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_zero_decimals(spark):
    """Formats a number with zero decimal places."""
    actual = spark.sql("SELECT format_number(12332.123456, 0) AS result").toPandas()

    expected = pd.DataFrame({"result": ["12,332"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_null_input(spark):
    """Returns NULL when the input number is NULL."""
    actual = spark.sql("SELECT format_number(NULL, 2) AS result").toPandas()

    assert pd.isna(actual["result"].iloc[0])
