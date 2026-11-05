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


def test_format_number_negative_decimal_places(spark):
    """Returns NULL when decimal places is negative."""
    actual = spark.sql("SELECT format_number(12345.678, -1) AS result").toPandas()

    assert pd.isna(actual["result"].iloc[0])


def test_format_number_empty_pattern(spark):
    """Empty pattern uses default format with grouping and no decimals."""
    actual = spark.sql("SELECT format_number(12831273.83421, '') AS result").toPandas()

    expected = pd.DataFrame({"result": ["12,831,274"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_pattern_with_grouping(spark):
    """Pattern with grouping separators formats correctly."""
    actual = spark.sql("SELECT format_number(12831273.23481, '###,###,###,###,###.###') AS result").toPandas()

    expected = pd.DataFrame({"result": ["12,831,273.235"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_pattern_integer_only(spark):
    """Pattern with no decimal part formats as integer with grouping."""
    actual = spark.sql("SELECT format_number(12332.123456, '#,###,###,###,###,###,##0') AS result").toPandas()

    expected = pd.DataFrame({"result": ["12,332"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_negative_value(spark):
    """Formats a negative number with comma grouping."""
    actual = spark.sql("SELECT format_number(-12332.123456, 4) AS result").toPandas()

    expected = pd.DataFrame({"result": ["-12,332.1235"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_large_value(spark):
    """Formats a very large number with comma grouping."""
    actual = spark.sql("SELECT format_number(1234567890123.0, 2) AS result").toPandas()

    expected = pd.DataFrame({"result": ["1,234,567,890,123.00"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_format_number_zero(spark):
    """Formats zero with decimal places."""
    actual = spark.sql("SELECT format_number(0, 4) AS result").toPandas()

    expected = pd.DataFrame({"result": ["0.0000"]})
    pd.testing.assert_frame_equal(actual, expected)
