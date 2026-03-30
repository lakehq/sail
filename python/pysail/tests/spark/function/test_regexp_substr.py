"""Tests for the regexp_substr function."""

import pandas as pd


def test_regexp_substr_basic_match(spark):
    """Returns the first substring matching the pattern."""
    actual = spark.sql("SELECT regexp_substr('Steven Jones and Stephen Smith', 'Ste(v|ph)en') AS result").toPandas()

    expected = pd.DataFrame({"result": ["Steven"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_regexp_substr_no_match(spark):
    """Returns NULL when the pattern does not match."""
    actual = spark.sql("SELECT regexp_substr('hello world', 'xyz') AS result").toPandas()

    assert pd.isna(actual["result"].iloc[0])


def test_regexp_substr_with_groups(spark):
    """Returns the entire match even when the pattern contains capture groups."""
    actual = spark.sql("SELECT regexp_substr('100-200, 300-400', '(\\\\d+)-(\\\\d+)') AS result").toPandas()

    expected = pd.DataFrame({"result": ["100-200"]})
    pd.testing.assert_frame_equal(actual, expected)
