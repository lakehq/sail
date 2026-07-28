"""Tests for the randstr function."""


def test_randstr(spark):
    """Tests randstr with a seed produces deterministic alphanumeric output."""
    actual = spark.sql("SELECT randstr(3, 0) AS result").toPandas()

    assert actual["result"].iloc[0] == "ceV"
