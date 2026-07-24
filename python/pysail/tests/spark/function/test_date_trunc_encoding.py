"""`date_trunc` / `trunc` on a string column whose ENCODING merely wraps the string.

A pandas `category` column arrives as a dictionary-encoded Arrow array. The plan builder matches
the value on its LOGICAL type, so such a column is accepted -- but the parser it routes to
declares a user-defined signature and does no coercion of its own, so the encoding has to be
unwrapped before the call or the query is rejected for its layout alone.

This cannot be written as a `.feature` scenario: SQL has no syntax that produces a
dictionary-encoded column.
"""

import pandas as pd
import pytest
from pyspark.sql.types import Row


def _categorical(spark, values):
    return spark.createDataFrame(pd.DataFrame({"v": pd.Series(values, dtype="category")}))


@pytest.mark.date_trunc
def test_date_trunc_on_dictionary_encoded_string(spark):
    df = _categorical(spark, ["2024-05-15 13:45:30", "2024-05-15 13:45:30"])
    df.createOrReplaceTempView("dict_values")
    # Compare the rendered string: collecting a timestamp converts it to the CLIENT's local zone,
    # which would make the expected value depend on where the test runs.
    actual = spark.sql("SELECT CAST(date_trunc('MM', v) AS STRING) AS result FROM dict_values").collect()
    assert actual == [Row(result="2024-05-01 00:00:00")] * 2


@pytest.mark.trunc
def test_trunc_on_dictionary_encoded_string(spark):
    df = _categorical(spark, ["2024-05-15", "2024-05-15"])
    df.createOrReplaceTempView("dict_dates")
    actual = spark.sql("SELECT CAST(trunc(v, 'MM') AS STRING) AS result FROM dict_dates").collect()
    assert actual == [Row(result="2024-05-01")] * 2


@pytest.mark.date_trunc
def test_date_trunc_with_dictionary_encoded_unit(spark):
    """The UNIT, not the value: it reaches a different function, with its own signature."""
    df = _categorical(spark, ["MM", "YEAR"])
    df.createOrReplaceTempView("dict_units")
    actual = spark.sql(
        "SELECT CAST(date_trunc(v, TIMESTAMP '2024-05-15 13:45:30') AS STRING) AS result FROM dict_units"
    ).collect()
    assert actual == [Row(result="2024-05-01 00:00:00"), Row(result="2024-01-01 00:00:00")]


@pytest.mark.trunc
def test_trunc_with_dictionary_encoded_unit(spark):
    df = _categorical(spark, ["MM", "YEAR"])
    df.createOrReplaceTempView("dict_trunc_units")
    actual = spark.sql("SELECT CAST(trunc(DATE '2024-05-15', v) AS STRING) AS result FROM dict_trunc_units").collect()
    assert actual == [Row(result="2024-05-01"), Row(result="2024-01-01")]
