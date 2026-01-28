"""Tests for the soundex function."""

import pandas as pd


def test_soundex(spark):
    """Tests soundex produces correct phonetic codes."""
    df = spark.createDataFrame(
        [("Robert",), ("Rupert",), ("Smith",), ("",), (None,)],
        ["name"],
    )
    df.createOrReplaceTempView("test_soundex")

    actual = spark.sql("SELECT soundex(name) AS code FROM test_soundex").toPandas()

    assert actual["code"].iloc[0] == "R163"
    assert actual["code"].iloc[1] == "R163"
    assert actual["code"].iloc[2] == "S530"
    assert actual["code"].iloc[3] == ""
    assert pd.isna(actual["code"].iloc[4])
