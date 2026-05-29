"""Tests for the soundex function."""

import pandas as pd


def test_soundex(spark):
    """Tests soundex produces correct phonetic codes."""
    df = spark.createDataFrame(
        [(0, "Robert"), (1, "Rupert"), (2, "Smith"), (3, "Tymczak"), (4, "Ashcraft"), (5, ""), (6, None)],
        ["id", "name"],
    )
    df.createOrReplaceTempView("test_soundex")

    actual = spark.sql("SELECT id, soundex(name) AS code FROM test_soundex ORDER BY id").toPandas()

    assert actual["code"].iloc[0] == "R163"
    assert actual["code"].iloc[1] == "R163"
    assert actual["code"].iloc[2] == "S530"
    assert actual["code"].iloc[3] == "T522"
    assert actual["code"].iloc[4] == "A261"
    assert actual["code"].iloc[5] == ""
    assert pd.isna(actual["code"].iloc[6])
