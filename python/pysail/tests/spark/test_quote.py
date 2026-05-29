"""Tests for the quote function."""

import pandas as pd
import pytest

from pysail.testing.spark.utils.common import pyspark_version


def test_quote(spark):
    """Tests quote escapes single quotes and backslashes, and wraps in single quotes."""
    df = spark.createDataFrame(
        [
            (0, "hello"),
            (1, "Don't"),
            (2, "a\\b"),
            (3, "a\\'b"),
            (4, ""),
            (5, None),
        ],
        ["id", "value"],
    )

    actual = df.selectExpr("id", "quote(value) AS result").sort("id").toPandas()

    assert actual["result"].iloc[0] == "'hello'"
    assert actual["result"].iloc[1] == "'Don\\'t'"
    assert actual["result"].iloc[2] == "'a\\\\b'"
    assert actual["result"].iloc[3] == "'a\\\\\\'b'"
    assert actual["result"].iloc[4] == "''"
    assert pd.isna(actual["result"].iloc[5])


@pytest.mark.skipif(
    pyspark_version() < (4, 1),
    reason="pyspark.sql.functions.quote was added in PySpark 4.1",
)
def test_quote_function_api(spark):
    """Tests sf.quote() function from pyspark.sql.functions matches the SQL form."""
    from pyspark.sql import functions as sf

    df = spark.createDataFrame([(0, "Don't")], "id INT, value STRING")
    actual = df.select(sf.quote("value").alias("result")).toPandas()
    assert actual["result"].iloc[0] == "'Don\\'t'"
