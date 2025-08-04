import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.tests.spark.utils import escape_sql_string_literal, is_jvm_spark


def test_insert_overwrite_directory(spark, tmpdir):
    location = str(tmpdir / "test")

    spark.sql(f"""
        INSERT OVERWRITE DIRECTORY '{escape_sql_string_literal(location)}' USING PARQUET
        SELECT CAST(101 AS LONG) AS id, 'Alice' AS name, 22 AS age
    """)
    actual = spark.read.parquet(location).toPandas()
    expected = pd.DataFrame(
        {"id": [101], "name": ["Alice"], "age": [22]},
    ).astype({"age": "int32"})
    assert_frame_equal(actual, expected)

    if not is_jvm_spark():
        pytest.skip("overwrite for existing data is not supported in Sail yet")

    spark.sql(f"""
        INSERT OVERWRITE DIRECTORY '{escape_sql_string_literal(location)}' USING JSON
        SELECT CAST(201 AS LONG) AS id, 'Bob' AS name, 32 AS age
    """)
    actual = spark.read.json(location).select("id", "name", "age").toPandas()
    # `INT` and `LONG` are written as JSON numbers and both read as `int64`.
    expected = pd.DataFrame(
        {"id": [201], "name": ["Bob"], "age": [32]},
    )
    assert_frame_equal(actual, expected)
