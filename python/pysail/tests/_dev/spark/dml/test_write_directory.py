# SPDX-License-Identifier: Apache-2.0

import re

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.testing.spark.utils.sql import escape_sql_string_literal


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


def _assert_path_self_overwrite_rejected(path, write, read):
    expected = sorted(read().collect())
    with pytest.raises(
        Exception,
        match=r"\[UNSUPPORTED_OVERWRITE\.PATH\] Can't overwrite the target that is also being read from\. "
        + re.escape(f"The target path is file://{path}."),
    ):
        write()
    assert sorted(read().collect()) == expected


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark does not check unqualified paths and loses the data")
@pytest.mark.parametrize("suffix", ["", "/"])
def test_insert_overwrite_directory_reading_itself_is_rejected(spark, tmp_path, suffix):
    path = str(tmp_path / "p")
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "a INT, b STRING").write.parquet(path)
    target = escape_sql_string_literal(path + suffix)
    spark.sql(f"CREATE TEMP VIEW v AS SELECT * FROM parquet.`{path}`")  # noqa: S608
    try:
        for query in [
            f"INSERT OVERWRITE DIRECTORY '{target}' USING parquet SELECT * FROM parquet.`{path}`",  # noqa: S608
            f"INSERT OVERWRITE DIRECTORY '{target}' USING parquet SELECT * FROM v",  # noqa: S608
        ]:
            _assert_path_self_overwrite_rejected(
                path, lambda query=query: spark.sql(query), lambda: spark.read.parquet(path)
            )
    finally:
        spark.catalog.dropTempView("v")


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark does not check path writes and loses the data")
@pytest.mark.parametrize("data_format", ["parquet", "json", "csv"])
def test_dataframe_overwrite_of_path_reading_itself_is_rejected(spark, tmp_path, data_format):
    path = str(tmp_path / "p")
    schema = "a INT, b STRING"
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema).write.format(data_format).save(path)

    def read():
        return spark.read.format(data_format).schema(schema).load(path)

    for target, df in [(path, read()), (path + "/", read()), (path, read().where("a > 1"))]:
        _assert_path_self_overwrite_rejected(
            path,
            lambda target=target, df=df: df.write.mode("overwrite").format(data_format).save(target),
            read,
        )


def test_dataframe_append_of_path_reading_itself(spark, tmp_path):
    path = str(tmp_path / "p")
    rows = [(1, "a"), (2, "b"), (3, "c")]
    spark.createDataFrame(rows, "a INT, b STRING").write.parquet(path)
    spark.read.parquet(path).write.mode("append").parquet(path)
    assert sorted(spark.read.parquet(path).collect()) == sorted(rows * 2)
