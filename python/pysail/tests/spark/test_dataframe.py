import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, lit


def test_dataframe_drop(sail):
    df = sail.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    df2 = sail.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])

    assert_frame_equal(
        df.drop("age").toPandas(),
        pd.DataFrame({"name": ["Tom", "Alice", "Bob"]}),
    )
    assert_frame_equal(
        df.drop(df.age).toPandas(),
        pd.DataFrame({"name": ["Tom", "Alice", "Bob"]}),
    )

    assert_frame_equal(
        df.join(df2, df.name == df2.name, "inner").drop("name").sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16], "height": [80, 85]}),
    )

    df3 = df.join(df2)
    assert_frame_equal(
        df3.select(
            df["age"],
            df["name"].alias("name_left"),
            df2["height"],
            df2["name"].alias("name_right"),
        ).toPandas(),
        pd.DataFrame(
            {
                "age": [14, 23, 16, 14, 23, 16],
                "name_left": ["Tom", "Alice", "Bob", "Tom", "Alice", "Bob"],
                "height": [80, 80, 80, 85, 85, 85],
                "name_right": ["Tom", "Tom", "Tom", "Bob", "Bob", "Bob"],
            }
        ),
    )

    assert_frame_equal(
        df3.drop("name").toPandas(),
        pd.DataFrame({"age": [14, 23, 16, 14, 23, 16], "height": [80, 80, 80, 85, 85, 85]}),
    )

    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df3.drop(col("name")).toPandas()

    df4 = df.withColumn("a.b.c", lit(1))
    assert_frame_equal(
        df4.toPandas(),
        pd.DataFrame({"age": [14, 23, 16], "name": ["Tom", "Alice", "Bob"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )

    assert_frame_equal(
        df4.drop("a.b.c").toPandas(),
        pd.DataFrame({"age": [14, 23, 16], "name": ["Tom", "Alice", "Bob"]}),
    )

    assert_frame_equal(
        df4.drop(col("a.b.c")).toPandas(),
        pd.DataFrame({"age": [14, 23, 16], "name": ["Tom", "Alice", "Bob"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )
