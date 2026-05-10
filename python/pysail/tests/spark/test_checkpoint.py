import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import col, lit


def _roundtrip(df):
    return df.sort(*df.columns).toPandas()


@pytest.mark.parametrize("eager", [True, False])
def test_dataframe_local_checkpoint(spark, eager):
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    checkpointed = df.localCheckpoint(eager)

    # The checkpointed DataFrame preserves the schema of the source DataFrame.
    assert checkpointed.schema == df.schema
    assert checkpointed.columns == ["age", "name"]
    assert repr(checkpointed) == "DataFrame[age: bigint, name: string]"

    # The checkpointed DataFrame returns the same rows as the source DataFrame.
    assert_frame_equal(
        _roundtrip(checkpointed),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"]}).sort_values(
            ["age", "name"], ignore_index=True
        ),
    )


@pytest.mark.parametrize("eager", [True, False])
def test_dataframe_checkpoint(spark, eager):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
    checkpointed = df.checkpoint(eager)

    assert checkpointed.schema == df.schema
    assert repr(checkpointed) == "DataFrame[id: bigint, value: string]"
    assert_frame_equal(
        _roundtrip(checkpointed),
        pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]}),
    )


def test_dataframe_local_checkpoint_default_eager(spark):
    df = spark.createDataFrame([(1,)], ["x"])
    # Default `eager=True` should also work.
    checkpointed = df.localCheckpoint()
    assert checkpointed.columns == ["x"]
    assert_frame_equal(checkpointed.toPandas(), pd.DataFrame({"x": [1]}))


def test_dataframe_local_checkpoint_with_storage_level(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    # The storage level argument is accepted and the result is a usable DataFrame.
    checkpointed = df.localCheckpoint(eager=True, storageLevel=StorageLevel.MEMORY_AND_DISK)
    assert checkpointed.columns == ["id", "name"]
    assert_frame_equal(
        _roundtrip(checkpointed),
        pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
    )


def test_dataframe_checkpoint_after_transformation(spark):
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    transformed = df.filter(col("age") > lit(15)).withColumn("country", lit("US"))
    checkpointed = transformed.localCheckpoint()

    assert checkpointed.columns == ["age", "name", "country"]
    assert_frame_equal(
        _roundtrip(checkpointed),
        pd.DataFrame({"age": [16, 23], "name": ["Bob", "Alice"], "country": ["US", "US"]}).sort_values(
            ["age", "country", "name"], ignore_index=True
        ),
    )

    # The checkpointed DataFrame can be used in further operations.
    aggregated = checkpointed.groupBy("country").count().toPandas()
    assert_frame_equal(
        aggregated.sort_values("country", ignore_index=True),
        pd.DataFrame({"country": ["US"], "count": [2]}),
    )


def test_dataframe_local_checkpoint_with_nulls(spark):
    df = spark.createDataFrame([Row(a=1, b="x"), Row(a=None, b="y"), Row(a=3, b=None)])
    checkpointed = df.localCheckpoint()
    pdf = checkpointed.sort("b").toPandas()
    # Null values are preserved through the checkpoint.
    assert pdf["a"].tolist()[:2] == [1, None] or pdf["a"].isna().sum() == 1
    assert pdf["b"].isna().sum() == 1


def test_dataframe_local_checkpoint_join(spark):
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    right = spark.createDataFrame([(1, 100), (2, 200)], ["id", "value"])
    checkpointed = left.localCheckpoint()
    joined = checkpointed.join(right, "id").toPandas()
    assert_frame_equal(
        joined.sort_values("id", ignore_index=True),
        pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [100, 200]}),
    )


def test_dataframe_local_checkpoint_empty(spark):
    df = spark.createDataFrame([], "id long, name string")
    checkpointed = df.localCheckpoint()
    assert checkpointed.columns == ["id", "name"]
    assert checkpointed.count() == 0


def test_dataframe_local_checkpoint_chained(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    cp1 = df.localCheckpoint()
    cp2 = cp1.filter(col("x") > 1).localCheckpoint()
    assert_frame_equal(
        cp2.sort("x").toPandas(),
        pd.DataFrame({"x": [2, 3]}),
    )
