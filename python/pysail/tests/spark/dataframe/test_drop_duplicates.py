import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row


def test_drop_duplicates(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "name"])
    assert sorted(df.dropDuplicates().collect()) == [Row(id=1, name="a"), Row(id=2, name="b")]


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_deduplicate_within_watermark_in_batch.
def test_drop_duplicates_within_watermark_rejects_batch_input(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    with pytest.raises(
        AnalysisException,
        match="dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets",
    ):
        df.dropDuplicatesWithinWatermark().collect()
