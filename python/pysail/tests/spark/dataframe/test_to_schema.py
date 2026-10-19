import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


@pytest.fixture
def people(spark):
    return spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_to; a nullable field that is not in the input is filled with nulls.
def test_to_fills_missing_nullable_fields_with_null(people):
    schema = StructType([StructField("col1", IntegerType(), True), StructField("col2", StringType(), True)])
    df = people.to(schema)
    assert df.schema == schema
    assert df.collect() == [Row(col1=None, col2=None), Row(col1=None, col2=None)]


def test_to_reorders_and_casts_existing_fields(people):
    schema = StructType([StructField("name", StringType(), True), StructField("id", StringType(), True)])
    assert people.to(schema).collect() == [Row(name="a", id="1"), Row(name="b", id="2")]


def test_to_rejects_missing_non_nullable_field(people):
    schema = StructType([StructField("missing", LongType(), False)])
    with pytest.raises(AnalysisException):
        people.to(schema).collect()


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/test_dataframe.py, DataFrameTestsMixin.test_to;
# a nullable column cannot be assigned to a non-nullable field.
def test_to_rejects_nullable_column_for_non_nullable_field(spark):
    df = spark.createDataFrame([(1,), (None,)], "j long")
    schema = StructType([StructField("j", LongType(), False)])
    with pytest.raises(AnalysisException, match="NULLABLE_COLUMN_OR_FIELD"):
        df.to(schema).count()
