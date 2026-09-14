import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import IntegerType, StructField, StructType


def test_nested_field_comment_from_dataframe_schema_is_named(spark):
    # A comment set through a DataFrame schema reaches the plan by a different route than
    # SQL `COMMENT`, but Spark names it the same way in the arithmetic error.
    inner = StructType([StructField("a", IntegerType(), metadata={"comment": "note"})])
    spark.createDataFrame([((1,),)], StructType([StructField("s", inner)])).createOrReplaceTempView(
        "arithmetic_field_comment"
    )
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql("SELECT s + 1 FROM arithmetic_field_comment").collect()
    assert "STRUCT<a: INT COMMENT 'note'>" in str(excinfo.value)
