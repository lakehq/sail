from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.tests.spark.dataframe.udt import PythonPoint, PythonPointUDT


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_types.py,
# TypesParityTests.test_cast_to_string_with_udt; retain the Python-only UDT and add NULL coverage.
def test_python_udt_cast_to_string(spark):
    schema = StructType([StructField("id", IntegerType(), False), StructField("point", PythonPointUDT())])
    df = spark.createDataFrame([(1, PythonPoint(3.0, 4.0)), (2, None)], schema)
    result = df.orderBy("id").select(F.col("point").cast("string"))
    assert result.schema == StructType([StructField("point", StringType())])
    assert result.collect() == [Row(point="[3.0, 4.0]"), Row(point=None)]


# Regression extending Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_types.py,
# TypesParityTests.test_cast_to_string_with_udt; also cover a nested Python-only UDT.
def test_nested_python_udt_cast_to_string(spark):
    schema = StructType([StructField("point", PythonPointUDT())])
    df = spark.createDataFrame([(PythonPoint(3.0, 4.0),)], schema)
    result = df.select(F.struct("point").alias("nested")).select(F.col("nested.point").cast("string"))
    assert result.collect() == [Row(point="[3.0, 4.0]")]
