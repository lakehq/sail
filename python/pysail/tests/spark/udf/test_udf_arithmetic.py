from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StructType

from pysail.tests.spark.dataframe.udt import UnnamedPythonUDT


def test_udf_result_in_arithmetic(spark):
    # The arithmetic operand guards read the operand's `Field`, so a UDF call --
    # whose field is built by the UDF machinery rather than by a column -- must
    # still reach every operator.
    spark.udf.register("plus_one", udf(lambda x: x + 1, IntegerType()))
    for expression, expected in [
        ("plus_one(2) + 1", 4),
        ("plus_one(2) - 1", 2),
        ("plus_one(2) * 1", 3),
        ("plus_one(2) % 1", 0),
        ("plus_one(2) / 1", 3.0),
        ("plus_one(2) + plus_one(2)", 6),
    ]:
        assert spark.sql(f"SELECT {expression} AS r").collect()[0][0] == expected


def test_udf_consuming_udt_in_arithmetic(spark):
    # A UDT is rejected as an arithmetic operand, but feeding it to a UDF and
    # doing arithmetic on the UDF's ordinary result stays allowed.
    spark.udf.register("udt_len", udf(lambda v: len(v), IntegerType()))
    spark.createDataFrame(data=[], schema=StructType().add("a", UnnamedPythonUDT())).createOrReplaceTempView(
        "udf_arithmetic_udt"
    )
    assert spark.sql("SELECT udt_len(a) + 1 AS r FROM udf_arithmetic_udt").collect() == []
