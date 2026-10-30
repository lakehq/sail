from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/test_dataframe.py,
# DataFrameTestsMixin.test_duplicate_field_names.
def test_collect_struct_with_duplicate_field_names(spark):
    schema = StructType(
        [
            StructField(
                "struct",
                StructType([StructField("x", StringType()), StructField("x", IntegerType())]),
            ),
            StructField("x", IntegerType()),
        ]
    )
    data = [Row(Row("a", 1), 2), Row(Row("b", 3), 4)]
    df = spark.createDataFrame(data, schema=schema)
    assert df.schema == schema
    assert df.collect() == data


def test_collect_nested_struct_with_duplicate_field_names(spark):
    df = spark.sql("SELECT named_struct('a', 1, 'a', named_struct('b', 2, 'b', 3)) AS s")
    row = df.collect()[0]
    assert row.s[0] == 1
    assert tuple(row.s[1]) == (2, 3)
    assert df.schema["s"].dataType.names == ["a", "a"]
