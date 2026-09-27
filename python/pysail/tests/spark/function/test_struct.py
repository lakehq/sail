import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StructField, StructType


def test_struct_preserves_lambda_field_name(spark):
    observations = spark.createDataFrame(
        [([(-2.5,), (6.0,)],), ([(8.0,), (16.5,)],)],
        "readings array<struct<temperature:double>>",
    )

    result = observations.select(
        F.exists("readings", lambda reading: F.struct(reading.temperature).temperature < 0).alias("freezing")
    )

    assert result.collect() == [Row(freezing=True), Row(freezing=False)]


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("a", [1.0, 2.0, 2.0, 3.0, 3.0, None, None]),
        ("b", ["apple", "banana", "banana", "orange", "orange", None, None]),
        ("c", [2, 2, 3, 3, 4, None, None]),
    ],
)
@pytest.mark.parametrize("access", ["getField", "wildcard"])
def test_struct_field_preserves_parent_nulls(spark, field, expected, access):
    data = spark.createDataFrame(
        [
            Row(abc=Row(a=1.0, b="banana", c=2)),
            Row(abc=Row(a=2.0, b="apple", c=3)),
            Row(abc=Row(a=3.0, b="orange", c=4)),
            Row(abc=Row(a=None, b="banana", c=2)),
            Row(abc=Row(a=2.0, b=None, c=3)),
            Row(abc=None),
            Row(abc=Row(a=3.0, b="orange", c=None)),
        ]
    )
    if access == "getField":
        result = data.select(data.abc.getField(field).alias("field"))
    else:
        result = data.select("abc.*").select(F.col(field).alias("field"))

    assert [row.field for row in result.orderBy(F.col("field").asc_nulls_last()).collect()] == expected


def test_nested_struct_field_preserves_nulls_and_schema(spark):
    schema = StructType(
        [
            StructField(
                "s",
                StructType([StructField("inner", StructType([StructField("x", IntegerType(), False)]), True)]),
                True,
            )
        ]
    )
    data = spark.createDataFrame([(None,), ((None,),), (((7,),),)], schema)
    result = data.select(data.s.getField("inner").getField("x").alias("x"))

    assert result.schema == StructType([StructField("x", IntegerType(), True)])
    assert result.collect() == [Row(x=None), Row(x=None), Row(x=7)]


def test_struct_field_in_lambda_preserves_parent_nulls(spark):
    data = spark.createDataFrame([([None, Row(x=2), Row(x=None)],)], "items array<struct<x:int>>")
    result = data.select(F.transform("items", lambda item: item.x).alias("values"))

    assert result.collect() == [Row(values=[None, 2, None])]


def test_struct_null_typed_field(spark):
    data = spark.createDataFrame([(None,), (Row(x=None),)], "s struct<x:void>")

    assert data.select("s.x").collect() == [Row(x=None), Row(x=None)]


def test_struct_fields_after_offset(spark):
    data = spark.createDataFrame([(Row(a=1, b="skip"),), (None,), (Row(a=2, b="keep"),)], "s struct<a:int,b:string>")

    assert data.offset(1).select("s.*").collect() == [Row(a=None, b=None), Row(a=2, b="keep")]
