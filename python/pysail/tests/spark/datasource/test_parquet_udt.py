import pytest
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, LongType, MapType, StructType

from pysail.tests.spark.dataframe.udt import PythonPoint, PythonPointUDT


def test_udt_column_is_kept_in_parquet(spark, tmp_path):
    path = str(tmp_path / "points")
    spark.createDataFrame([Row(label=1.0, point=PythonPoint(1.0, 2.0))]).write.parquet(path)
    df = spark.read.parquet(path)
    assert df.schema["point"].dataType == PythonPointUDT()
    point = df.head().point
    assert (point.x, point.y) == (1.0, 2.0)


@pytest.mark.parametrize(
    ("udt_type", "value"),
    [
        (ArrayType(PythonPointUDT()), [PythonPoint(1.0, 2.0)]),
        (MapType(LongType(), PythonPointUDT()), {1: PythonPoint(3.0, 4.0)}),
        (StructType().add("p", PythonPointUDT()), (PythonPoint(5.0, 6.0),)),
    ],
    ids=["array", "map", "struct"],
)
def test_nested_udt_is_kept_in_parquet(spark, tmp_path, udt_type, value):
    path = str(tmp_path / "nested")
    schema = StructType().add("key", LongType()).add("val", udt_type)
    spark.createDataFrame([(1, value)], schema=schema).write.parquet(path)
    df = spark.read.parquet(path)
    assert df.schema == schema
    assert df.count() == 1
