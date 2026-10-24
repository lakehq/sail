import pytest
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, LongType, MapType, StructType
from pyspark.testing.objects import PythonOnlyPoint, PythonOnlyUDT


def test_udt_column_is_kept_in_parquet(spark, tmp_path):
    path = str(tmp_path / "points")
    spark.createDataFrame([Row(label=1.0, point=PythonOnlyPoint(1.0, 2.0))]).write.parquet(path)
    df = spark.read.parquet(path)
    assert df.schema["point"].dataType == PythonOnlyUDT()
    assert df.head().point == PythonOnlyPoint(1.0, 2.0)


@pytest.mark.parametrize(
    ("udt_type", "value"),
    [
        (ArrayType(PythonOnlyUDT()), [PythonOnlyPoint(1.0, 2.0)]),
        (MapType(LongType(), PythonOnlyUDT()), {1: PythonOnlyPoint(3.0, 4.0)}),
        (StructType().add("p", PythonOnlyUDT()), (PythonOnlyPoint(5.0, 6.0),)),
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
