import pytest
from pyspark.errors import SparkRuntimeException
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_empty_map_preserves_schema_and_value(spark):
    result = spark.range(1).select(F.create_map().alias("m"))
    expected = T.StructType([T.StructField("m", T.MapType(T.NullType(), T.NullType(), False), False)])
    assert result.schema == expected
    assert [row.m for row in result.collect()] == [{}]


def test_map_rejects_null_keys(spark):
    result = spark.range(1).select(F.create_map(F.lit(None).cast("int"), F.lit("a")))
    with pytest.raises(SparkRuntimeException):
        result.collect()
