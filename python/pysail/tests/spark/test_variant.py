import pytest
from pyspark.sql import Row
from pyspark.sql import types as spark_types

pytestmark = pytest.mark.skipif(
    not hasattr(spark_types, "VariantType") or not hasattr(spark_types, "VariantVal"),
    reason="VariantType is not available in this Spark version",
)


def test_variant_val_round_trip(spark):
    value = spark_types.VariantVal.parseJson('{"a": 1}')
    schema = spark_types.StructType([spark_types.StructField("f1", spark_types.VariantType())])

    df = spark.createDataFrame([Row(f1=value)], schema=schema)
    actual = df.first()["f1"]

    assert isinstance(df.schema["f1"].dataType, spark_types.VariantType)
    assert bytes(actual.value) == bytes(value.value)
    assert bytes(actual.metadata) == bytes(value.metadata)
