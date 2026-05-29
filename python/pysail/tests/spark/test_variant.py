import pytest
from pyspark.sql import Row
from pyspark.sql import functions as spark_functions
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


def test_variant_udf_output_casts_to_json(spark):
    @spark_functions.udf(spark_types.VariantType())
    def make_variant(i):
        return spark_types.VariantVal(bytes([2, 1, 0, 0, 2, 5, 97 + i]), bytes([1, 1, 0, 1, 97]))

    actual = spark.range(0, 3).select(make_variant("id").cast("string").alias("v")).collect()

    assert actual == [Row(v='{"a":"a"}'), Row(v='{"a":"b"}'), Row(v='{"a":"c"}')]


def test_complex_variant_udf_output_casts_to_json(spark):
    def make_variant(i):
        return spark_types.VariantVal(bytes([2, 1, 0, 0, 2, 5, 97 + i]), bytes([1, 1, 0, 1, 97]))

    @spark_functions.udf(spark_types.StructType([spark_types.StructField("v", spark_types.VariantType())]))
    def make_struct(i):
        return {"v": make_variant(i)}

    actual = spark.range(0, 3).select(make_struct("id").cast("string").alias("v")).collect()
    assert actual == [Row(v='{{"a":"a"}}'), Row(v='{{"a":"b"}}'), Row(v='{{"a":"c"}}')]

    @spark_functions.udf(spark_types.ArrayType(spark_types.VariantType()))
    def make_array(i):
        return [make_variant(i)]

    actual = spark.range(0, 3).select(make_array("id").cast("string").alias("v")).collect()
    assert actual == [Row(v='[{"a":"a"}]'), Row(v='[{"a":"b"}]'), Row(v='[{"a":"c"}]')]

    @spark_functions.udf(spark_types.MapType(spark_types.StringType(), spark_types.VariantType()))
    def make_map(i):
        return {"v": make_variant(i)}

    actual = spark.range(0, 3).select(make_map("id").cast("string").alias("v")).collect()
    assert actual == [Row(v='{v -> {"a":"a"}}'), Row(v='{v -> {"a":"b"}}'), Row(v='{v -> {"a":"c"}}')]


def test_variant_pandas_udf_output_casts_to_int(spark):
    pandas = pytest.importorskip("pandas")

    @spark_functions.pandas_udf(spark_types.VariantType())
    def make_variant(values: pandas.Series) -> pandas.Series:
        return values.apply(lambda i: spark_types.VariantVal(bytes([12, i]), bytes([1, 0, 0])))

    actual = spark.range(0, 3).select(make_variant("id").cast("int").alias("v")).collect()

    assert actual == [Row(v=0), Row(v=1), Row(v=2)]
