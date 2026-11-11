from collections.abc import Iterator

import pandas as pd
import pyarrow as pa
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, BinaryType, MapType, StringType, StructField, StructType

VALUE = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab"
VALUE_BYTES = bytes.fromhex("6f1c2d3e4b5a4c6d8e7f0123456789ab")


def _binary_value(value):
    assert value is None or isinstance(value, bytes)
    return value


@pytest.mark.parametrize(
    "storage",
    [
        pytest.param("uuid", marks=pytest.mark.spark(skip_if_jvm=True, reason="Uses Sail's UUID type")),
        "binary",
    ],
)
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param("map_arrow", marks=pytest.mark.spark(version=">=4.0", reason="Uses mapInArrow")),
        "map_pandas",
        "scalar_iterator",
    ],
)
def test_uuid_iterator_inputs_are_binary(spark, storage, entry):
    source = spark.createDataFrame([(0, VALUE), (1, None)], "id int, text string").repartition(2)
    value = F.col("text").cast("UUID") if storage == "uuid" else F.unhex(F.regexp_replace("text", "-", ""))
    frame = source.select("id", value.alias("value"))
    assert frame.schema["value"].dataType == BinaryType()

    if entry == "map_arrow":

        def inspect(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in batches:
                expected_type = pa.binary(16) if storage == "uuid" else pa.binary()
                assert batch.schema.field("value").type == expected_type
                values = [_binary_value(value) for value in batch.column("value").to_pylist()]
                yield pa.record_batch([batch.column("id"), pa.array(values, type=pa.binary())], names=["id", "value"])

        result = frame.mapInArrow(inspect, "id int, value binary")
    elif entry == "map_pandas":

        def inspect(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for batch in batches:
                yield pd.DataFrame({"id": batch.id, "value": batch.value.map(_binary_value)})

        result = frame.mapInPandas(inspect, "id int, value binary")
    else:

        @F.pandas_udf("binary", F.PandasUDFType.SCALAR_ITER)
        def inspect(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for batch in batches:
                yield batch.map(_binary_value)

        result = frame.select("id", inspect("value").alias("value"))

    assert result.orderBy("id").collect() == [Row(id=0, value=VALUE_BYTES), Row(id=1, value=None)]


@pytest.mark.spark(skip_if_jvm=True, reason="Uses Sail's UUID type")
@pytest.mark.parametrize("shape", ["list", "struct", "map"])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param("map_arrow", marks=pytest.mark.spark(version=">=4.0", reason="Uses mapInArrow")),
        pytest.param("arrow_scalar", marks=pytest.mark.spark(version=">=4.1", reason="Uses arrow_udf")),
    ],
)
def test_nested_uuid_arrow_inputs_are_binary(spark, shape, entry):
    source = spark.createDataFrame([(0, VALUE), (1, None), (2, None)], "id int, text string").repartition(2)
    expressions = {
        "list": "CAST(array(text, NULL) AS ARRAY<UUID>)",
        "struct": "CAST(named_struct('u', text) AS STRUCT<u:UUID>)",
        "map": "CAST(map('k', text) AS MAP<STRING, UUID>)",
    }
    frame = source.selectExpr("id", f"CASE WHEN id = 2 THEN NULL ELSE {expressions[shape]} END AS value")
    expected_types = {
        "list": ArrayType(BinaryType()),
        "struct": StructType([StructField("u", BinaryType())]),
        "map": MapType(StringType(), BinaryType()),
    }
    assert frame.schema["value"].dataType == expected_types[shape]

    def inspect(array: pa.Array) -> pa.Array:
        if shape == "list":
            assert array.type.value_type == pa.binary(16)
        elif shape == "struct":
            assert array.type.field("u").type == pa.binary(16)
        else:
            assert array.type.item_type == pa.binary(16)
        values = []
        for item in array.to_pylist():
            value = item
            if value is not None:
                if shape == "list":
                    assert len(value) == 2  # noqa: PLR2004
                    assert value[1] is None
                    value = value[0]
                elif shape == "struct":
                    value = value["u"]
                else:
                    value = dict(value)["k"]
            values.append(_binary_value(value))
        return pa.array(values, type=pa.binary())

    if entry == "map_arrow":

        def inspect_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            for batch in batches:
                yield pa.record_batch([batch.column("id"), inspect(batch.column("value"))], names=["id", "value"])

        result = frame.mapInArrow(inspect_batches, "id int, value binary")
    else:
        result = frame.select("id", F.arrow_udf(inspect, "binary")("value").alias("value"))

    assert result.orderBy("id").collect() == [
        Row(id=0, value=VALUE_BYTES),
        Row(id=1, value=None),
        Row(id=2, value=None),
    ]
