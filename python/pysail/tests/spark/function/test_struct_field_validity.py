import pyarrow as pa
import pytest
from pyspark.sql import Row
from pyspark.sql.types import BinaryType, IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.common import pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow Table input requires Spark 4+")
@pytest.mark.parametrize("null_masks", ["different", "equal", "child-superset"])
@pytest.mark.parametrize("offset", [0, 65], ids=["unsliced", "sliced"])
def test_struct_variable_width_fields_preserve_parent_and_child_nulls(spark, null_masks, offset):
    count = 80
    parent_is_null = [i % 3 == 0 for i in range(count)]
    child_is_null = {
        "different": [i % 5 == 0 for i in range(count)],
        "equal": parent_is_null,
        "child-superset": [parent_is_null[i] or i % 5 == 0 for i in range(count)],
    }[null_masks]
    text = [None if child_is_null[i] else f"{i}:" + "雪" * 257 for i in range(count)]
    binary = [None if child_is_null[i] else bytes([i, 0, 255]) * 257 for i in range(count)]
    payload = pa.StructArray.from_arrays(
        [pa.array(text, type=pa.string()), pa.array(binary, type=pa.binary())],
        names=["text", "binary"],
        mask=pa.array(parent_is_null),
    )
    data = spark.createDataFrame(pa.table({"id": pa.array(range(count), type=pa.int32()), "s": payload}))
    result = data.orderBy("id").offset(offset).select("id", "s.text", "s.binary")

    assert result.schema == StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("binary", BinaryType(), True),
        ]
    )
    assert result.collect() == [
        Row(
            id=i,
            text=None if parent_is_null[i] else text[i],
            binary=None if parent_is_null[i] else binary[i],
        )
        for i in range(offset, count)
    ]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow Table input requires Spark 4+")
def test_deep_struct_field_preserves_every_ancestor_null_mask(spark):
    count = 80
    levels = 8
    values = pa.array(range(count), type=pa.int64())
    nested = values
    for depth in range(levels):
        nested = pa.StructArray.from_arrays(
            [nested, values], names=["x", "unused"], mask=pa.array([i % (levels + 1) == depth for i in range(count)])
        )
    # SQL cannot construct valid child values underneath a NULL parent.
    data = spark.createDataFrame(pa.table({"id": values, "s": nested}))
    result = data.selectExpr("id", "s" + ".x" * levels + " AS value").orderBy("id")
    assert result.collect() == [Row(id=i, value=i if i % (levels + 1) == levels else None) for i in range(count)]
