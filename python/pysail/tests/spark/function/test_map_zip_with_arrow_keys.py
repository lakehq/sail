import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


@pytest.mark.parametrize(
    ("left_kind", "right_kind", "shape", "case", "ansi"),
    [
        ("list", "list", "array", "string", False),
        ("large", "large", "array", "string", True),
        ("large", "large", "array", "string", False),
        ("large", "list", "array", "string", True),
        ("list", "large", "array", "string", False),
        ("large", "large", "array", "float", True),
        ("large", "large", "array", "date", True),
        ("large", "large", "array", "date", False),
        ("fixed", "fixed", "array", "string", True),
        ("fixed", "list", "array", "string", False),
        ("fixed", "fixed", "array", "zero", True),
        ("large", "large", "struct", "string", False),
        ("fixed", "fixed", "struct", "zero", True),
        ("list", "list", "array", "empty_right", True),
        ("list", "list", "array", "empty_left", True),
        ("fixed", "fixed", "array", "empty_right", True),
        ("large", "large", "nested", "empty_right", True),
    ],
)
def test_map_zip_with_arrow_array_keys(spark, tmp_path, left_kind, right_kind, shape, case, ansi):
    # Parquet preserves Arrow list representations that cannot be specified in SQL.
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    if case == "string":
        left_type, right_type = pa.string(), pa.int32()
        left_value, right_value = "01", 1
        expected_type = T.LongType() if ansi else T.StringType()
        expected = [(1, 3)] if ansi else [("01", 1), ("1", 2)]
    elif case == "float":
        left_type, right_type = pa.float32(), pa.int32()
        left_value, right_value = 16777216.0, 16777217
        expected_type = T.DoubleType()
        expected = [(16777216.0, 1), (16777217.0, 2)]
    elif case == "date":
        left_type, right_type = pa.date32(), pa.int32()
        left_value = right_value = 1
    else:
        left_type = right_type = pa.float64()
        left_value, right_value = -0.0, 0.0
        expected_type = T.DoubleType()
        expected = [(-0.0, 1)] if case == "empty_right" else [(0.0, 2)] if case == "empty_left" else [(-0.0, 3)]

    def key_type(kind, item):
        array = pa.large_list(item) if kind == "large" else pa.list_(item, 1) if kind == "fixed" else pa.list_(item)
        return pa.struct([("a", array)]) if shape == "struct" else pa.list_(array) if shape == "nested" else array

    def key(value):
        return {"a": [value]} if shape == "struct" else [[value]] if shape == "nested" else [value]

    path = tmp_path / "array_keys.parquet"
    pq.write_table(
        pa.table(
            {
                "left": pa.array([[(key(left_value), 1)]], type=pa.map_(key_type(left_kind, left_type), pa.int32())),
                "right": pa.array(
                    [[(key(right_value), 2)]], type=pa.map_(key_type(right_kind, right_type), pa.int32())
                ),
            }
        ),
        path,
    )
    source = spark.read.parquet(str(path))
    expression = F.map_zip_with(
        F.create_map() if case == "empty_left" else F.col("left"),
        F.create_map() if case == "empty_right" else F.col("right"),
        lambda _key, left, right: F.coalesce(left, F.lit(0)) + F.coalesce(right, F.lit(0)),
    )
    if case == "date":
        with pytest.raises(Exception, match=r"(?i)key|types"):
            source.select(expression).collect()
        return

    result = source.select(expression.alias("result"))
    expected_key_type = T.ArrayType(expected_type)
    if shape == "struct":
        expected_key_type = T.StructType([T.StructField("a", expected_key_type)])
    elif shape == "nested":
        expected_key_type = T.ArrayType(expected_key_type)
    assert result.schema[0].dataType.keyType == expected_key_type
    entries = result.select(F.map_entries("result")).first()[0]
    assert [entry.asDict(recursive=True) for entry in entries] == [
        {"key": key(value), "value": merged} for value, merged in expected
    ]
