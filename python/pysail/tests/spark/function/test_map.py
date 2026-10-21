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


@pytest.mark.parametrize("function", ["map_from_arrays", "map_from_entries"])
@pytest.mark.parametrize("nullable_values", [False, True])
def test_map_constructors_preserve_interval_fields(spark, function, nullable_values):
    from datetime import timedelta

    key_type = T.DayTimeIntervalType(T.DayTimeIntervalType.HOUR, T.DayTimeIntervalType.HOUR)
    value_type = T.DayTimeIntervalType(T.DayTimeIntervalType.DAY, T.DayTimeIntervalType.DAY)
    rows = [([timedelta(hours=1)], [timedelta(days=2)]), ([], []), (None, None)]
    if nullable_values:
        rows.append(([timedelta(hours=3)], [None]))
    if function == "map_from_arrays":
        schema = T.StructType(
            [
                T.StructField("keys", T.ArrayType(T.DayTimeIntervalType(), False)),
                T.StructField("values", T.ArrayType(T.DayTimeIntervalType(), nullable_values)),
            ]
        )
        frame = spark.createDataFrame(rows, schema).select(
            F.col("keys").cast(T.ArrayType(key_type, False)).alias("keys"),
            F.col("values").cast(T.ArrayType(value_type, nullable_values)).alias("values"),
        )
        result = frame.select(F.map_from_arrays("keys", "values").alias("result"))
    else:
        entry_type = T.StructType(
            [
                T.StructField("k", key_type, False, {"comment": "source key"}),
                T.StructField("v", value_type, nullable_values, {"comment": "source value"}),
            ]
        )
        source_entry_type = T.StructType(
            [T.StructField(f.name, T.DayTimeIntervalType(), f.nullable, f.metadata) for f in entry_type]
        )
        schema = T.StructType([T.StructField("entries", T.ArrayType(source_entry_type, False))])
        entries = [(None if keys is None else list(zip(keys, values, strict=True)),) for keys, values in rows]
        frame = spark.createDataFrame(entries, schema).select(
            F.col("entries").cast(T.ArrayType(entry_type, False)).alias("entries")
        )
        result = frame.select(F.map_from_entries("entries").alias("result"))

    assert result.schema == T.StructType([T.StructField("result", T.MapType(key_type, value_type, nullable_values))])
    assert [row.result for row in result.collect()] == [
        None if keys is None else dict(zip(keys, values, strict=True)) for keys, values in rows
    ]
    assert result.toArrow().to_pylist() == [
        {"result": None if keys is None else list(zip(keys, values, strict=True))} for keys, values in rows
    ]
