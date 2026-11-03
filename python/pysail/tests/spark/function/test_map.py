import pytest
from pyspark.errors import SparkRuntimeException
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_empty_map_preserves_schema_and_value(spark):
    result = spark.range(1).select(F.create_map().alias("m"))
    expected = T.StructType([T.StructField("m", T.MapType(T.NullType(), T.NullType(), False), False)])
    assert result.schema == expected
    assert [row.m for row in result.collect()] == [{}]


# Spark 4.2: MapValues exposes containsNull=true even for non-null map values.
@pytest.mark.parametrize(
    ("expression", "value_type", "expected"),
    [
        pytest.param(
            "map('a', named_struct('b', 1))",
            T.MapType(T.StringType(), T.StructType([T.StructField("b", T.IntegerType(), False)]), False),
            [{"a": Row(b=1)}] * 2,
            id="literal-struct-value",
        ),
        pytest.param(
            "map(named_struct('a', 1), named_struct('b', 2))",
            T.MapType(
                T.StructType([T.StructField("a", T.IntegerType(), False)]),
                T.StructType([T.StructField("b", T.IntegerType(), False)]),
                False,
            ),
            [{Row(a=1): Row(b=2)}] * 2,
            id="literal-struct-key-and-value",
        ),
        *[
            pytest.param(
                expression,
                T.MapType(T.StringType(), T.StructType([T.StructField("b", T.LongType(), False)]), False),
                [{"a": Row(b=i)} for i in range(2)],
                id=name,
            )
            for name, expression in [
                ("map", "map('a', named_struct('b', id))"),
                ("arrays", "map_from_arrays(array('a'), array(named_struct('b', id)))"),
                ("entries", "map_from_entries(array(struct('a', named_struct('b', id))))"),
            ]
        ],
        pytest.param(
            "map_concat(map('a', named_struct('b', id)), map('c', named_struct('b', id + 1)))",
            T.MapType(T.StringType(), T.StructType([T.StructField("b", T.LongType(), False)]), False),
            [{"a": Row(b=i), "c": Row(b=i + 1)} for i in range(2)],
            id="concat-non-null-values",
        ),
        pytest.param(
            "map_concat(map('a', named_struct('b', id)), map('c', CASE WHEN id = 0 THEN named_struct('b', id) END))",
            T.MapType(T.StringType(), T.StructType([T.StructField("b", T.LongType(), False)]), True),
            [{"a": Row(b=0), "c": Row(b=0)}, {"a": Row(b=1), "c": None}],
            id="concat-nullable-values",
        ),
        pytest.param(
            "map_values(map('a', named_struct('b', id)))",
            T.ArrayType(T.StructType([T.StructField("b", T.LongType(), False)]), True),
            [[Row(b=i)] for i in range(2)],
            id="values-struct",
        ),
        pytest.param(
            "map_values(map('a', id))", T.ArrayType(T.LongType(), True), [[0], [1]], id="values-primitive"
        ),
    ],
)
def test_nested_map_values_and_schema(spark, expression, value_type, expected):
    result = spark.range(2).selectExpr(f"{expression} AS m")
    assert result.schema == T.StructType([T.StructField("m", value_type, False)])
    assert [row.m for row in result.collect()] == expected


def test_nullable_entries_do_not_make_map_values_nullable(spark):
    value_type = T.StructType([T.StructField("b", T.LongType(), False)])
    entry_type = T.StructType(
        [T.StructField("key", T.StringType(), False), T.StructField("value", value_type, False)]
    )
    schema = T.StructType([T.StructField("entries", T.ArrayType(entry_type, True), True)])
    source = spark.createDataFrame([([("a", (1,))],), ([None],), ([],), (None,)], schema)
    result = source.select(F.map_from_entries("entries").alias("m"))
    assert result.schema == T.StructType([T.StructField("m", T.MapType(T.StringType(), value_type, False), True)])
    assert [row.m for row in result.collect()] == [{"a": Row(b=1)}, None, {}, None]


def test_map_rejects_null_keys(spark):
    result = spark.range(1).select(F.create_map(F.lit(None).cast("int"), F.lit("a")))
    with pytest.raises(SparkRuntimeException):
        result.collect()
