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
            "map_concat(map(named_struct('a', id), named_struct('b', id)), "
            "map(named_struct('a', id + 2), named_struct('b', id + 2)))",
            T.MapType(
                T.StructType([T.StructField("a", T.LongType(), False)]),
                T.StructType([T.StructField("b", T.LongType(), False)]),
                False,
            ),
            [{Row(a=i): Row(b=i), Row(a=i + 2): Row(b=i + 2)} for i in range(2)],
            id="concat-dynamic-struct-key-and-value",
        ),
        pytest.param(
            "map_values(map('a', named_struct('b', id)))",
            T.ArrayType(T.StructType([T.StructField("b", T.LongType(), False)]), True),
            [[Row(b=i)] for i in range(2)],
            id="values-struct",
        ),
        pytest.param("map_values(map('a', id))", T.ArrayType(T.LongType(), True), [[0], [1]], id="values-primitive"),
    ],
)
def test_nested_map_values_and_schema(spark, expression, value_type, expected):
    result = spark.range(2).selectExpr(f"{expression} AS m")
    assert result.schema == T.StructType([T.StructField("m", value_type, False)])
    assert [row.m for row in result.collect()] == expected


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("map(k, v)", id="map"),
        pytest.param("map_from_arrays(array(k), array(v))", id="arrays"),
        pytest.param("map_from_entries(array(struct(k, v)))", id="entries"),
        pytest.param("map_concat(map(k, v))", id="concat"),
    ],
)
@pytest.mark.parametrize("value_nullable", [False, True])
def test_dynamic_struct_map_keys_and_values_preserve_fields(spark, expression, value_nullable):
    key_type = T.StructType([T.StructField("a", T.LongType(), False, {"tag": "key"})])
    value_type = T.StructType([T.StructField("b", T.LongType(), True, {"tag": "value"})])
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("k", key_type, False),
            T.StructField("v", value_type, value_nullable),
        ]
    )
    values = [Row(b=10), Row(b=None), None if value_nullable else Row(b=30)]
    source = spark.createDataFrame([(i, (i,), value) for i, value in enumerate(values)], schema)
    result = source.orderBy("id").selectExpr(f"{expression} AS m")
    map_type = T.MapType(key_type, value_type, value_nullable)
    assert result.schema == T.StructType([T.StructField("m", map_type, False)])
    assert [row.m for row in result.collect()] == [{Row(a=i): value} for i, value in enumerate(values)]
    extracted = result.select(F.map_values("m").alias("values"))
    assert extracted.schema == T.StructType([T.StructField("values", T.ArrayType(value_type, True), False)])
    assert [row.values for row in extracted.collect()] == [[value] for value in values]


def test_empty_map_concat_preserves_schema_and_value(spark):
    result = spark.range(2).selectExpr("map_concat() AS m")
    assert result.schema == T.StructType([T.StructField("m", T.MapType(T.StringType(), T.StringType(), True), False)])
    assert [row.m for row in result.collect()] == [{}, {}]


@pytest.mark.parametrize("null_on_left", [False, True])
def test_map_concat_preserves_nullable_maps(spark, null_on_left):
    value_type = T.StructType([T.StructField("b", T.LongType(), False, {"tag": "kept"})])
    map_type = T.MapType(T.StringType(), value_type, False)
    schema = T.StructType([T.StructField("a", map_type, True), T.StructField("b", map_type, False)])
    source = spark.createDataFrame([({"a": (1,)}, {"b": (2,)}), (None, {"b": (3,)}), ({}, {})], schema)
    args = ("a", "b") if null_on_left else ("b", "a")
    result = source.select(F.map_concat(*args).alias("m"))
    assert result.schema == T.StructType([T.StructField("m", map_type, True)])
    assert [row.m for row in result.collect()] == [{"a": Row(b=1), "b": Row(b=2)}, None, {}]


# Spark 4.2 keeps DUPLICATED_MAP_KEY a runtime error even during constant folding.
@pytest.mark.parametrize("policy", ["EXCEPTION", "LAST_WIN"])
@pytest.mark.parametrize("constant", [False, True], ids=["dynamic", "constant"])
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("map('a', {value}, 'a', {value} + 1)", id="map"),
        pytest.param("map_from_arrays(array('a', 'a'), array({value}, {value} + 1))", id="arrays"),
        pytest.param(
            "map_from_entries(array(named_struct('key', 'a', 'value', {value}), "
            "named_struct('key', 'a', 'value', {value} + 1)))",
            id="entries",
        ),
        pytest.param("map_concat(map('a', {value}), map('a', {value} + 1))", id="concat"),
    ],
)
def test_map_constructor_and_concat_duplicate_policies(spark, policy, constant, expression):
    key = "spark.sql.mapKeyDedupPolicy"
    original = spark.conf.get(key)
    spark.conf.set(key, policy)
    try:
        expression = expression.format(value="cast(0 AS bigint)" if constant else "id")
        result = spark.range(2).selectExpr(f"{expression} AS m")
        assert result.schema == T.StructType(
            [T.StructField("m", T.MapType(T.StringType(), T.LongType(), False), False)]
        )
        if policy == "EXCEPTION":
            with pytest.raises(SparkRuntimeException, match=r"\[DUPLICATED_MAP_KEY\]"):
                result.collect()
        else:
            assert [row.m for row in result.collect()] == [{"a": 1}, {"a": 1 if constant else 2}]
    finally:
        spark.conf.set(key, original)


@pytest.mark.parametrize(
    ("expression", "nullable", "value_nullable", "expected"),
    [
        pytest.param(
            "map_from_arrays(CASE WHEN id = 0 THEN array('a') END, array(id))",
            True,
            False,
            [{"a": 0}, None],
            id="nullable-arrays",
        ),
        pytest.param(
            "map_concat(map('a', id), cast(NULL AS map<string,bigint>))",
            True,
            True,
            [None, None],
            id="null-map",
        ),
        pytest.param(
            "map_concat(map('a', id), map('b', cast(NULL AS bigint)))",
            False,
            True,
            [{"a": 0, "b": None}, {"a": 1, "b": None}],
            id="null-value",
        ),
    ],
)
def test_map_null_inputs_and_values(spark, expression, nullable, value_nullable, expected):
    result = spark.range(2).selectExpr(f"{expression} AS m")
    assert result.schema == T.StructType(
        [T.StructField("m", T.MapType(T.StringType(), T.LongType(), value_nullable), nullable)]
    )
    assert [row.m for row in result.collect()] == expected


def test_nullable_entries_do_not_make_map_values_nullable(spark):
    value_type = T.StructType([T.StructField("b", T.LongType(), False)])
    entry_type = T.StructType([T.StructField("key", T.StringType(), False), T.StructField("value", value_type, False)])
    schema = T.StructType([T.StructField("entries", T.ArrayType(entry_type, True), True)])
    source = spark.createDataFrame([([("a", (1,))],), ([None],), ([],), (None,)], schema)
    result = source.select(F.map_from_entries("entries").alias("m"))
    assert result.schema == T.StructType([T.StructField("m", T.MapType(T.StringType(), value_type, False), True)])
    assert [row.m for row in result.collect()] == [{"a": Row(b=1)}, None, {}, None]


def test_map_rejects_null_keys(spark):
    result = spark.range(1).select(F.create_map(F.lit(None).cast("int"), F.lit("a")))
    with pytest.raises(SparkRuntimeException):
        result.collect()
