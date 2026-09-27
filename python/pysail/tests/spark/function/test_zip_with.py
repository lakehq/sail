import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.mark.parametrize(("left_nullable", "right_nullable"), [(False, False), (False, True), (True, False)])
@pytest.mark.parametrize("constant", [False, True])
def test_zip_with_schema(spark, left_nullable, right_nullable, constant):
    source = spark.createDataFrame(
        [([1], [2, 3])],
        T.StructType(
            [
                T.StructField("left", T.ArrayType(T.IntegerType(), containsNull=False), left_nullable),
                T.StructField("right", T.ArrayType(T.IntegerType(), containsNull=False), right_nullable),
            ]
        ),
    )
    result = source.select(F.zip_with("left", "right", lambda x, y: F.lit(7) if constant else x + y).alias("result"))
    assert result.schema == T.StructType(
        [T.StructField("result", T.ArrayType(T.IntegerType(), not constant), left_nullable or right_nullable)]
    )
    assert result.collect()[0].result == ([7, 7] if constant else [3, None])


@pytest.mark.parametrize(("left_nullable", "right_nullable"), [(False, False), (False, True), (True, False)])
@pytest.mark.parametrize("body", ["key", "value", "constant"])
def test_map_zip_with_schema(spark, left_nullable, right_nullable, body):
    input_type = T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=False)
    source = spark.createDataFrame(
        [({"a": 1}, {"b": 2})],
        T.StructType(
            [T.StructField("left", input_type, left_nullable), T.StructField("right", input_type, right_nullable)]
        ),
    )
    result = source.select(
        F.map_zip_with(
            "left",
            "right",
            lambda key, _left, right: key if body == "key" else F.lit(7) if body == "constant" else right,
        ).alias("result")
    )
    output_type = T.MapType(
        T.StringType(), T.StringType() if body == "key" else T.IntegerType(), valueContainsNull=body == "value"
    )
    assert result.schema == T.StructType([T.StructField("result", output_type, left_nullable or right_nullable)])
    expected = {"key": {"a": "a", "b": "b"}, "constant": {"a": 7, "b": 7}, "value": {"a": None, "b": 2}}
    assert result.collect()[0].result == expected[body]


def test_zip_functions_capture_columns_across_batches(spark):
    source = spark.range(9000, numPartitions=1)
    result = source.select(
        "id",
        F.zip_with(F.array("id"), F.array(F.lit(1)), lambda _left, right: F.col("id") + right).alias("array"),
        F.map_zip_with(
            F.create_map(F.lit("a"), F.col("id")),
            F.create_map(F.lit("b"), F.lit(1)),
            lambda _key, _left, right: F.col("id") + F.coalesce(right, F.lit(0)),
        ).alias("map"),
    )
    assert [(row.id, row.array, row.map) for row in result.orderBy("id").collect()] == [
        (i, [i + 1], {"a": i, "b": i + 1}) for i in range(9000)
    ]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow Table input requires PySpark 4+")
def test_map_zip_with_uses_first_duplicate_values_and_stable_key_order(spark):
    source = spark.createDataFrame(
        pa.table(
            {
                "left": pa.array([[("a", 1), ("a", 99), ("b", 2)]], type=pa.map_(pa.string(), pa.int32())),
                "right": pa.array([[("b", 3), ("b", 88), ("c", 4)]], type=pa.map_(pa.string(), pa.int32())),
            }
        )
    )
    result = source.select(
        F.map_entries(
            F.map_zip_with(
                "left", "right", lambda _key, left, right: F.coalesce(left, F.lit(0)) + F.coalesce(right, F.lit(0))
            )
        ).alias("entries")
    )
    assert [(entry.key, entry.value) for entry in result.collect()[0].entries] == [("a", 1), ("b", 5), ("c", 4)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow Table input requires PySpark 4+")
def test_zip_functions_ignore_sliced_and_hidden_null_values(spark):
    left_array = pa.ListArray.from_arrays(
        [0, 1, 2, 2, 3, 4],
        [0, 0, 2, 0],
        mask=pa.array([False, True, False, False, False]),
    ).slice(1, 3)
    left_map = pa.MapArray.from_arrays(
        [0, 1, 2, 2, 3, 4],
        ["prefix", "hidden", "a", "suffix"],
        [0, 0, 2, 0],
        mask=pa.array([False, True, False, False, False]),
    ).slice(1, 3)
    source = spark.createDataFrame(pa.table({"a": left_array, "m": left_map}))
    result = source.select(
        F.zip_with("a", F.array(F.lit(1)), lambda left, _right: 10 / left).alias("a"),
        F.map_zip_with("m", F.create_map(F.lit("a"), F.lit(1)), lambda _key, left, _right: 10 / left).alias("m"),
    )
    assert [(row.a, row.m) for row in result.collect()] == [(None, None), ([None], {"a": None}), ([5.0], {"a": 5.0})]


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="Spark 4.1 added canonical floating map key equality")
def test_map_zip_with_float_key_equality(spark):
    source = spark.createDataFrame(
        [({-0.0: 1, float("nan"): 2}, {0.0: 3, float("nan"): 4})],
        "left map<double,int>, right map<double,int>",
    )
    entries = (
        source.select(
            F.map_entries(F.map_zip_with("left", "right", lambda _key, left, right: left + right)).alias("entries")
        )
        .collect()[0]
        .entries
    )
    assert [entry.value for entry in entries] == [4, 6]
    assert entries[0].key == 0.0
    assert math.isnan(entries[1].key)


def test_zip_with_default_column_names_preserve_captures_and_literals(spark):
    source = spark.createDataFrame([(["a"], ["b"], "c")], "left array<string>, right array<string>, x_0 string")
    result = source.select(F.zip_with("left", "right", lambda x, y: F.concat(F.lit("x_0:"), x, y, F.col("x_0"))))
    assert result.columns == [
        "zip_with(left, right, lambdafunction(concat(x_0:, namedlambdavariable(), "
        "namedlambdavariable(), x_0), namedlambdavariable(), namedlambdavariable()))"
    ]
    assert result.first()[0] == ["x_0:abc"]


def test_map_zip_with_default_column_names(spark):
    source = spark.createDataFrame([({"a": 1}, {"a": 2})], "left map<string,int>, right map<string,int>")
    result = source.select(F.map_zip_with("left", "right", lambda _key, left, right: left + right))
    assert result.columns == [
        "map_zip_with(left, right, lambdafunction((namedlambdavariable() + namedlambdavariable()), "
        "namedlambdavariable(), namedlambdavariable(), namedlambdavariable()))"
    ]
    assert result.first()[0] == {"a": 3}


def test_zip_with_anonymous_display_preserves_struct_field_names(spark):
    result = spark.sql("SELECT zip_with(array(1), array('a'), (x, y) -> (y, x))")
    assert result.schema[0].dataType.elementType == T.StructType(
        [T.StructField("y", T.StringType()), T.StructField("x", T.IntegerType())]
    )
    assert result.columns == [
        "zip_with(array(1), array(a), lambdafunction(named_struct(y, namedlambdavariable(), "
        "x, namedlambdavariable()), namedlambdavariable(), namedlambdavariable()))"
    ]
    assert result.first()[0][0].asDict() == {"y": "a", "x": 1}


@pytest.mark.parametrize("key_kind", ["array", "struct"])
def test_map_zip_with_nested_temporal_key_schema(spark, key_kind):
    left = "DATE'2020-01-01'"
    right = "TIMESTAMP_NTZ'2020-01-01 00:00:00'"
    if key_kind == "array":
        left, right = f"array({left})", f"array({right})"
        expected = T.ArrayType(T.TimestampNTZType(), containsNull=True)
    else:
        left, right = f"named_struct('d', {left})", f"named_struct('d', {right})"
        expected = T.StructType([T.StructField("d", T.TimestampNTZType(), nullable=True)])
    result = spark.sql(f"SELECT map_zip_with(map({left}, 1), map({right}, 2), (k, x, y) -> x + y) AS result")
    assert result.schema[0].dataType.keyType == expected
    assert result.select(F.map_values("result")).first()[0] == [3]


@pytest.mark.parametrize("map_input", [False, True])
def test_zip_functions_default_names_for_plain_body(spark, map_input):
    if map_input:
        result = spark.sql("SELECT map_zip_with(map(1, 2), map(1, 3), 7)")
        assert result.columns == [
            "map_zip_with(map(1, 2), map(1, 3), lambdafunction(7, namedlambdavariable(), "
            "namedlambdavariable(), namedlambdavariable()))"
        ]
        assert result.first()[0] == {1: 7}
    else:
        result = spark.sql("SELECT zip_with(array(1), array(2), 7)")
        assert result.columns == [
            "zip_with(array(1), array(2), lambdafunction(7, namedlambdavariable(), namedlambdavariable()))"
        ]
        assert result.first()[0] == [7]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT zip_with(array(1), array(2), (1, 2))",
            "zip_with(array(1), array(2), lambdafunction(named_struct(col1, 1, col2, 2), "
            "namedlambdavariable(), namedlambdavariable()))",
        ),
        (
            "SELECT map_zip_with(map(1, 2), map(1, 3), (1, 2))",
            "map_zip_with(map(1, 2), map(1, 3), lambdafunction(named_struct(col1, 1, col2, 2), "
            "namedlambdavariable(), namedlambdavariable(), namedlambdavariable()))",
        ),
        (
            "SELECT zip_with(transform(array(1), x -> x), array(2), 7)",
            "zip_with(transform(array(1), lambdafunction(namedlambdavariable(), namedlambdavariable())), "
            "array(2), lambdafunction(7, namedlambdavariable(), namedlambdavariable()))",
        ),
        (
            "SELECT zip_with(array(1), array(2), transform(array(3), x -> x))",
            "zip_with(array(1), array(2), lambdafunction(transform(array(3), "
            "lambdafunction(namedlambdavariable(), namedlambdavariable())), "
            "namedlambdavariable(), namedlambdavariable()))",
        ),
    ],
)
def test_zip_functions_plain_body_names_resolve_like_explicit_lambdas(spark, query, expected):
    result = spark.sql(query)
    assert result.select(expected).collect() == result.collect()
    assert result.columns == [expected]


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="The existing named_struct constructor marks literal fields nullable before map_zip_with receives them",
    strict=True,
)
def test_map_zip_with_literal_struct_key_nullability(spark):
    result = spark.sql(
        "SELECT map_zip_with(map(named_struct('x', 1), 2), map(named_struct('x', 1), 3), "
        "(k, v1, v2) -> v1 + v2) AS result"
    )
    assert result.schema[0].dataType.keyType == T.StructType([T.StructField("x", T.IntegerType(), False)])
    assert list(result.first().result.values()) == [5]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT zip_with(array(1), array(2), (x, y) -> struct(x, y))",
            "zip_with(array(1), array(2), lambdafunction(struct(namedlambdavariable(), "
            "namedlambdavariable()), namedlambdavariable(), namedlambdavariable()))",
        ),
        (
            "SELECT map_zip_with(map('a', 1), map('a', 2), (k, x, y) -> struct(x, y))",
            "map_zip_with(map(a, 1), map(a, 2), lambdafunction(struct(namedlambdavariable(), "
            "namedlambdavariable()), namedlambdavariable(), namedlambdavariable(), namedlambdavariable()))",
        ),
        (
            "SELECT zip_with(array(struct(1)), array(struct(2)), (x, y) -> x)",
            "zip_with(array(struct(1)), array(struct(2)), lambdafunction(namedlambdavariable(), "
            "namedlambdavariable(), namedlambdavariable()))",
        ),
    ],
)
def test_zip_functions_preserve_explicit_struct_column_names(spark, query, expected):
    assert spark.sql(query).columns == [expected]


def test_zip_with_preserves_connect_struct_column_name(spark):
    result = spark.range(1).select(F.zip_with(F.array(F.lit(1)), F.array(F.lit(2)), lambda x, y: F.struct(x, y)))
    assert result.columns == [
        "zip_with(array(1), array(2), lambdafunction(struct(namedlambdavariable(), "
        "namedlambdavariable()), namedlambdavariable(), namedlambdavariable()))"
    ]


@pytest.mark.parametrize("map_input", [False, True])
@pytest.mark.parametrize("capture", [False, True])
def test_zip_functions_reject_python_udfs_in_lambda_bodies(spark, map_input, capture):
    @F.udf("long")
    def identity(value):
        return value

    if map_input:
        expression = F.map_zip_with(
            F.create_map(F.lit(1), F.lit(1)),
            F.create_map(F.lit(1), F.lit(2)),
            lambda _key, left, _right: identity(F.col("id") if capture else left),
        )
    else:
        expression = F.zip_with(
            F.array(F.lit(1)),
            F.array(F.lit(2)),
            lambda left, _right: identity(F.col("id") if capture else left),
        )

    with pytest.raises(Exception, match=r"(?i)lambda function with (a )?python udf"):
        spark.range(1).select(expression).collect()


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Python UDF subexpressions in collection arguments are not extracted before null short-circuiting",
    strict=True,
)
@pytest.mark.parametrize("map_input", [False, True])
def test_zip_functions_extract_collection_python_udfs_before_null_short_circuit(spark, map_input):
    message = "zip collection Python UDF executed"

    @F.udf("int")
    def fail(_value):
        raise RuntimeError(message)

    if map_input:
        left = F.when(F.col("id") == 0, F.lit(None)).otherwise(F.create_map(F.lit(1), F.lit(1)))
        expression = F.map_zip_with(left, F.create_map(F.lit(1), fail("id")), lambda _key, left, right: left + right)
    else:
        left = F.when(F.col("id") == 0, F.lit(None)).otherwise(F.array(F.lit(1)))
        expression = F.zip_with(left, F.array(fail("id")), lambda left, right: left + right)

    with pytest.raises(Exception, match=message):
        spark.range(1).select(expression).collect()


@pytest.mark.parametrize("map_input", [False, True])
def test_zip_functions_skip_native_errors_around_python_udfs_after_null_input(spark, map_input):
    @F.udf("long")
    def identity(value):
        return value

    right_value = F.lit(10) / F.col("id") + identity("id")
    if map_input:
        expression = F.map_zip_with(
            F.lit(None).cast("map<int,double>"),
            F.create_map(F.lit(1), right_value),
            lambda _key, left, right: left + right,
        )
    else:
        expression = F.zip_with(
            F.lit(None).cast("array<double>"),
            F.array(right_value),
            lambda left, right: left + right,
        )

    assert spark.range(1).select(expression.alias("result")).first().result is None


@pytest.mark.parametrize(
    ("query", "expected_name", "expected_fields"),
    [
        (
            "SELECT zip_with(array(1), array(2), (x, y) -> (x AS first, y AS second))",
            "zip_with(array(1), array(2), lambdafunction(named_struct(first, "
            "namedlambdavariable() AS first, second, namedlambdavariable() AS second), "
            "namedlambdavariable(), namedlambdavariable()))",
            {"first": 1, "second": 2},
        ),
        (
            "SELECT map_zip_with(map(1, 2), map(1, 3), (k, x, y) -> (x AS first, y AS second))",
            "map_zip_with(map(1, 2), map(1, 3), lambdafunction(named_struct(first, "
            "namedlambdavariable() AS first, second, namedlambdavariable() AS second), "
            "namedlambdavariable(), namedlambdavariable(), namedlambdavariable()))",
            {"first": 2, "second": 3},
        ),
        (
            "SELECT zip_with(array(1), array(2), (x, y) -> struct(x AS first, y AS second))",
            "zip_with(array(1), array(2), lambdafunction(struct(namedlambdavariable() AS first, "
            "namedlambdavariable() AS second), namedlambdavariable(), namedlambdavariable()))",
            {"first": 1, "second": 2},
        ),
        (
            "SELECT zip_with(array(1), array(2), (7 AS `a b`, 8 AS `c``d`))",
            "zip_with(array(1), array(2), lambdafunction(named_struct(a b, 7 AS `a b`, "
            "c`d, 8 AS `c``d`), namedlambdavariable(), namedlambdavariable()))",
            {"a b": 7, "c`d": 8},
        ),
    ],
)
def test_zip_functions_preserve_aliased_struct_column_names(spark, query, expected_name, expected_fields):
    result = spark.sql(query)
    assert result.columns == [expected_name]
    quoted_name = "`" + expected_name.replace("`", "``") + "`"
    assert result.select(F.col(quoted_name)).collect() == result.collect()
    collection = result.first()[0]
    structs = list(collection.values()) if isinstance(collection, dict) else collection
    assert [value.asDict() for value in structs] == [expected_fields]


def test_zip_with_preserves_chained_struct_alias_display(spark):
    result = spark.range(1).select(
        F.zip_with(
            F.array(F.lit(1)),
            F.array(F.lit(2)),
            lambda x, y: F.struct(x.alias("inner").alias("outer"), y.alias("right")),
        )
    )
    expected_name = (
        "zip_with(array(1), array(2), lambdafunction(struct(namedlambdavariable() AS inner AS outer, "
        "namedlambdavariable() AS right), namedlambdavariable(), namedlambdavariable()))"
    )
    assert result.columns == [expected_name]
    assert result.select(F.col("`" + expected_name + "`")).collect() == result.collect()
    assert result.first()[0][0].asDict() == {"outer": 1, "right": 2}


@pytest.mark.parametrize(("left_nullable", "right_nullable"), [(False, False), (False, True), (True, False)])
def test_map_zip_with_case_renamed_keys_preserves_outer_nullability(spark, left_nullable, right_nullable):
    spark.conf.set("spark.sql.caseSensitive", "false")
    left_key = T.StructType([T.StructField("a", T.IntegerType(), False), T.StructField("A", T.IntegerType(), False)])
    right_key = T.StructType([T.StructField("A", T.IntegerType(), False), T.StructField("a", T.IntegerType(), False)])
    source = spark.createDataFrame(
        [({(1, 2): 10}, {(1, 2): 20})],
        T.StructType(
            [
                T.StructField("left", T.MapType(left_key, T.IntegerType(), False), left_nullable),
                T.StructField("right", T.MapType(right_key, T.IntegerType(), False), right_nullable),
            ]
        ),
    )
    result = source.select(F.map_zip_with("left", "right", lambda _key, left, right: left + right).alias("result"))
    assert result.schema[0].nullable is (left_nullable or right_nullable)
    assert result.select(F.map_values("result")).first()[0] == [30]


@pytest.mark.parametrize(("left_kind", "right_kind"), [("large", "list"), ("list", "fixed")])
def test_map_zip_with_renames_struct_keys_before_widening_array_representation(spark, tmp_path, left_kind, right_kind):
    left_struct = pa.struct([("a", pa.int32()), ("A", pa.int32())])
    right_struct = pa.struct([("A", pa.int32()), ("a", pa.int32())])
    left_key = pa.large_list(left_struct) if left_kind == "large" else pa.list_(left_struct)
    right_key = pa.list_(right_struct, 1) if right_kind == "fixed" else pa.list_(right_struct)
    path = tmp_path / "struct_array_keys.parquet"
    pq.write_table(
        pa.table(
            {
                "left": pa.array([[([{"a": 1, "A": 2}], 10)]], type=pa.map_(left_key, pa.int32())),
                "right": pa.array([[([{"A": 1, "a": 2}], 20)]], type=pa.map_(right_key, pa.int32())),
            }
        ),
        path,
    )
    previous_case_sensitive = spark.conf.get("spark.sql.caseSensitive")
    source = None
    try:
        # Materialize the Parquet fields case-sensitively before comparing keys
        # case-insensitively. Parquet preserves the Arrow list representations.
        spark.conf.set("spark.sql.caseSensitive", "true")
        source = spark.read.parquet(str(path)).cache()
        assert source.count() == 1
        source.createOrReplaceTempView("map_zip_struct_array_inputs")
        spark.conf.set("spark.sql.caseSensitive", "false")
        result = spark.table("map_zip_struct_array_inputs").select(
            F.map_values(
                F.map_zip_with(
                    "left", "right", lambda _key, left, right: F.coalesce(left, F.lit(0)) + F.coalesce(right, F.lit(0))
                )
            ).alias("result")
        )
        assert result.first().result == [30]
    finally:
        if source is not None:
            source.unpersist()
        spark.catalog.dropTempView("map_zip_struct_array_inputs")
        spark.conf.set("spark.sql.caseSensitive", previous_case_sensitive)


@pytest.mark.parametrize("nested_array", [False, True])
@pytest.mark.parametrize("right_nullable", [False, True])
def test_map_zip_with_preserves_equal_struct_key_metadata(spark, nested_array, right_nullable):
    metadata = {"meaning": "identity"}
    left_struct = T.StructType([T.StructField("a", T.IntegerType(), False, metadata)])
    right_struct = T.StructType([T.StructField("a", T.IntegerType(), right_nullable, metadata)])
    source = spark.createDataFrame(
        [((1,), (1,))],
        T.StructType([T.StructField("left", left_struct, False), T.StructField("right", right_struct, False)]),
    ).select(
        F.create_map(F.array("left") if nested_array else F.col("left"), F.lit(2)).alias("left"),
        F.create_map(F.array("right") if nested_array else F.col("right"), F.lit(3)).alias("right"),
    )
    expected_input_key = T.ArrayType(left_struct, False) if nested_array else left_struct
    assert source.schema["left"].dataType.keyType == expected_input_key

    result = source.select(F.map_zip_with("left", "right", lambda _key, left, right: left + right).alias("result"))
    expected_struct = T.StructType(
        [T.StructField("a", T.IntegerType(), right_nullable, {} if right_nullable else metadata)]
    )
    expected_key = T.ArrayType(expected_struct, False) if nested_array else expected_struct
    assert result.schema[0].dataType.keyType == expected_key
    assert result.select(F.map_values("result")).first()[0] == [5]
