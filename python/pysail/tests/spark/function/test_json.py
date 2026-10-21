import pytest


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (
            "json_object_keys(value) AS keys",
            [(0, ["a", "b"]), (1, []), (2, None), (3, None)],
        ),
        (
            "json_tuple(value, 'a', 'b') AS (a, b)",
            [(0, "1", "text"), (1, None, None), (2, None, None), (3, None, None)],
        ),
    ],
    ids=["json_object_keys", "json_tuple"],
)
def test_json_functions(spark, expression, expected):
    df = spark.createDataFrame(
        [(0, '{"a":1,"b":"text"}'), (1, "{}"), (2, "invalid"), (3, None)],
        "id INT, value STRING",
    ).repartition(2)
    rows = df.selectExpr("id", expression).orderBy("id").collect()
    assert [tuple(row) for row in rows] == expected


def test_from_json_preserves_schema_metadata(spark):
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import LongType, StructField, StructType

    metadata = {"comment": "keep me", "id": 7, "flags": [True, False]}
    schema = StructType(
        [
            StructField(
                "nested",
                StructType([StructField("value", LongType(), True, metadata)]),
                True,
                {"parent": "keep me too"},
            )
        ]
    )
    result = spark.range(1).select(F.from_json(F.lit('{"nested":{"value":1}}'), schema).alias("result"))
    assert result.schema["result"].dataType == schema
    assert result.collect()[0].result.nested.value == 1


@pytest.mark.parametrize("same_metadata", [False, True])
@pytest.mark.parametrize(
    "expression",
    [
        "CASE WHEN id = 0 THEN a ELSE b END",
        "if(id = 0, a, b)",
        "nvl2(a, a, b)",
        "coalesce(a, b)",
        "greatest(a, b)",
        "least(a, b)",
        "array(a, b)",
    ],
)
def test_merged_json_struct_metadata(spark, expression, same_metadata):
    from pyspark.sql import Row
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType

    left = StructType([StructField("x", IntegerType(), True, {"comment": "a"})])
    right = left if same_metadata else StructType([StructField("x", IntegerType(), True, {"comment": "b"})])
    source = spark.range(2).select(
        "id",
        F.from_json(F.when(F.col("id") == 0, F.lit('{"x":1}')), left).alias("a"),
        F.from_json(F.lit('{"x":2}'), right).alias("b"),
    )
    result = source.selectExpr(f"{expression} AS result")
    result_type = result.schema["result"].dataType
    if isinstance(result_type, ArrayType):
        result_type = result_type.elementType
    # Spark creates new StructFields when merging different complex types.
    assert result_type["x"].metadata == ({"comment": "a"} if same_metadata else {})
    expected = [Row(x=1), Row(x=2)]
    if expression.startswith("greatest"):
        expected = [Row(x=2), Row(x=2)]
    elif expression.startswith("array"):
        expected = [[Row(x=1), Row(x=2)], [None, Row(x=2)]]
    assert [row.result for row in result.collect()] == expected


@pytest.mark.parametrize("schema_case", ["wider_different_metadata", "wider_same_metadata", "identical_schema"])
@pytest.mark.parametrize(
    "expression",
    [
        "coalesce(a, b)",
        "if(id = 0, a, b)",
        "CASE WHEN id = 0 THEN a ELSE b END",
        "nvl2(a, a, b)",
        "greatest(a, b)",
        "least(a, b)",
    ],
)
def test_merged_json_struct_metadata_with_numeric_coercion(spark, expression, schema_case):
    from pyspark.sql import Row
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import IntegerType, LongType, StructField, StructType

    inner = StructType([StructField("value", IntegerType(), True, {"comment": "inner"})])
    nested = StructField("nested", inner, True, {"comment": "parent"})
    left = StructType([StructField("x", IntegerType(), True, {"comment": "a"}), nested])
    right_type = IntegerType() if schema_case == "identical_schema" else LongType()
    right_comment = "b" if schema_case == "wider_different_metadata" else "a"
    right = StructType([StructField("x", right_type, True, {"comment": right_comment}), nested])
    source = spark.range(2).select(
        "id",
        F.from_json(F.when(F.col("id") == 0, F.lit('{"x":1,"nested":{"value":3}}')), left).alias("a"),
        F.from_json(F.lit('{"x":2,"nested":{"value":4}}'), right).alias("b"),
    )
    result = source.selectExpr(f"{expression} AS result")
    result_type = result.schema["result"].dataType
    # Rebuilding the outer struct clears all its field annotations, even matching
    # ones. An unchanged nested datatype retains its own child annotations.
    assert result_type["x"].metadata == ({"comment": "a"} if schema_case == "identical_schema" else {})
    assert result_type["nested"].metadata == ({"comment": "parent"} if schema_case == "identical_schema" else {})
    assert result_type["nested"].dataType["value"].metadata == {"comment": "inner"}
    expected = [Row(x=1, nested=Row(value=3)), Row(x=2, nested=Row(value=4))]
    if expression.startswith("greatest"):
        expected = [Row(x=2, nested=Row(value=4)), Row(x=2, nested=Row(value=4))]
    assert [row.result for row in result.collect()] == expected
