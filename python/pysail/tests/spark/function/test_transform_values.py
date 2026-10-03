import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_transform_values_rewrites_values(spark):
    source = spark.createDataFrame([({"a": 1, "b": 2},)], "m map<string,int>")
    result = source.select(F.transform_values("m", lambda _key, value: value * 10).alias("m"))
    assert result.collect()[0].m == {"a": 10, "b": 20}


def test_transform_values_changes_value_type(spark):
    result = spark.sql("SELECT transform_values(map('a', 1), (k, v) -> CAST(v AS STRING)) AS r")
    assert result.schema == T.StructType(
        [
            T.StructField(
                "r",
                T.MapType(T.StringType(), T.StringType(), valueContainsNull=False),
                nullable=False,
            )
        ]
    )
    assert result.collect()[0].r == {"a": "1"}


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("k", {1: 1, 2: 2}),
        ("v", {1: 10, 2: 20}),
        ("k + v", {1: 11, 2: 22}),
        ("v - k", {1: 9, 2: 18}),
    ],
)
def test_transform_values_lambda_argument_binding(spark, expression, expected):
    result = spark.sql(f"SELECT transform_values(map(1, 10, 2, 20), (k, v) -> {expression}) AS r")
    assert result.collect()[0].r == expected


def test_transform_values_keeps_null_values_reachable(spark):
    result = spark.sql("SELECT transform_values(map(1, CAST(NULL AS INT)), (k, v) -> v + 1) AS r")
    assert result.collect()[0].r == {1: None}


def test_transform_values_preserves_null_and_empty_maps(spark):
    source = spark.createDataFrame(
        [({"a": 1},), ({},), (None,)],
        "m map<string,int>",
    )
    result = source.select(F.transform_values("m", lambda _key, value: value * 10).alias("m"))
    assert [row.m for row in result.collect()] == [{"a": 10}, {}, None]


def test_transform_values_does_not_evaluate_lambda_behind_null_map(spark):
    # 1 / v would raise for the null map's hidden entries if the lambda ran.
    source = spark.createDataFrame([({"a": 1},), (None,)], "m map<string,int>")
    result = source.select(F.transform_values("m", lambda _key, value: F.lit(1) / value).alias("m"))
    assert [row.m for row in result.collect()] == [{"a": 1.0}, None]


@pytest.mark.parametrize("nullable", [False, True])
def test_transform_values_preserves_map_nullability(spark, nullable):
    map_type = T.MapType(T.StringType(), T.IntegerType())
    schema = T.StructType([T.StructField("m", map_type, nullable=nullable)])
    source = spark.createDataFrame([({"a": 1},)], schema)
    result = source.select(F.transform_values("m", lambda _key, value: value + 1).alias("m"))
    assert result.schema[0].nullable == nullable
    assert result.collect()[0].m == {"a": 2}


def test_transform_values_captures_column_across_batches(spark):
    source = spark.range(9000, numPartitions=1).select(
        "id",
        F.create_map(F.lit("a"), F.col("id")).alias("m"),
    )
    result = source.select(
        "id",
        F.transform_values("m", lambda _key, value: value + F.col("id")).alias("m"),
    )
    assert [(row.id, row.m) for row in result.orderBy("id").collect()] == [(i, {"a": i * 2}) for i in range(9000)]
