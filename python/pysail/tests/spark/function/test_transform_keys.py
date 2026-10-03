import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_transform_keys_rewrites_keys(spark):
    source = spark.createDataFrame([({"a": 1, "b": 2},)], "m map<string,int>")
    result = source.select(F.transform_keys("m", lambda key, _value: F.upper(key)).alias("m"))
    assert result.collect()[0].m == {"A": 1, "B": 2}


def test_transform_keys_changes_key_type(spark):
    result = spark.sql("SELECT transform_keys(map('1', 10), (k, v) -> CAST(k AS INT)) AS r")
    assert result.schema == T.StructType(
        [
            T.StructField(
                "r",
                T.MapType(T.IntegerType(), T.IntegerType(), valueContainsNull=False),
                nullable=False,
            )
        ]
    )
    assert result.collect()[0].r == {1: 10}


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("k + 1", {2: 10, 3: 20}),
        ("v", {10: 10, 20: 20}),
        ("k + v", {11: 10, 22: 20}),
    ],
)
def test_transform_keys_lambda_argument_binding(spark, expression, expected):
    result = spark.sql(f"SELECT transform_keys(map(1, 10, 2, 20), (k, v) -> {expression}) AS r")
    assert result.collect()[0].r == expected


def test_transform_keys_preserves_null_values(spark):
    result = spark.sql("SELECT transform_keys(map(1, CAST(NULL AS INT)), (k, v) -> k + 1) AS r")
    assert result.collect()[0].r == {2: None}


def test_transform_keys_preserves_null_and_empty_maps(spark):
    source = spark.createDataFrame(
        [({"a": 1},), ({},), (None,)],
        "m map<string,int>",
    )
    result = source.select(F.transform_keys("m", lambda key, _value: F.upper(key)).alias("m"))
    assert [row.m for row in result.collect()] == [{"A": 1}, {}, None]


def test_transform_keys_does_not_evaluate_lambda_behind_null_map(spark):
    # 1 / v would raise for the null map's hidden entries if the lambda ran.
    source = spark.createDataFrame([({"a": 2},), (None,)], "m map<string,int>")
    result = source.select(F.transform_keys("m", lambda _key, value: F.lit(1) / value).alias("m"))
    assert [row.m for row in result.collect()] == [{0.5: 2}, None]


def test_transform_keys_keeps_last_value_when_dedup_policy_allows(spark):
    source = spark.range(1).select(F.create_map(F.lit(1), F.lit(10), F.lit(3), F.lit(30)).alias("m"))
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    try:
        result = source.select(F.transform_keys("m", lambda key, _value: key % 2).alias("m"))
        assert result.collect()[0].m == {1: 30}
    finally:
        spark.conf.unset("spark.sql.mapKeyDedupPolicy")


@pytest.mark.parametrize("nullable", [False, True])
def test_transform_keys_preserves_map_nullability(spark, nullable):
    map_type = T.MapType(T.StringType(), T.IntegerType())
    schema = T.StructType([T.StructField("m", map_type, nullable=nullable)])
    source = spark.createDataFrame([({"a": 1},)], schema)
    result = source.select(F.transform_keys("m", lambda key, _value: F.upper(key)).alias("m"))
    assert result.schema[0].nullable == nullable
    assert result.collect()[0].m == {"A": 1}


def test_transform_keys_captures_column_across_batches(spark):
    source = spark.range(9000, numPartitions=1).select(
        "id",
        F.create_map(F.lit("a"), F.col("id")).alias("m"),
    )
    result = source.select(
        "id",
        F.transform_keys("m", lambda key, _value: F.concat(key, F.col("id").cast("string"))).alias("m"),
    )
    assert [(row.id, row.m) for row in result.orderBy("id").collect()] == [(i, {f"a{i}": i}) for i in range(9000)]
