import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


def test_get_item_ignore_case(spark):
    df = spark.sql("SELECT struct(1 AS b) AS a")
    assert df.select(df.a.getItem("b")).collect() == [Row(**{"a.b": 1})]
    assert df.select(df.a.getItem("B")).collect() == [Row(**{"a.B": 1})]


@pytest.mark.parametrize("computed", [False, True])
def test_struct_field_selector_kind(spark, computed):
    df = spark.createDataFrame([((1,), "selector")], "payload struct<selector:int>, selector string")
    payload = F.coalesce(F.col("payload"), F.col("payload")) if computed else F.col("payload")

    assert df.select(payload.getField("selector").alias("value")).collect() == [Row(value=1)]
    assert df.select(payload["selector"].alias("value")).collect() == [Row(value=1)]
    if pyspark_version() < (4,):
        pytest.skip("the Spark Connect client does not support column selectors before PySpark 4")
    with pytest.raises(AnalysisException, match=r"INVALID_EXTRACT_FIELD_TYPE|extraction must be a literal"):
        df.select(payload[F.col("selector")]).collect()


def test_struct_wildcard_after_join(spark):
    df = spark.createDataFrame(
        data=[(1, "A"), (2, "B"), (3, "C")],
        schema="id INTEGER, some_payload STRING",
    )
    df_joined = df.alias("foo").join(
        other=df.alias("bar"),
        on=F.col("foo.id").eqNullSafe(F.col("bar.id")),
        how="left",
    )

    out = df_joined.select(F.struct("foo.*").alias("some_struct"))

    assert out.schema == StructType(
        [
            StructField(
                "some_struct",
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("some_payload", StringType(), True),
                    ]
                ),
                False,
            )
        ]
    )
    assert out.collect() == [
        Row(some_struct=Row(id=1, some_payload="A")),
        Row(some_struct=Row(id=2, some_payload="B")),
        Row(some_struct=Row(id=3, some_payload="C")),
    ]


def test_struct_wildcard_on_struct_column(spark):
    df = spark.createDataFrame(
        data=[(1, "A"), (2, "B")],
        schema="id INTEGER, some_payload STRING",
    ).select(F.struct("id", "some_payload").alias("rec"))

    out = df.select(F.struct("rec.*").alias("some_struct"))

    assert out.schema == StructType(
        [
            StructField(
                "some_struct",
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("some_payload", StringType(), True),
                    ]
                ),
                False,
            )
        ]
    )
    assert out.collect() == [
        Row(some_struct=Row(id=1, some_payload="A")),
        Row(some_struct=Row(id=2, some_payload="B")),
    ]


@pytest.mark.skip(reason="not working")
def test_get_item_nested_map(spark):
    df = spark.sql("SELECT struct(map(1, 2) AS b) AS a")
    assert df.select(df.a.getItem("b").getItem(1)).collect() == [Row(**{"a.b[1]": 2})]
    df = spark.sql("SELECT map('b', map(1, 2)) AS a")
    assert df.select(df.a.getItem("b").getItem(1)).collect() == [Row(**{"a[b][1]": 2})]


def test_try_cast_invalid_date(spark):
    """Test that try_cast returns NULL for invalid date/timestamp values.

    See: https://github.com/lakehq/sail/issues/1192
    """
    # try_cast was added in Spark 4.0
    col = F.col("x")
    if not callable(getattr(col, "try_cast", None)):
        pytest.skip("try_cast not available in this Spark version")

    df = spark.createDataFrame(
        schema="id STRING, some_date STRING",
        data=[("a", "2025-99-99")],
    )
    result = df.select(
        F.col("id").try_cast("integer").alias("id"),
        F.col("some_date").try_cast("date").alias("date_col"),
        F.col("some_date").try_cast("timestamp").alias("ts_col"),
        F.col("some_date").try_cast("timestamp_ntz").alias("ts_ntz_col"),
    ).collect()

    assert result == [Row(id=None, date_col=None, ts_col=None, ts_ntz_col=None)]


def test_array_struct_field(spark):
    df = spark.createDataFrame(
        data=[
            ("0", [{"b": 42, "c": {"d": 100.0}}]),
            ("1", [{"b": 1, "c": None}, {"b": -1}]),
            ("2", [None, {"b": 3, "c": {"d": None}}, {"b": None}]),
            ("3", None),
        ],
        schema="id: string, a: array<struct<b: int, c: struct<d: double>>>",
    )
    actual = df.select("id", F.col("a.b")).collect()
    assert sorted(actual, key=lambda row: row.id) == [
        Row(id="0", b=[42]),
        Row(id="1", b=[1, -1]),
        Row(id="2", b=[None, 3, None]),
        Row(id="3", b=None),
    ]

    actual = df.select("id", F.col("a.c.d")).collect()
    assert sorted(actual, key=lambda row: row.id) == [
        Row(id="0", d=[100.0]),
        Row(id="1", d=[None, None]),
        Row(id="2", d=[None, None, None]),
        Row(id="3", d=None),
    ]


def test_wide_qualified_nested_projection(spark):
    width = 128
    source = spark.createDataFrame([(1,), (2,), (None,)], "value int")
    source = source.select(
        F.struct(F.struct(F.lit(-1).alias("value")).alias("s0")).alias("origin"),
        *[F.struct("value").alias(f"s{i}") for i in range(width)],
    ).alias("origin")
    # The matching roots span the schema. The qualifier also names a struct,
    # whose nested value must not override the qualified column's value.
    result = source.select(*[F.col(f"origin.s{i}.value").alias(f"v{i}") for i in range(width)])
    assert result.schema == StructType([StructField(f"v{i}", IntegerType(), True) for i in range(width)])
    assert sorted((tuple(row) for row in result.collect()), key=lambda row: row[0] or 0) == [
        tuple([value] * width) for value in (None, 1, 2)
    ]


def test_recovered_struct_field_respects_case_sensitive_resolution(spark):
    previous = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        source = spark.createDataFrame([((1,), 7)], "s struct<x:int>, a int")
        projected = source.select(F.struct(F.lit(2).alias("X")).alias("s"), "a")
        with pytest.raises(AnalysisException):
            projected.where("s.x = 1").collect()
        assert projected.select("a").where("s.x = 1").collect() == [Row(a=7)]
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


@pytest.mark.parametrize(
    "replacement",
    [
        lambda: F.create_map(F.lit("a"), F.struct(F.lit(2).alias("y"))),
        lambda: F.create_map(F.lit("a"), F.lit(2)),
        lambda: F.array(F.lit(2)),
    ],
    ids=["map-struct", "map-scalar", "array-scalar"],
)
def test_recovered_struct_field_skips_failed_extraction_after_map_or_array(spark, replacement):
    source = spark.createDataFrame([(((1,),), 7), (((2,),), 8)], "s struct<a:struct<x:int>>, marker int")
    projected = source.select(replacement().alias("s"), "marker").select("marker")
    assert projected.where("s.a.x = 1").collect() == [Row(marker=7)]


def test_recovered_struct_field_preserves_successful_extraction_after_array(spark):
    source = spark.createDataFrame([(((1,),), 7)], "s struct<a:struct<x:int>>, marker int")
    replacement = F.array(F.create_map(F.lit("a"), F.struct(F.lit(2).alias("y"))))
    projected = source.select(replacement.alias("s"), "marker").select("marker")
    with pytest.raises(AnalysisException):
        projected.where("s.a.x = 1").collect()


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="NullType extraction propagation was added in Spark 4.2")
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail follows Spark 3.5-4.1 recovery; Spark 4.2 propagates NULL through the newer map value",
    strict=True,
)
def test_recovered_null_map_preserves_spark_42_null_propagation(spark):
    source = spark.createDataFrame([(((1,),), 7)], "s struct<a:struct<x:int>>, marker int")
    projected = source.select(F.create_map(F.lit("a"), F.lit(None)).alias("s"), "marker").select("marker")
    assert projected.where("s.a.x = 1").collect() == []
