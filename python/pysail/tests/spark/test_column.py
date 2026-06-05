import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType


def test_get_item_ignore_case(spark):
    df = spark.sql("SELECT struct(1 AS b) AS a")
    assert df.select(df.a.getItem("b")).collect() == [Row(**{"a.b": 1})]
    assert df.select(df.a.getItem("B")).collect() == [Row(**{"a.B": 1})]


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
