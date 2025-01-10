import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType


@pytest.fixture(scope="module")
def udf_add_one():
    @F.udf(IntegerType())
    def add_one(x):
        if x is None:
            return None
        return x + 1

    return add_one


@pytest.fixture(scope="module")
def udf_add_x_y():
    @F.udf(IntegerType())
    def add_x_y(s):
        x, y = s["x"], s["y"]
        if x is None or y is None:
            return None
        return x + y

    return add_x_y


@pytest.fixture(scope="module")
def udf_add():
    @F.udf(IntegerType())
    def add(x, y):
        return x + y

    return add


@pytest.fixture(scope="module")
def df(sail):
    return sail.createDataFrame(
        [Row(a=1, b=Row(foo="hello")), Row(a=2, b=Row(foo="world"))],
        schema="a integer, b struct<foo: string>",
    )


@pytest.fixture(scope="module")
def df_view(sail, df):
    name = "df"
    df.createOrReplaceTempView(name)
    yield name
    sail.catalog.dropTempView(name)


def test_data_frame_schema(df):
    assert df.schema == StructType(
        [
            StructField("a", IntegerType(), True),
            StructField("b", StructType([StructField("foo", StringType(), True)]), True),
        ]
    )


def test_range(sail):
    assert_frame_equal(sail.range(-1).toPandas(), pd.DataFrame({"id": []}, dtype="int64"))
    assert_frame_equal(
        sail.range(10, 0, -2, 3).toPandas().sort_values("id").reset_index(drop=True),
        pd.DataFrame({"id": [2, 4, 6, 8, 10]}, dtype="int64"),
    )


def test_create_data_frame(sail):
    assert_frame_equal(
        sail.createDataFrame([1, 2, 3], schema="long").toPandas(), pd.DataFrame({"value": [1, 2, 3]}, dtype="int64")
    )
    assert_frame_equal(
        sail.createDataFrame([(1, "a"), (2, "b")], schema="a integer, t string").toPandas(),
        pd.DataFrame({"a": [1, 2], "t": ["a", "b"]}).astype({"a": "int32"}),
    )


def test_schema_simple_string(sail):
    df = sail.range(1).selectExpr("struct(id, 1, 2.0D AS foo, id) as struct")
    assert df.schema.simpleString() == "struct<struct:struct<id:bigint,col2:int,foo:double,id:bigint>>"


def test_data_frame_operations(df):
    assert_frame_equal(
        df.selectExpr("struct(a, b, 1 AS c, 2)").toPandas(),
        pd.DataFrame(
            {
                "struct(a, b, c, 2)": [
                    {"a": 1, "b": {"foo": "hello"}, "c": 1, "col4": 2},
                    {"a": 2, "b": {"foo": "world"}, "c": 1, "col4": 2},
                ],
            }
        ),
    )

    assert_frame_equal(
        df.select(F.create_map(F.col("a"), df.b["foo"], F.lit(10), F.lit("foo"))).toPandas(),
        pd.DataFrame(
            {
                "map(a, b.foo, 10, foo)": [
                    {1: "hello", 10: "foo"},
                    {2: "world", 10: "foo"},
                ],
            }
        ),
    )

    assert_frame_equal(
        df.select(F.abs(F.col("a"))).toPandas(),
        pd.DataFrame(
            {
                "abs(a)": [1, 2],
            },
            dtype="int32",
        ),
    )

    assert_frame_equal(
        df.distinct()
        .dropDuplicates(["a"])
        .repartition(3)
        .repartition(2, "a")
        .toPandas()
        .sort_values("a")
        .reset_index(drop=True),
        pd.DataFrame(
            {
                "a": [1, 2],
                "b": [{"foo": "hello"}, {"foo": "world"}],
            }
        ).astype({"a": "int32"}),
    )

    assert_frame_equal(
        df.select("a", "b").limit(1).toPandas(),
        pd.DataFrame(
            {
                "a": [1],
                "b": [{"foo": "hello"}],
            }
        ).astype({"a": "int32"}),
    )

    assert_frame_equal(
        df.select(df.b["foo"]).toPandas(),
        pd.DataFrame(
            {
                "b.foo": ["hello", "world"],
            }
        ),
    )

    assert_frame_equal(
        df.withColumn("c", F.col("a")).withColumn("a", F.col("b")).toPandas(),
        pd.DataFrame(
            {
                "a": [{"foo": "hello"}, {"foo": "world"}],
                "b": [{"foo": "hello"}, {"foo": "world"}],
                "c": [1, 2],
            }
        ).astype({"c": "int32"}),
    )

    assert_frame_equal(
        df.drop("b").toPandas(),
        pd.DataFrame(
            {
                "a": [1, 2],
            }
        ).astype({"a": "int32"}),
    )

    assert_frame_equal(
        df.orderBy(F.col("a").desc_nulls_first()).toPandas(),
        pd.DataFrame(
            {
                "a": [2, 1],
                "b": [{"foo": "world"}, {"foo": "hello"}],
            }
        ).astype({"a": "int32"}),
    )

    assert_frame_equal(
        df.withColumnsRenamed({"a": "c", "missing": "d"}).toPandas(),
        pd.DataFrame(
            {
                "c": [1, 2],
                "b": [{"foo": "hello"}, {"foo": "world"}],
            }
        ).astype({"c": "int32"}),
    )

    assert_frame_equal(
        df.toDF("c", "d").toPandas(),
        pd.DataFrame(
            {
                "c": [1, 2],
                "d": [{"foo": "hello"}, {"foo": "world"}],
            }
        ).astype({"c": "int32"}),
    )


def test_sql(sail):
    assert_frame_equal(
        sail.sql("SELECT 1").alias("a").select("a.*").toPandas(), pd.DataFrame({"1": [1]}, dtype="int32")
    )
    assert_frame_equal(
        sail.sql("SELECT 1").alias("a").selectExpr("a.*").toPandas(), pd.DataFrame({"1": [1]}, dtype="int32")
    )


def test_sql_temp_view(sail, df, df_view):
    assert_frame_equal(sail.sql(f"SELECT * FROM {df_view}").toPandas(), df.toPandas())  # noqa: S608


def test_write(sail, df, tmpdir):
    path = str(tmpdir.join("df.json"))
    df.write.json(path)
    out = sail.read.json(path)
    assert_frame_equal(df.toPandas(), out.toPandas(), check_dtype=False)


def test_explode(sail):
    assert_frame_equal(
        sail.createDataFrame(
            [
                Row(a=[1, 2, None], b={"m": 3.0, "n": 4.0}, c="foo"),
                Row(a=[], b={"p": 1.0}, c="bar"),
                Row(a=None, b={"q": 2.0}, c="baz"),
            ]
        )
        .select(
            (F.explode_outer("a") + F.lit(1)).alias("d"),
            F.posexplode(F.col("a")),
            F.col("c"),
        )
        .toPandas(),
        pd.DataFrame(
            {
                "d": [2, 2, 2, 3, 3, 3, None, None, None],
                "pos": [0, 1, 2] * 3,
                "col": [1, 2, None] * 3,
                "c": ["foo"] * 9,
            }
        ).astype({"pos": "int32"}),
    )

    assert_frame_equal(
        sail.createDataFrame([Row(a=[[1, 2], None], b={"m": 3.0, "n": 4.0})])
        .select(
            F.posexplode_outer(F.explode("a")).alias("p", "a"),
            F.posexplode("b").alias("q", "k", "v"),
        )
        .toPandas(),
        pd.DataFrame(
            {
                "p": [0, 0, 1, 1, None, None],
                "a": [1, 1, 2, 2, None, None],
                "q": [0, 1] * 3,
                "k": ["m", "n"] * 3,
                "v": [3.0, 4.0] * 3,
            }
        ).astype({"q": "int32"}),
    )


def test_udf(df, udf_add_one, udf_add_x_y, udf_add):
    assert_frame_equal(
        df.limit(1).select(udf_add_one(F.col("a"))).toPandas(), pd.DataFrame({"add_one(a)": [2]}, dtype="int32")
    )
    assert_frame_equal(
        df.select(udf_add_one(F.col("a"))).toPandas(), pd.DataFrame({"add_one(a)": [2, 3]}, dtype="int32")
    )
    assert_frame_equal(
        df.withColumn("x", F.col("a"))
        .withColumn("y", F.col("a"))
        .select(udf_add_x_y(F.struct(F.col("x"), F.col("y"))))
        .toPandas(),
        pd.DataFrame({"add_x_y(struct(x, y))": [2, 4]}, dtype="int32"),
    )
    assert_frame_equal(
        df.select(udf_add(F.col("a"), F.col("a"))).toPandas(), pd.DataFrame({"add(a, a)": [2, 4]}, dtype="int32")
    )


def test_sql_with_clause(sail, df, df_view):
    assert_frame_equal(
        sail.sql(f"WITH test AS (SELECT * FROM {df_view}) SELECT * FROM test").toPandas(),  # noqa: S608
        df.toPandas(),
    )


def test_sql_parameters(sail):
    actual = sail.sql("SELECT 1 AS text WHERE $1 > 'a'", ["b"]).toPandas()
    expected = pd.DataFrame({"text": [1]}).astype({"text": "int32"})
    assert_frame_equal(actual, expected)
    actual = sail.sql("SELECT 1 AS text WHERE $foo > 'a'", {"foo": "b"}).toPandas()
    expected = pd.DataFrame({"text": [1]}).astype({"text": "int32"})
    assert_frame_equal(actual, expected)


def test_save_table(df):
    df.write.saveAsTable("meow")


def test_select_expression(df):
    assert_frame_equal(df.selectExpr("b.foo").toPandas(), pd.DataFrame({"foo": ["hello", "world"]}))
    assert_frame_equal(df.selectExpr("b.*").toPandas(), pd.DataFrame({"foo": ["hello", "world"]}))


@pytest.mark.skip(reason="not implemented")
def test_stream(sail):
    sail.readStream.format("rate").load().writeStream.format("console").start()
