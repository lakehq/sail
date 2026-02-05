import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal

from pysail.tests.spark.utils import escape_sql_string_literal, is_jvm_spark


@pytest.fixture(autouse=True)
def tables(spark, tmp_path):
    location = str(tmp_path / "t1")
    spark.sql(f"CREATE TABLE t1 (id LONG, name STRING, age LONG) LOCATION '{escape_sql_string_literal(location)}'")
    yield
    spark.sql("DROP TABLE t1")
    spark.sql("DROP TABLE IF EXISTS t2")
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t4")
    spark.sql("DROP TABLE IF EXISTS t5")


def test_insert_into_with_values(spark):
    spark.sql("INSERT INTO t1 VALUES (101, 'Alice', 20)")
    df = spark.createDataFrame([(102, "Bob", 30)])
    df.write.insertInto("t1")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (101, 102) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [101, 102], "name": ["Alice", "Bob"], "age": [20, 30]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_by_position(spark):
    # The field names are ignored.
    spark.sql("INSERT INTO t1 SELECT 201 AS age, 'Alice' AS name, 21 AS id")
    df = spark.createDataFrame([(202, "Bob", 31)], schema=["age", "name", "id"])
    df.write.insertInto("t1")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (201, 202) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [201, 202], "name": ["Alice", "Bob"], "age": [21, 31]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_by_name(spark):
    spark.sql("INSERT INTO t1 (age, name, id) VALUES (21, 'Alice', 301)")
    spark.sql("INSERT INTO t1 BY NAME SELECT 31 AS age, 'Bob' AS name, 302 AS id")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (301, 302) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [301, 302], "name": ["Alice", "Bob"], "age": [21, 31]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_with_invalid_options(spark):
    df = spark.createDataFrame([(101, "Alice", 20)])

    with pytest.raises(Exception, match="partition"):
        df.write.partitionBy("id").insertInto("t1")


@pytest.mark.skipif(not is_jvm_spark(), reason="`INSERT OVERWRITE` is not supported in Sail yet")
def test_insert_overwrite(spark):
    spark.sql("INSERT INTO t1 VALUES (401, 'Alice', 22)")
    spark.sql("INSERT OVERWRITE t1 VALUES (402, 'Bob', 32)")
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [402], "name": ["Bob"], "age": [32]},
    )
    assert_frame_equal(actual, expected)

    df = spark.createDataFrame([(403, "Alice", 25)])
    df.write.insertInto("t1", overwrite=True)
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [403], "name": ["Alice"], "age": [25]},
    )
    assert_frame_equal(actual, expected)

    df = spark.createDataFrame([(404, "Bob", 35)])
    df.write.mode("overwrite").insertInto("t1")
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [404], "name": ["Bob"], "age": [35]},
    )
    assert_frame_equal(actual, expected)


def test_save_as_table(spark, tmp_path):
    location = str(tmp_path / "t2")
    df = spark.createDataFrame([(1001, "Alice")], schema="id LONG, name STRING")

    def expected(n: int):
        return pd.DataFrame(
            {"id": [1001] * n, "name": ["Alice"] * n},
        )

    df.write.saveAsTable("t2", path=location)
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(1))

    if not is_jvm_spark():
        # The "ignore" mode seems broken in Spark Connect.
        df.write.saveAsTable("t2", mode="ignore", path=location)
        actual = spark.sql("SELECT * FROM t2").toPandas()
        assert_frame_equal(actual, expected(1))

    with pytest.raises(Exception, match=r".*"):
        df.write.saveAsTable("t2", mode="error", location=location)

    with pytest.raises(Exception, match=r".*"):
        df.write.saveAsTable("t2", location=location)

    df.write.saveAsTable("t2", mode="append")
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(2))

    # `saveAsTable` matches columns by name.
    df.select("name", "id").write.saveAsTable("t2", mode="append")
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(3))

    with pytest.raises(Exception, match=r".*"):
        df.select(F.col("name").alias("n"), "id").write.saveAsTable("t2", mode="append")

    with pytest.raises(Exception, match=r".*"):
        df.select("id").write.saveAsTable("t2", mode="append")

    if is_jvm_spark():
        # The "overwrite" mode is not supported in Sail yet.
        df.write.saveAsTable("t2", mode="overwrite", path=location)
        actual = spark.sql("SELECT * FROM t2").toPandas()
        assert_frame_equal(actual, expected(1))


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
def test_write_to(spark, tmp_path):
    location = str(tmp_path / "t3")
    df = spark.createDataFrame([(2002, "Bob")], schema="id LONG, name STRING")

    def expected(n: int):
        return pd.DataFrame(
            {"id": [2002] * n, "name": ["Bob"] * n},
        )

    with pytest.raises(Exception, match=r".*"):
        # The table does not exist yet.
        df.writeTo("t3").option("location", location).replace()

    df.writeTo("t3").option("path", location).create()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    df.writeTo("t3").append()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(2))

    pytest.skip("replace and overwrite are not supported in Sail yet")

    df.writeTo("t3").option("location", location).replace()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    df.writeTo("t3").option("path", location).createOrReplace()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    df.writeTo("t3").option("location", location).overwrite(F.lit(True))
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))


@pytest.mark.skipif(not is_jvm_spark(), reason="the options overwrite logic is not fully compatible yet")
def test_write_options(spark, tmp_path):
    location = str(tmp_path / "t4")
    spark.sql(f"""
        CREATE TABLE t4 (a STRING, b STRING) USING CSV
        LOCATION '{escape_sql_string_literal(location)}'
        OPTIONS (header 'false', delimiter '|')
    """)

    df = spark.createDataFrame([("1-2", "3")], schema="a STRING, b STRING")
    # Spark requires `.format()` to match the table definition.
    # If the format is not specified, the default format is used, and an exception is raised
    # if it does not match the table definition.
    df.write.format("csv").saveAsTable("t4", mode="append")

    df = spark.createDataFrame([("1|2", "3")], schema="a STRING, b STRING")
    df.write.format("csv").option("delimiter", "-").saveAsTable("t4", mode="append")
    df.write.format("csv").option("SEP", "-").saveAsTable("t4", mode="append")

    # Spark rewrites the entire table with the new options.
    assert sorted(spark.read.format("csv").table("t4").collect()) == [
        ("1-2", "3"),
        ("1|2", "3"),
        ("1|2", "3"),
    ]
    # Spark ignores the options if it conflicts with the table definition.
    assert sorted(spark.read.format("csv").option("delimiter", "|").table("t4").collect()) == [
        ("1-2", "3"),
        ("1|2", "3"),
        ("1|2", "3"),
    ]


def test_write_with_partition_columns(spark, tmp_path):
    location = str(tmp_path / "t5")
    spark.sql(f"""
        CREATE TABLE t5 (a INT, b STRING)
        USING PARQUET
        LOCATION '{escape_sql_string_literal(location)}'
        PARTITIONED BY (a)
    """)
    df = spark.createDataFrame([(42, "foo")], schema="a INT, b STRING")
    df.write.partitionBy("a").format("parquet").saveAsTable("t5", mode="append")

    with pytest.raises(Exception, match=r".*"):
        df.write.partitionBy("b").format("parquet").saveAsTable("t5", mode="append")

    assert spark.read.format("parquet").table("t5").select("a", "b").collect() == [
        (42, "foo"),
    ]

    if is_jvm_spark():
        pytest.skip("Spark cannot infer partition columns when reading paths")

    assert spark.read.parquet(location).select("a", "b").collect() == [
        ("42", "foo"),
    ]
