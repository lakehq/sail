import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    # A secure connection can be handled by a gateway in production.
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)


    @F.udf(IntegerType())
    def add_one(x):
        if x is None:
            return None
        return x + 1


    @F.udf(IntegerType())
    def add_x_y(structured_data):
        x, y = structured_data['x'], structured_data['y']
        if x is None or y is None:
            return None
        return x + y


    @F.udf(IntegerType())
    def add(x, y):
        return x + y


    print(spark.range(-1).toPandas())
    print(spark.range(10, 0, -2, 3).toPandas())
    print(spark.createDataFrame([1, 2, 3], schema="long").toPandas())
    print(spark.createDataFrame([(1, "a"), (2, "b")], schema="a integer, t string").toPandas())

    df = spark.range(1).selectExpr("struct(id, 1, 2 AS foo, id) as struct")
    print(df.schema.simpleString())

    df = spark.createDataFrame(
        [Row(a=1, b=Row(foo="hello")), Row(a=2, b=Row(foo="world"))],
        schema="a integer, b struct<foo: string>",
    )
    print(df.selectExpr("struct(a, b, 1 AS c, 2)").toPandas())
    print(df.select(F.create_map(F.col("a"), df.b["foo"], F.lit(10), F.lit("foo"))).toPandas())
    print(df.schema)
    print(df.select(F.abs(F.col("a"))).toPandas())
    print(df.distinct().dropDuplicates(["a"]).repartition(3).repartition(2, "a").toPandas())
    print(df.select("a", "b").limit(1).toPandas())
    print(df.select(df.b["foo"]).toPandas())
    print(df.withColumn("c", F.col("a")).withColumn("a", F.col("b")).toPandas())
    print(df.drop("b").toPandas())
    print(df.orderBy(F.col("a").desc_nulls_first()).toPandas())
    print(df.withColumnsRenamed({"a": "c", "missing": "d"}).toPandas())
    print(df.toDF("c", "d").toPandas())
    print(spark.sql("SELECT 1").alias("a").select("a.*").toPandas())
    print(spark.sql("SELECT 1").alias("a").selectExpr("a.*").toPandas())
    df.createOrReplaceTempView("df")
    print(spark.sql("SELECT * FROM df").toPandas())
    # df.write.json("/tmp/df.json")
    # df = spark.read.json("/tmp/df.json/")
    # print(df.toPandas())

    print(
        spark.createDataFrame(
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
        .toPandas()
    )
    print(
        spark.createDataFrame([Row(a=[[1, 2], None], b={"m": 3.0, "n": 4.0})])
        .select(
            F.posexplode_outer(F.explode("a")).alias("p", "a"),
            F.posexplode("b").alias("q", "k", "v"),
        )
        .toPandas()
    )

    print(df.limit(1).select(add_one(F.col("a"))).toPandas())
    print(df.select(add_one(F.col("a"))).toPandas())
    print(df.withColumn("x", F.col("a")).withColumn("y", F.col("a")).select(
        add_x_y(F.struct(F.col("x"), F.col("y")))).toPandas())
    print(df.select(add(F.col("a"), F.col("a"))).toPandas())

    # FIXME: not working
    # print(spark.sql("SELECT 1 AS text WHERE $1 > 'a'", ["b"]).toPandas())
    # print(spark.sql("SELECT 1 AS text WHERE $foo > 'a'", {"foo": "b"}).toPandas())
    # print(df.selectExpr("b.foo").toPandas())
    # print(df.selectExpr("b.*").toPandas())
    # spark.readStream.format("rate").load().writeStream.format("console").start()
    # df.write.saveAsTable("meow")
    # expression: contains
    # expression: LiteralType::Array
    # expression: LiteralType::Struct
    # expression: LiteralType::Map

    spark.stop()
