import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    # A secure connection can be handled by a gateway in production.
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

    @F.udf(IntegerType())
    def add_one(x):
        if x is None:
            return None
        return x + 1

    print(spark.range(-1).toPandas())
    print(spark.range(10, 0, -2, 3).toPandas())
    print(spark.createDataFrame([1, 2, 3], schema="long").toPandas())
    print(spark.createDataFrame([(1, "a"), (2, "b")], schema="a integer, t string").toPandas())

    df = spark.createDataFrame([Row(a=1, b=Row(foo="hello")), Row(a=2, b=Row(foo="world"))])
    print(df.schema)
    print(df.select(F.abs(F.col("a"))).toPandas())
    print(df.distinct().dropDuplicates(["a"]).repartition(3).repartition(2, "a").toPandas())
    print(df.select("a", "b").limit(1).toPandas())
    print(df.select(df.b["foo"]).toPandas())
    print(df.selectExpr("b.foo").toPandas())
    print(df.withColumn("c", F.col("a")).withColumn("a", F.col("b")).toPandas())
    print(df.drop("b").toPandas())
    print(df.orderBy(F.col("a").desc_nulls_first()).toPandas())
    print(df.withColumnsRenamed({"a": "c", "missing": "d"}).toPandas())
    print(df.toDF("c", "d").toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $1 > 'a'", ["b"]).toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $foo > 'a'", {"foo": "b"}).toPandas())
    print(spark.sql("SELECT 1").alias("a").select("a.*").toPandas())
    print(spark.sql("SELECT 1").alias("a").selectExpr("a.*").toPandas())
    df.createOrReplaceTempView("df")
    print(spark.sql("SELECT * FROM df").toPandas())
    # df.write.json("/tmp/df.json")
    # df = spark.read.json("/tmp/df.json/")
    # print(df.toPandas())

    # FIXME: not working
    # print(df.selectExpr("b.*").toPandas())
    # print(df.select(add_one(F.col("a"))).toPandas())
    # spark.readStream.format("rate").load().writeStream.format("console").start()

    spark.stop()
