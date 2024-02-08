from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


if __name__ == "__main__":
    # A secure connection can be handled by a gateway in production.
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

    @F.udf(IntegerType())
    def add_one(x):
        if x is None:
            return None
        return x + 1

    df = spark.createDataFrame([Row(a=1, b=Row(foo="hello")), Row(a=2, b=Row(foo="world"))])
    print(df.select("a", "b").limit(1).toPandas())
    print(df.selectExpr("b.foo").toPandas())
    print(df.orderBy(F.col("a").desc_nulls_first()).toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $1 > 'a'", ["b"]).toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $foo > 'a'", {"foo": "b"}).toPandas())
    print(spark.sql("SELECT 1").alias("a").select("a.*").toPandas())

    # FIXME: not working
    # print(df.selectExpr("b.*").toPandas())
    # print(df.select(add_one(F.col("a"))).toPandas())
    # spark.readStream.format("rate").load().writeStream.format("console").start()

    spark.stop()
