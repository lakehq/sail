from pyspark.sql import SparkSession, Row

if __name__ == "__main__":
    // TODO: Use TLS
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
    df = spark.createDataFrame([Row(a=1, b="hello"), Row(a=2, b="world")])
    print(df.limit(1).toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $1 > 'a'", ["b"]).toPandas())
    print(spark.sql("SELECT 1 AS text WHERE $foo > 'a'", {"foo": "b"}).toPandas())
    print(spark.sql("SELECT 1").alias("a").toPandas())
    spark.stop()
