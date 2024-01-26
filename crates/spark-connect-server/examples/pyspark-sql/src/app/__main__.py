from pyspark.sql import SparkSession, Row

if __name__ == "__main__":
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
    df = spark.createDataFrame([Row(a=1, b="hello")])
    df.show()
    spark.stop()
