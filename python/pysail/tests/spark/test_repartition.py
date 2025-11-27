import pandas as pd
import pyspark.sql.functions as F  # noqa: N812


def partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


def test_explicit_repartition(spark):
    assert partition_count(spark.range(0, 10, 1, 2)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 3).select("id", F.lit("foo").alias("a")).repartition("a")) == 3  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(5)) == 5  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(6, "b", "a")) == 6  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition("a")) == 1


def test_explicit_coalesce(spark):
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(1)) == 1
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(3)) == 3  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 4).coalesce(2)) == 2  # noqa: PLR2004
