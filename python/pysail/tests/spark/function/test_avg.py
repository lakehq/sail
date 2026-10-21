from decimal import Decimal

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_avg_of_decimals_rounds_half_up(spark):
    df = spark.sql("SELECT * FROM VALUES (NULL), (2.0), (3.5), (1.0) AS t(c)")
    actual = df.select(F.avg("c").alias("avg"), F.mean("c").alias("mean"))
    assert actual.schema.simpleString() == "struct<avg:decimal(6,5),mean:decimal(6,5)>"
    assert actual.collect() == [Row(avg=Decimal("2.16667"), mean=Decimal("2.16667"))]


def test_avg_of_negative_decimals_rounds_half_up(spark):
    df = spark.sql("SELECT * FROM VALUES (-2.00), (-3.50), (-1.00) AS t(c)")
    assert df.select(F.avg("c").alias("avg")).collect() == [Row(avg=Decimal("-2.166667"))]


def test_avg_of_decimals_by_group(spark):
    df = spark.sql("SELECT * FROM VALUES (1, 1.00), (1, 1.00), (1, 2.00), (2, 0.01), (2, 0.02) AS t(k, c)")
    actual = df.groupBy("k").agg(F.avg("c").alias("avg")).orderBy("k")
    assert actual.schema.simpleString() == "struct<k:int,avg:decimal(7,6)>"
    assert actual.collect() == [Row(k=1, avg=Decimal("1.333333")), Row(k=2, avg=Decimal("0.015000"))]
