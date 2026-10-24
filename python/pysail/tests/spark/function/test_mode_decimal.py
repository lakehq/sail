from decimal import Decimal

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_mode_of_decimals(spark):
    df = spark.sql("SELECT * FROM VALUES (0, NULL), (1, 2.0), (1, 3.5), (0, 1.0), (1, 2.0) AS tab(a, c)")
    assert df.schema["c"].dataType.simpleString() == "decimal(2,1)"
    actual = df.select(F.mode("c").alias("mode"))
    assert actual.schema["mode"].dataType.simpleString() == "decimal(2,1)"
    assert actual.collect() == [Row(mode=Decimal("2.0"))]
    grouped = df.groupBy("a").agg(F.mode("c").alias("mode")).orderBy("a")
    assert grouped.collect() == [Row(a=0, mode=Decimal("1.0")), Row(a=1, mode=Decimal("2.0"))]


def test_mode_of_wide_decimals(spark):
    df = spark.sql("SELECT CAST(c AS DECIMAL(38, 10)) AS c FROM VALUES (1.5), (2.5), (2.5) AS tab(c)")
    assert df.select(F.mode("c").alias("mode")).collect() == [Row(mode=Decimal("2.5000000000"))]
