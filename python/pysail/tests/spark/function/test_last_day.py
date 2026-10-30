import datetime

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row


@pytest.fixture
def ansi_disabled(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    yield
    spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.usefixtures("ansi_disabled")
def test_last_day_of_strings(spark):
    df = spark.createDataFrame([("2015-02-10 10:30:00",), ("1997/02/28 10:30:00",), ("2016-02-01",)], "s string")
    actual = df.select(F.last_day("s").alias("d"))
    assert actual.collect() == [
        Row(d=datetime.date(2015, 2, 28)),
        Row(d=None),
        Row(d=datetime.date(2016, 2, 29)),
    ]
