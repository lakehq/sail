import datetime

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import Row

VALID = "2015-02-10 10:30:45"
INVALID = "1997/02/28 10:30:00"


@pytest.fixture
def ansi_disabled(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    yield
    spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.fixture
def strings(spark):
    return spark.createDataFrame([(VALID,), (INVALID,), ("",), (None,)], "s string")


@pytest.mark.usefixtures("ansi_disabled")
def test_date_parts_of_strings_are_null_when_invalid(strings):
    actual = strings.select(
        F.year("s").alias("year"),
        F.quarter("s").alias("quarter"),
        F.month("s").alias("month"),
        F.dayofmonth("s").alias("dayofmonth"),
        F.dayofweek("s").alias("dayofweek"),
        F.dayofyear("s").alias("dayofyear"),
        F.weekofyear("s").alias("weekofyear"),
        F.hour("s").alias("hour"),
        F.minute("s").alias("minute"),
        F.second("s").alias("second"),
    )
    expected = [
        Row(
            year=2015,
            quarter=1,
            month=2,
            dayofmonth=10,
            dayofweek=3,
            dayofyear=41,
            weekofyear=7,
            hour=10,
            minute=30,
            second=45,
        ),
        *[Row(**dict.fromkeys(actual.columns)) for _ in range(3)],
    ]
    assert actual.collect() == expected


@pytest.mark.usefixtures("ansi_disabled")
def test_date_arithmetic_on_strings_is_null_when_invalid(strings):
    actual = strings.select(
        F.date_add("s", 6).alias("date_add"),
        F.date_sub("s", 6).alias("date_sub"),
        F.add_months("s", 6).alias("add_months"),
        F.datediff("s", F.lit("2015-02-01")).alias("datediff"),
        F.next_day("s", "Mon").alias("next_day"),
        F.trunc("s", "mm").alias("trunc"),
    )
    date = datetime.date
    assert actual.collect() == [
        Row(
            date_add=date(2015, 2, 16),
            date_sub=date(2015, 2, 4),
            add_months=date(2015, 8, 10),
            datediff=9,
            next_day=date(2015, 2, 16),
            trunc=date(2015, 2, 1),
        ),
        *[Row(**dict.fromkeys(actual.columns)) for _ in range(3)],
    ]


@pytest.mark.usefixtures("ansi_disabled")
def test_timestamp_functions_on_strings_are_null_when_invalid(strings):
    actual = strings.select(
        F.date_format(F.date_trunc("mm", "s"), "yyyy-MM-dd HH:mm:ss").alias("date_trunc"),
        F.months_between("s", F.lit("2015-01-10 10:30:45")).alias("months_between"),
    )
    assert actual.collect() == [
        Row(date_trunc="2015-02-01 00:00:00", months_between=1.0),
        *[Row(**dict.fromkeys(actual.columns)) for _ in range(3)],
    ]
