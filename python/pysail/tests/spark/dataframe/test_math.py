import datetime
from decimal import Decimal

import pytest
from pyspark.errors import ArithmeticException, PySparkException
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version
from pysail.testing.spark.utils.sql import any_of


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone"),
    [("America/Los_Angeles", "America/Los_Angeles")],
    indirect=["session_timezone", "local_timezone"],
)
def test_plus(spark, session_timezone, local_timezone):  # noqa: ARG001
    assert spark.sql("SELECT 1 + 2 AS result").collect() == [Row(result=3)]
    assert spark.sql("SELECT DATE'2025-02-26' + INTERVAL '2' MONTH AS result").collect() == [
        Row(result=datetime.date(2025, 4, 26))
    ]
    assert spark.sql("SELECT TIMESTAMP'2025-02-26 12:15:29' + INTERVAL '3' SECOND AS result").collect() == [
        Row(result=datetime.datetime(2025, 2, 26, 12, 15, 32))  # noqa: DTZ001
    ]
    assert spark.sql("SELECT DATE'2025-03-31' + INTERVAL '1' MONTH AS result").collect() == [
        Row(result=datetime.date(2025, 4, 30))
    ]
    assert spark.sql("SELECT DATE'2025-02-26' + 1 AS result").collect() == [Row(result=datetime.date(2025, 2, 27))]
    assert spark.sql("SELECT 1 + DATE'2025-02-26' AS result").collect() == [Row(result=datetime.date(2025, 2, 27))]


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone"),
    [("America/Los_Angeles", "America/Los_Angeles")],
    indirect=["session_timezone", "local_timezone"],
)
def test_minus(spark, session_timezone, local_timezone):  # noqa: ARG001
    assert spark.sql("SELECT 2 - 1 AS result").collect() == [Row(result=1)]
    assert spark.sql("SELECT TIMESTAMP'2025-02-26 12:15:29' - INTERVAL '3' SECOND AS result").collect() == [
        Row(result=datetime.datetime(2025, 2, 26, 12, 15, 26))  # noqa: DTZ001
    ]
    assert spark.sql("SELECT DATE'2025-03-31' - INTERVAL '1' MONTH AS result").collect() == [
        Row(result=datetime.date(2025, 2, 28))
    ]
    assert spark.sql("SELECT DATE'2025-02-26' - 1 AS result").collect() == [Row(result=datetime.date(2025, 2, 25))]
    with pytest.raises(Exception):  # noqa: B017,PT011
        spark.sql("SELECT 1 - DATE'2025-02-26' AS result").collect()


def test_multiply(spark):
    assert spark.sql("SELECT 3 * 2 AS result").collect() == [Row(result=6)]
    assert spark.sql("SELECT 2L * 2L AS result").collect() == [Row(result=4)]
    if not is_jvm_spark():
        pytest.skip("the following tests are not supported in Sail yet")
    assert spark.sql("SELECT INTERVAL '3:15' HOUR TO MINUTE * 3 AS result").collect() == [
        Row(result=datetime.timedelta(seconds=35100))
    ]
    assert spark.sql("SELECT INTERVAL '3' YEAR * 3 AS result").collect() == [Row(result=108)]


def test_divide(spark):
    assert spark.sql("SELECT 3 / 2 AS result").collect() == [Row(result=1.5)]
    assert spark.sql("SELECT 2L / 2L AS result").collect() == [Row(result=1.0)]
    assert spark.sql("SELECT 6 / Decimal(3) AS result").collect() == [Row(result=Decimal("2.00000000000"))]
    # TODO: In the following tests, the expected values differ when running on JVM Spark and Sail.
    assert spark.sql("SELECT Decimal(6) / 3 AS result").collect() == [
        Row(result=any_of(Decimal("2.000000"), Decimal("2.00000000000")))
    ]
    assert spark.sql("SELECT Decimal(3) / Decimal(2) AS result").collect() == [
        Row(result=any_of(Decimal("1.50000000000"), Decimal("1.50000")))
    ]
    assert spark.sql("SELECT Decimal(3) / 2 AS result").collect() == [
        Row(result=any_of(Decimal("1.500000"), Decimal("1.50000")))
    ]
    assert spark.sql("SELECT 2 / Decimal(3) AS result").collect() == [
        Row(result=any_of(Decimal("0.66666666667"), Decimal("0.6666")))
    ]
    assert spark.sql("SELECT 2.0 / 3.0 AS result").collect() == [
        Row(result=any_of(Decimal("0.666667"), Decimal("0.66666")))
    ]
    if not is_jvm_spark():
        pytest.skip("the following tests are not supported in Sail yet")
    assert spark.sql("SELECT INTERVAL '3:15' HOUR TO MINUTE / 3 AS result").collect() == [
        Row(result=datetime.timedelta(seconds=3900))
    ]


@pytest.mark.parametrize("query", ["SELECT 1 / 0", "SELECT 1 / id FROM range(1)"])
@pytest.mark.parametrize(
    "action",
    [
        "collect",
        "show",
        "toPandas",
        pytest.param("toArrow", marks=pytest.mark.skipif(pyspark_version() < (4,), reason="requires Spark 4")),
    ],
)
def test_divide_by_zero_exception(spark, query, action):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(ArithmeticException, match=r"(?i)divi(?:de|sion).*zero"):
            getattr(spark.sql(query), action)()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)


def test_raise_error_divide_by_zero_text_is_not_arithmetic(spark):
    with pytest.raises(PySparkException, match="divide by zero") as error:
        spark.sql("SELECT raise_error('divide by zero')").collect()
    assert not isinstance(error.value, ArithmeticException)


def test_div(spark):
    assert spark.sql("SELECT 3 div 2 AS result").collect() == [Row(result=1)]
    assert spark.sql("SELECT -5.9 div 1").collect() == [Row(result=-5)]
    if not is_jvm_spark():
        pytest.skip("the following tests are not supported in Sail yet")
    assert spark.sql("SELECT INTERVAL '10' DAY div INTERVAL '3' DAY AS result").collect() == [Row(result=3)]
    assert spark.sql("SELECT INTERVAL '1-1' YEAR TO MONTH div INTERVAL '-1' MONTH AS result").collect() == [
        Row(result=-13)
    ]
    assert spark.sql("SELECT INTERVAL '100' HOUR div INTERVAL '1' DAY AS result").collect() == [Row(result=4)]
