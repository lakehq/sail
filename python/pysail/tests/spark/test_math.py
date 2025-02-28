import datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import Row


def test_plus(sail):
    assert sail.sql("SELECT 1 + 2 AS result;").collect() == [Row(result=3)]
    assert sail.sql("SELECT DATE'2025-02-26' + INTERVAL '2' MONTH AS result;").collect() == [
        Row(result=datetime.date(2025, 4, 26))
    ]
    assert sail.sql("SELECT TIMESTAMP'2025-02-26 12:15:29' + INTERVAL '3' SECOND AS result;").collect() == [
        Row(result=datetime.datetime(2025, 2, 26, 12, 15, 32))  # noqa: DTZ001
    ]
    assert sail.sql("SELECT DATE'2025-03-31' + INTERVAL '1' MONTH AS result;").collect() == [
        Row(result=datetime.date(2025, 4, 30))
    ]
    assert sail.sql("SELECT DATE'2025-02-26' + 1 AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]
    assert sail.sql("SELECT 1 + DATE'2025-02-26' AS result;").collect() == [Row(result=datetime.date(2025, 2, 27))]


def test_minus(sail):
    assert sail.sql("SELECT 2 - 1 AS result;").collect() == [Row(result=1)]
    assert sail.sql("SELECT TIMESTAMP'2025-02-26 12:15:29' - INTERVAL '3' SECOND AS result;").collect() == [
        Row(result=datetime.datetime(2025, 2, 26, 12, 15, 26))  # noqa: DTZ001
    ]
    assert sail.sql("SELECT DATE'2025-03-31' - INTERVAL '1' MONTH AS result;").collect() == [
        Row(result=datetime.date(2025, 2, 28))
    ]
    assert sail.sql("SELECT DATE'2025-02-26' - 1 AS result;").collect() == [Row(result=datetime.date(2025, 2, 25))]
    with pytest.raises(Exception, match="Cannot coerce arithmetic expression Int32 - Date32 to valid types"):
        sail.sql("SELECT 1 - DATE'2025-02-26' AS result;").collect()


def test_multiply(sail):
    assert sail.sql("SELECT 3 * 2 AS result;").collect() == [Row(result=6)]
    assert sail.sql("SELECT 2L * 2L AS result;").collect() == [Row(result=4)]
    # Not supported in Sail yet:
    # assert sail.sql("SELECT INTERVAL '3:15' HOUR TO MINUTE * 3 AS result;").collect() == [
    #     Row(result=datetime.timedelta(seconds=35100))
    # ]
    # assert sail.sql("SELECT INTERVAL '3' YEAR * 3 AS result;").collect() == [Row(result=108)]


def test_divide(sail):
    assert sail.sql("SELECT 3 / 2 AS result;").collect() == [Row(result=1.5)]
    assert sail.sql("SELECT 2L / 2L AS result;").collect() == [Row(result=1.0)]
    assert sail.sql("SELECT 6 / Decimal(3) AS result;").collect() == [Row(result=Decimal("2.00000000000"))]
    # TODO: Spark outputs Decimal("2.000000") but Sail outputs Decimal("2.00000000000")
    assert sail.sql("SELECT Decimal(6) / 3 AS result;").collect() == [Row(result=Decimal("2.00000000000"))]
    # TODO: Spark outputs Decimal("1.50000000000") but Sail outputs Decimal("1.50000")
    assert sail.sql("SELECT Decimal(3) / Decimal(2) AS result;").collect() == [Row(result=Decimal("1.50000"))]
    # TODO: Spark outputs Decimal("1.500000") but Sail outputs Decimal("1.50000")
    assert sail.sql("SELECT Decimal(3) / 2 AS result;").collect() == [Row(result=Decimal("1.50000"))]
    # TODO: Spark outputs Decimal("0.66666666667") but Sail outputs Decimal("0.6666")
    assert sail.sql("SELECT 2 / Decimal(3) AS result;").collect() == [Row(result=Decimal("0.6666"))]
    # TODO: Spark outputs Decimal("0.666667") but Sail outputs Decimal("0.6666")
    assert sail.sql("SELECT 2.0 / 3.0 AS result;").collect() == [Row(result=Decimal("0.66666"))]
    # Not supported in Sail yet:
    # assert sail.sql("SELECT INTERVAL '3:15' HOUR TO MINUTE / 3 AS result;").collect() == [
    #     Row(result=datetime.timedelta(seconds=3900))
    # ]


def test_div(sail):
    assert sail.sql("SELECT 3 div 2 AS result;").collect() == [Row(result=1)]
    assert sail.sql("SELECT -5.9 div 1;").collect() == [Row(result=-5)]
    # Not supported in Sail yet:
    # assert sail.sql("SELECT INTERVAL '10' DAY div INTERVAL '3' DAY AS result;").collect() == [Row(result=3)]
    # assert sail.sql("SELECT INTERVAL '1-1' YEAR TO MONTH div INTERVAL '-1' MONTH AS result;").collect() == [Row(result=-13)]
    # assert sail.sql("SELECT INTERVAL '100' HOUR div INTERVAL '1' DAY AS result;").collect() == [Row(result=4)]
