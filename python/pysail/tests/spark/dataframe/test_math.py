import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import is_jvm_spark
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


@pytest.mark.parametrize("data_type", ["float", "double"])
@pytest.mark.parametrize("zero", [0.0, -0.0])
def test_signed_zero_divisors(spark, data_type, zero):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        divisor = F.lit(zero).cast(data_type)
        rows = (
            spark.range(2)
            .select(
                (F.lit(1) / divisor).alias("literal_division"),
                (F.lit(1) % divisor).alias("literal_remainder"),
                (F.col("id") / divisor).alias("column_division"),
                (F.col("id") % divisor).alias("column_remainder"),
            )
            .collect()
        )
        assert (
            rows
            == [Row(literal_division=None, literal_remainder=None, column_division=None, column_remainder=None)] * 2
        )
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_div_after_projected_case(spark, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        projected = spark.range(2).select(
            F.when(F.col("id") == 0, F.lit(0)).otherwise(F.expr("1.5")).alias("numerator"),
            F.when(F.col("id") == 0, F.lit(1)).otherwise(F.expr("0.5")).alias("denominator"),
        )
        rows = projected.select(
            F.expr("numerator DIV 0.5").alias("dividend_case"),
            F.expr("3 DIV denominator").alias("divisor_case"),
        ).collect()
        assert sorted(rows) == [
            Row(dividend_case=0, divisor_case=3),
            Row(dividend_case=3, divisor_case=6),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.skipif(is_jvm_spark(), reason="Unsigned integer types are a Sail extension")
@pytest.mark.parametrize(
    ("small_type", "wide_type", "wide_bits"),
    [("UINT8", "UINT16", 16), ("UINT16", "UINT32", 32), ("UINT32", "UINT64", 64)],
)
def test_unsigned_complement_after_case_coercion(spark, small_type, wide_type, wide_bits):
    value = f"CASE WHEN id = 0 THEN CAST(0 AS {small_type}) ELSE CAST(1 AS {wide_type}) END"
    expected = [
        Row(result=str((1 << wide_bits) - 1), same_type=True),
        Row(result=str((1 << wide_bits) - 2), same_type=True),
    ]
    # Cover both the CASE itself and the stale field type of an intervening projection.
    for source, argument in [("range(2)", value), (f"(SELECT {value} AS v FROM range(2)) t", "v")]:  # noqa: S608
        for complement in [f"~({argument})", f"bitwise_not({argument})"]:
            rows = spark.sql(
                f"SELECT CAST({complement} AS STRING) AS result, "  # noqa: S608
                f"typeof({complement}) = typeof(CAST(NULL AS {wide_type})) AS same_type "
                f"FROM {source}"
            ).collect()
            assert sorted(rows) == sorted(expected)


@pytest.mark.parametrize("first_branch", ["NULL", "CAST(0 AS TINYINT)"])
def test_complement_preserves_resolved_case_type(spark, first_branch):
    value = f"CASE WHEN id = 0 THEN {first_branch} ELSE 2147483648L END"
    expected = [
        Row(result=None if first_branch == "NULL" else -1, same_type=True),
        Row(result=-2147483649, same_type=True),
    ]
    for source, argument in [("range(2)", value), (f"(SELECT {value} AS v FROM range(2)) t", "v")]:  # noqa: S608
        for complement in [f"~({argument})", f"bitwise_not({argument})"]:
            rows = spark.sql(
                f"SELECT CAST({complement} AS BIGINT) AS result, "  # noqa: S608
                f"typeof({complement}) = 'bigint' AS same_type FROM {source} ORDER BY result"
            ).collect()
            assert rows == sorted(expected, key=lambda row: (row.result is not None, row.result))
