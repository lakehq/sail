import pyarrow as pa
import pytest
from pyspark.errors import PySparkException
from pyspark.sql import types as T  # noqa: N812

# Spark 4.2.0 AstBuilder numericLiteral and SparkDateTimeUtils NTZ conversion.
# Fixed values were checked with genuine SQL literals, not string-to-timestamp casts.


@pytest.fixture(params=["true", "false"], autouse=True)
def ansi_mode(spark, request):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param == "true"
    spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("-9223372036854775808L", -(2**63)),
        ("9223372036854775807L", 2**63 - 1),
        ("+9223372036854775807L", 2**63 - 1),
        ("+-9223372036854775808L", -(2**63)),
        ("+(-9223372036854775808L)", -(2**63)),
        ("- 9223372036854775808L", -(2**63)),
        ("-(+9223372036854775807L)", -(2**63) + 1),
        ("- -9223372036854775807L", 2**63 - 1),
    ],
)
def test_signed_bigint_literals(spark, expression, expected):
    result = spark.sql(f"SELECT {expression} AS value")
    assert result.schema == T.StructType([T.StructField("value", T.LongType(), False)])
    assert [row.value for row in result.collect()] == [expected]


@pytest.mark.parametrize(
    "expression",
    [
        "9223372036854775808L",
        "+9223372036854775808L",
        "-9223372036854775809L",
        "-(9223372036854775808L)",
        "-(+9223372036854775808L)",
    ],
)
def test_bigint_literal_overflow_is_not_runtime_negation(spark, expression):
    with pytest.raises(PySparkException, match="(?i)bigint|INVALID_NUMERIC_LITERAL_RANGE"):
        spark.sql(f"SELECT {expression}").collect()


@pytest.mark.parametrize(
    ("expression", "data_type", "expected"),
    [
        ("- -9223372036854775808L", T.LongType(), -(2**63)),
        ("-(-9223372036854775808L)", T.LongType(), -(2**63)),
        ("-(-2147483648)", T.IntegerType(), -(2**31)),
        ("-(-32768S)", T.ShortType(), -32768),
        ("-(-128Y)", T.ByteType(), -128),
    ],
)
def test_outer_negation_of_minimum_integer_honors_ansi(spark, ansi_mode, expression, data_type, expected):
    if ansi_mode:
        with pytest.raises(PySparkException, match="(?i)overflow"):
            spark.sql(f"SELECT {expression}").collect()
    else:
        result = spark.sql(f"SELECT {expression} AS value")
        assert result.schema == T.StructType([T.StructField("value", data_type, False)])
        assert [row.value for row in result.collect()] == [expected]


@pytest.mark.parametrize(
    ("expression", "data_type", "expected"),
    [
        ("-128Y", T.ByteType(), -128),
        ("-32768S", T.ShortType(), -32768),
        ("-2147483648", T.IntegerType(), -(2**31)),
        ("-9223372036854775808", T.LongType(), -(2**63)),
    ],
)
def test_negative_integer_literals_keep_their_narrowest_type(spark, expression, data_type, expected):
    result = spark.sql(f"SELECT {expression} AS value")
    assert result.schema == T.StructType([T.StructField("value", data_type, False)])
    assert [row.value for row in result.collect()] == [expected]


@pytest.mark.parametrize("session_timezone", ["UTC", "+05:30", "America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize(
    ("value", "micros"),
    [
        ("-290308-12-21 19:59:05.224192", -(2**63)),
        ("+294247-01-10 04:00:54.775807", 2**63 - 1),
        ("-290308-12-21 19:59:05.224192999", -(2**63)),
        ("+294247-01-10 04:00:54.775807999", 2**63 - 1),
        ("0000-02-29 12:34:56.123456", -62162076303876544),
        ("-0001-12-31 23:59:59.999999", -62167219200000001),
        ("-0400-02-29 00:00:00", -74784902400000000),
        ("+280000-02-29 00:00:00", 8773784438400000000),
        ("1969-12-31 23:59:59.999999999", -1),
        ("-290308-12-21 19:59:05.224192+05:30", -(2**63)),
    ],
)
def test_ntz_literal_microseconds_are_timezone_independent(spark, session_timezone, value, micros):
    result = spark.sql(f"SELECT TIMESTAMP_NTZ '{value}' AS value")
    assert result.schema == T.StructType([T.StructField("value", T.TimestampNTZType(), False)])
    # Inspect Arrow's physical value; Python datetime cannot represent these years.
    assert result.toArrow()["value"].cast(pa.int64()).to_pylist() == [micros]


@pytest.mark.parametrize(
    "value",
    [
        "-290308-12-21 19:59:05.224191",
        "+294247-01-10 04:00:54.775808",
        "-0100-02-29 00:00:00",
        "1900-02-29 00:00:00",
        "+280100-02-29 00:00:00",
        "+294247-13-01",
        "+294247-01-00",
        "+294247-01-10 24:00:00",
        "2024-01-01 00:00:00BAD",
    ],
)
def test_ntz_literals_reject_overflow_and_invalid_calendar_values(spark, value):
    with pytest.raises(PySparkException, match="(?i)invalid|outside the microsecond range"):
        spark.sql(f"SELECT TIMESTAMP_NTZ '{value}'").collect()


@pytest.mark.parametrize("session_timezone", ["UTC", "+05:30", "America/Los_Angeles"], indirect=True)
def test_configured_ntz_literals_preserve_endpoints_in_a_projection(spark, session_timezone):
    previous = spark.conf.get("spark.sql.timestampType")
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    try:
        result = spark.sql("""
            SELECT CASE WHEN id = 0 THEN TIMESTAMP '-290308-12-21 19:59:05.224192'
                        ELSE TIMESTAMP '+294247-01-10 04:00:54.775807' END AS value
            FROM range(2)
        """)
        assert result.schema == T.StructType([T.StructField("value", T.TimestampNTZType(), False)])
        assert sorted(result.toArrow()["value"].cast(pa.int64()).to_pylist()) == [-(2**63), 2**63 - 1]
    finally:
        spark.conf.set("spark.sql.timestampType", previous)


@pytest.mark.parametrize(("value", "days"), [("0000-02-29", -719469), ("-0001-12-31", -719529), ("1970-01-01", 0)])
def test_date_literals_remain_day_values(spark, value, days):
    result = spark.sql(f"SELECT DATE '{value}' AS value")
    assert result.schema == T.StructType([T.StructField("value", T.DateType(), False)])
    assert result.toArrow()["value"].cast(pa.int32()).to_pylist() == [days]


def test_date_literal_calendar_validation(spark):
    with pytest.raises(PySparkException, match="(?i)invalid"):
        spark.sql("SELECT DATE '1900-02-29'").collect()


@pytest.mark.parametrize("session_timezone", ["+05:30"], indirect=True)
def test_ntz_and_ltz_literals_remain_distinct(spark, session_timezone):
    result = spark.sql("""
        SELECT TIMESTAMP_NTZ '1970-01-01 00:00:00' AS ntz,
               TIMESTAMP_LTZ '1970-01-01 00:00:00' AS ltz
    """)
    assert result.schema == T.StructType(
        [T.StructField("ntz", T.TimestampNTZType(), False), T.StructField("ltz", T.TimestampType(), False)]
    )
    table = result.toArrow()
    assert table["ntz"].cast(pa.int64()).to_pylist() == [0]
    assert table["ltz"].cast(pa.int64()).to_pylist() == [-19800000000]
