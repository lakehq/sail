import datetime
from decimal import Decimal

import pytest
from pyspark.errors import ArithmeticException, PySparkException
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version
from pysail.testing.spark.utils.sql import any_of, parse_show_string


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


@pytest.mark.parametrize("query", ["SELECT 10 % 0", "SELECT mod(10, id) FROM (VALUES (0)) AS t(id)"])
def test_remainder_by_zero_exception(spark, query):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(ArithmeticException, match=r"(?i)remainder.*zero"):
            spark.sql(query).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)


def test_raise_error_divide_by_zero_text_is_not_arithmetic(spark):
    with pytest.raises(PySparkException, match="divide by zero") as error:
        spark.sql("SELECT raise_error('divide by zero')").collect()
    assert not isinstance(error.value, ArithmeticException)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT raise_error('Remainder by zero')",
        "SELECT raise_error('Remainder by zero user message')",
        "SELECT assert_true(false, 'Remainder by zero')",
        "SELECT assert_true(false, 'Remainder by zero user message')",
    ],
)
def test_raise_error_remainder_by_zero_text_is_not_arithmetic(spark, query):
    with pytest.raises(PySparkException, match="Remainder by zero") as error:
        spark.sql(query).collect()
    assert not isinstance(error.value, ArithmeticException)


@pytest.mark.parametrize(
    "expression",
    [
        "CASE WHEN true THEN 1 / 0 ELSE id END",
        "CASE WHEN false THEN id ELSE 1 / 0 END",
        "CASE WHEN CAST(NULL AS BOOLEAN) THEN id ELSE 1 / 0 END",
        "coalesce(CAST(NULL AS DOUBLE), CASE WHEN true THEN 1 / 0 ELSE id END)",
    ],
)
@pytest.mark.parametrize("union_all", [False, True])
def test_empty_division_conditional_collect_and_show(spark, remote, expression, union_all, capsys):
    original = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        query = f"SELECT {expression} AS result FROM range(0)"  # noqa: S608
        if union_all:
            query += " UNION ALL SELECT CAST(id AS DOUBLE) AS result FROM range(0)"
        assert spark.sql(query).collect() == []
        # Spark collect() returns no rows, but show() adds ToPrettyString and raises
        # after extra constant folding. Sail deliberately keeps show() consistent
        # with collect() for these empty-input conditional expressions.
        if remote.startswith("local"):
            with pytest.raises(ArithmeticException, match=r"(?i)divi(?:de|sion).*zero"):
                spark.sql(query).show()
        else:
            spark.sql(query).show()
            assert parse_show_string(capsys.readouterr().out) == [["result"]]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param("SELECT 1 / 0 AS result FROM range(0)", [], id="empty-range"),
        pytest.param("SELECT 1 / 0 AS result WHERE false", [], id="false-filter"),
        pytest.param("SELECT id + (1 / 0) AS result FROM range(0)", [], id="nonfoldable-parent"),
        pytest.param("SELECT (1 / 0 > 0) AND id=0 AS result FROM range(0)", [], id="boolean-parent"),
        pytest.param(
            "SELECT abs(CASE WHEN true THEN 1 / 0 ELSE id END) AS result FROM range(0)",
            [],
            id="scalar-parent-of-case",
        ),
        pytest.param(
            "SELECT 1 + (CASE WHEN true THEN 1 / 0 ELSE id END) AS result FROM range(0)",
            [],
            id="arithmetic-parent-of-case",
        ),
        pytest.param(
            "SELECT CAST(CASE WHEN true THEN 1 / 0 ELSE id END AS STRING) AS result FROM range(0)",
            [],
            id="cast-parent-of-case",
        ),
        pytest.param(
            "SELECT CASE WHEN false THEN id WHEN true THEN 1 / 0 END AS result FROM range(0)",
            [],
            id="pruned-false-case-branch",
        ),
        pytest.param(
            "SELECT CASE WHEN true THEN 1 / 0 WHEN false THEN id END AS result FROM range(0)",
            [],
            id="pruned-later-case-branch",
        ),
        pytest.param("SELECT 1 / 0 AS result FROM range(0) LIMIT 0", [], id="redundant-zero-limit"),
        pytest.param("SELECT 1 / 0 AS result FROM range(1) WHERE false LIMIT 0", [], id="redundant-filtered-limit"),
        pytest.param("SELECT 1 / 0 AS result FROM range(0) GROUP BY id LIMIT 0", [], id="redundant-grouped-limit"),
        pytest.param("SELECT sum(1 / 0) AS result FROM range(0)", [Row(result=None)], id="aggregate-argument"),
        pytest.param(
            "SELECT transform(array(), x -> 1 / 0) AS result", [Row(result=[])], id="empty-higher-order-input"
        ),
    ],
)
def test_division_by_zero_in_unevaluated_expressions(spark, remote, query, expected):
    # Evaluated division by zero on non-NULL inputs must raise in ANSI mode.
    # Sail deliberately does not reproduce Spark errors from folding unused expressions:
    #   SELECT 1 / 0 FROM range(1) LIMIT 0 -> []
    #   SELECT 1 / 0 FROM range(0) LIMIT 0 -> DIVIDE_BY_ZERO
    # Spark removes a redundant limit over known-empty input before constant folding.
    # Sail keeps errors tied to evaluated division, independent of those optimizer choices.
    # An aggregate output or empty array does not require evaluating its argument/lambda.
    # Explicit Spark errors below preserve the reference behavior for this intentional
    # parity exception; ANSI=false and shared evaluation controls remain in the BDD tests.
    original = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        if remote.startswith("local"):
            with pytest.raises(ArithmeticException, match=r"(?i)divi(?:de|sion).*zero"):
                spark.sql(query).collect()
        else:
            assert spark.sql(query).collect() == expected
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)


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
