"""Output-schema nullability via the DataFrame API.

Complements the SQL `@function(nullability)` feature suite with the DataFrame path: build a
DataFrame from an explicit `StructType` (so the input nullability is exactly what
we declare — a non-nullable and a nullable column), apply functions through the
`F.*` API, and assert the resulting schema's `nullable` flags.

Spark's `nullable` flag is static and, for a deterministic function that only
transforms its arguments, follows the inputs:
  - a non-null column   -> non-nullable output
  - a nullable column   -> nullable output

Divergences (Sail declares `nullable` differently from Spark) are marked
`xfail(strict=True)` off JVM, the same contract as the `@sail-bug` feature tag.

NOTE — this file is a representative sample, not exhaustive. The DataFrame-API
path and the SQL path are NOT equivalent: some divergences only show up via SQL
constant-folding. For example `bround` on a decimal is tagged `@sail-bug` in the
SQL suite (the folded `SELECT bround(2.5, 0)` path) yet matches Spark through the
DataFrame API (both column and `F.lit`) — so it passes here. `round` diverges on
both paths, which is why it stays a bug. A full DataFrame-API sweep across all
functions is a worthwhile follow-up: it is the path real PySpark code takes, and
it catches (and clears) different cases than the SQL sweep.
"""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark

sail_bug = pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail nullability bug", strict=True)


def _frame(spark):
    """One non-nullable ("nn") and one nullable ("nl") string column."""
    schema = T.StructType(
        [
            T.StructField("nn", T.StringType(), nullable=False),
            T.StructField("nl", T.StringType(), nullable=True),
        ]
    )
    return spark.createDataFrame([("hello", "world")], schema)


def _decimal_frame(spark):
    """One non-nullable ("nn") and one nullable ("nl") decimal column."""
    schema = T.StructType(
        [
            T.StructField("nn", T.DecimalType(2, 1), nullable=False),
            T.StructField("nl", T.DecimalType(2, 1), nullable=True),
        ]
    )
    return spark.createDataFrame([(Decimal("2.5"), Decimal("2.5"))], schema)


def _nullable(df, column):
    return df.schema[column].nullable


def test_input_schema_is_honored(spark):
    df = _frame(spark)
    assert _nullable(df, "nn") is False
    assert _nullable(df, "nl") is True


@pytest.mark.parametrize("nullable", [False, True])
def test_nullifzero_preserves_null_type_with_nullable_output(spark, nullable):
    # Arrange
    schema = T.StructType([T.StructField("x", T.NullType(), nullable=nullable)])
    # A non-nullable NullType field requires an empty input.
    df = spark.createDataFrame([], schema)
    expected = T.StructType([T.StructField("result", T.NullType(), nullable=True)])

    # Act
    result = df.selectExpr("nullifzero(x) AS result")

    # Assert
    assert df.schema == schema
    assert result.schema == expected


@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize("constructor", ["array", "map"])
@pytest.mark.parametrize("nested", [False, True], ids=["scalar", "nested"])
@pytest.mark.parametrize(
    ("left", "right", "interval_type", "nullable"),
    [
        ("INTERVAL '1' YEAR", "INTERVAL '2' MONTH", T.YearMonthIntervalType(0, 1), False),
        ("INTERVAL '1' DAY", "INTERVAL '2' HOUR", T.DayTimeIntervalType(0, 1), False),
        ("INTERVAL '1' HOUR", "INTERVAL '2' SECOND", T.DayTimeIntervalType(1, 3), False),
        (
            "INTERVAL '1' YEAR",
            "CAST(INTERVAL '2' MONTH AS INTERVAL YEAR TO MONTH)",
            T.YearMonthIntervalType(0, 1),
            False,
        ),
        ("INTERVAL '1' YEAR", "NULL", T.YearMonthIntervalType(0, 0), True),
        ("INTERVAL '1' YEAR", "CAST(NULL AS INTERVAL MONTH)", T.YearMonthIntervalType(0, 1), True),
        ("INTERVAL '1' YEAR", "CAST(NULL AS INTERVAL YEAR TO MONTH)", T.YearMonthIntervalType(0, 1), True),
        ("INTERVAL '1' HOUR", "NULL", T.DayTimeIntervalType(1, 1), True),
        ("INTERVAL '1' HOUR", "CAST(NULL AS INTERVAL SECOND)", T.DayTimeIntervalType(1, 3), True),
        ("INTERVAL '1' HOUR", "CAST(NULL AS INTERVAL DAY TO SECOND)", T.DayTimeIntervalType(0, 3), True),
    ],
    ids=[
        "year-month",
        "day-hour",
        "hour-second",
        "typed-wide",
        "year-null",
        "year-typed-null",
        "year-wide-null",
        "hour-null",
        "hour-typed-null",
        "hour-wide-null",
    ],
)
def test_constructor_interval_qualifiers(spark, left, right, interval_type, nullable, nested, constructor, reverse):
    # Spark 4.1.3 widens declared interval ranges, including those of typed NULLs.
    if reverse:
        left, right = right, left
    if nested:
        left, right = f"array({left})", f"array({right})"
        interval_type = T.ArrayType(interval_type, containsNull=nullable)
        nullable = False
    if constructor == "array":
        expression = f"array({left}, {right})"
        expected = T.ArrayType(interval_type, containsNull=nullable)
    else:
        expression = f"map('a', {left}, 'b', {right})"
        expected = T.MapType(T.StringType(), interval_type, valueContainsNull=nullable)
    assert spark.sql(f"SELECT {expression} AS result").schema == T.StructType(
        [T.StructField("result", expected, nullable=False)]
    )


@pytest.mark.parametrize("reverse", [False, True], ids=["year-month", "month-year"])
def test_array_preserves_duplicate_struct_field_intervals(spark, reverse):
    fields = [
        (F.expr("INTERVAL '1' YEAR").alias("x"), T.YearMonthIntervalType(0, 0), "INTERVAL '1-0' YEAR TO MONTH"),
        (F.expr("INTERVAL '2' MONTH").alias("x"), T.YearMonthIntervalType(1, 1), "INTERVAL '0-2' YEAR TO MONTH"),
    ]
    if reverse:
        fields.reverse()
    result = spark.range(2, numPartitions=2).select(F.array(F.struct(*(field[0] for field in fields))).alias("a"))
    expected_type = T.ArrayType(
        T.StructType([T.StructField("x", field[1], nullable=False) for field in fields]), containsNull=False
    )
    assert result.schema == T.StructType([T.StructField("a", expected_type, nullable=False)])
    expected_value = "[{" + ", ".join(field[2] for field in fields) + "}]"
    assert [row[0] for row in result.selectExpr("CAST(a AS STRING)").collect()] == [expected_value] * 2


# --- functions that correctly propagate nullability (pass on Sail and JVM) ---


def test_length_propagates_nullability(spark):
    out = _frame(spark).select(
        F.length("nn").alias("from_nn"),
        F.length("nl").alias("from_nl"),
    )
    assert _nullable(out, "from_nn") is False
    assert _nullable(out, "from_nl") is True


def test_concat_propagates_nullability(spark):
    out = _frame(spark).select(
        F.concat("nn", "nn").alias("both_non_null"),
        F.concat("nn", "nl").alias("one_nullable"),
    )
    assert _nullable(out, "both_non_null") is False
    assert _nullable(out, "one_nullable") is True


def test_coalesce_propagates_nullability(spark):
    out = _frame(spark).select(
        F.coalesce("nn", "nl").alias("has_non_null"),
        F.coalesce("nl", "nl").alias("all_nullable"),
    )
    assert _nullable(out, "has_non_null") is False
    assert _nullable(out, "all_nullable") is True


# --- functions where Sail is over-nullable ---
# Both cases are checked so we see exactly where each is right and wrong:
# Sail gets the nullable input right (nullable -> nullable) but the non-null
# input wrong (non-null -> Sail still says nullable, Spark says non-nullable).


@pytest.mark.parametrize("fn", [F.upper, F.lower, F.ascii, F.initcap])
def test_pure_string_fn_of_nullable_is_nullable(spark, fn):
    # nullable input -> nullable output: Sail and Spark agree
    out = _frame(spark).select(fn("nl").alias("result"))
    assert _nullable(out, "result") is True


@pytest.mark.parametrize(
    "fn",
    [
        F.upper,
        F.lower,
        pytest.param(F.ascii, marks=sail_bug),
        pytest.param(F.initcap, marks=sail_bug),
    ],
)
def test_pure_string_fn_of_non_null_is_non_nullable(spark, fn):
    # non-null input -> non-nullable output: Sail diverges for ascii and initcap (says nullable=True)
    out = _frame(spark).select(fn("nn").alias("result"))
    assert _nullable(out, "result") is False


@pytest.mark.parametrize(
    "fn",
    [
        F.trim,
        F.ltrim,
        F.rtrim,
        lambda c: F.expr(f"btrim({c})"),
        lambda c: F.expr(f"trim(BOTH 'h' FROM {c})"),
        lambda c: F.expr(f"replace({c}, 'l', 'L')"),
    ],
    ids=["trim", "ltrim", "rtrim", "btrim", "trim_both_chars", "replace"],
)
def test_trim_and_replace_nullability_follows_the_input(spark, fn):
    df = _frame(spark)
    assert _nullable(df.select(fn("nn").alias("result")), "result") is False
    assert _nullable(df.select(fn("nl").alias("result")), "result") is True


# --- the dangerous direction: Sail UNDER-nullable ---
# round on a decimal can overflow, so Spark declares it nullable=True even for a
# non-null input. Sail declares it non-nullable -> if a NULL ever appears an
# optimizer could drop null-checks, so this is a correctness risk, not just
# schema fidelity. (bround matches Spark here, kept as the passing counterpart.)


def test_round_of_nullable_decimal_is_nullable(spark):
    out = _decimal_frame(spark).select(F.round("nl", 1).alias("result"))
    assert _nullable(out, "result") is True  # Sail and Spark agree


def test_bround_of_non_null_decimal_is_nullable(spark):
    out = _decimal_frame(spark).select(F.bround("nn", 1).alias("result"))
    assert _nullable(out, "result") is True  # Sail and Spark agree


@sail_bug
def test_round_of_non_null_decimal_stays_nullable(spark):
    # Spark keeps round(decimal) nullable (overflow); Sail says non-nullable.
    out = _decimal_frame(spark).select(F.round("nn", 1).alias("result"))
    assert _nullable(out, "result") is True
