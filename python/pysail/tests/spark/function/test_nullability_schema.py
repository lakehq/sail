"""Output-schema nullability via the DataFrame API.

Complements the SQL `@spark_null` feature suite with the DataFrame path: build a
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


@pytest.mark.parametrize("fn", [F.upper, F.lower, F.ascii, F.initcap])
@sail_bug
def test_pure_string_fn_of_non_null_is_non_nullable(spark, fn):
    # non-null input -> non-nullable output: Sail diverges (says nullable=True)
    out = _frame(spark).select(fn("nn").alias("result"))
    assert _nullable(out, "result") is False


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
