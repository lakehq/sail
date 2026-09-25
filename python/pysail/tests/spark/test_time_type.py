"""`spark.sql.timeType.enabled`, the flag that hides the TIME type.

Spark keeps TIME off outside its own tests and refuses it in every Connect execution, not only
when converting to Arrow. Sail implements TIME and keeps it on, so the flag is what a user sets
to get Spark's answer.

The flag is tested here rather than in a `.feature` because a scenario has to leave the session
as it found it: these tests unset the key, which is what tells "unset" from "set to the default".
"""

import contextlib

import pytest
from pyspark.errors import AnalysisException

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

# The refusal is server-side and any client can read it, but a TIME *value* only decodes from
# PySpark 4.1: 3.5 has no such type and 4.0 raises `[UNSUPPORTED_OPERATION]` client-side.
client_decodes_time = pyspark_version() >= (4, 1)

TIME_EXPRESSIONS = [
    pytest.param("TIME'01:02:03'", id="literal"),
    pytest.param("CAST('01:02:03' AS TIME(6))", id="cast"),
    pytest.param("CAST('01:02:03' AS TIME(0))", id="cast-to-time32"),
]

# Spark maps every TIME precision to Arrow `time64[ns]` (`ArrowUtils.scala`), so a client can
# always decode it. Sail sends `time32[s]` for TIME(0), which PySpark refuses outright.
RESOLVING_TIME_EXPRESSIONS = [
    *TIME_EXPRESSIONS[:2],
    pytest.param(
        "CAST('01:02:03' AS TIME(0))",
        id="cast-to-time32",
        marks=pytest.mark.xfail(
            not is_jvm_spark(),
            strict=True,
            reason="Sail sends TIME(0) as `time32[s]`, which the client cannot convert to Arrow",
        ),
    ),
]


@contextlib.contextmanager
def time_type_enabled(spark, value):
    # Restore rather than unset: other modules set this key for a whole session
    # (`test_to_arrow.py`, `test_arithmetic_matrix_types.py`, `test_result_type_parity.py`), so
    # unsetting on exit would silently drop their setting. The explicit `None` default is the
    # only way to tell "unset" from "set to the default" -- same reasoning as the `config` step.
    previous = spark.conf.get("spark.sql.timeType.enabled", None)
    spark.conf.set("spark.sql.timeType.enabled", value)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset("spark.sql.timeType.enabled")
        else:
            spark.conf.set("spark.sql.timeType.enabled", previous)


@pytest.mark.parametrize("expression", TIME_EXPRESSIONS)
def test_the_time_type_is_refused_when_the_flag_is_off(spark, expression):
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.sql(f"SELECT {expression} AS result").collect()


@pytest.mark.skipif(not client_decodes_time, reason="the TIME type needs PySpark 4.1+")
@pytest.mark.parametrize("expression", RESOLVING_TIME_EXPRESSIONS)
def test_the_time_type_resolves_when_the_flag_is_on(spark, expression):
    with time_type_enabled(spark, "true"):
        assert spark.sql(f"SELECT {expression} AS result").collect()[0][0].isoformat() == "01:02:03"


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail keeps the TIME type on by default, where Spark keeps it off",
)
def test_the_time_type_is_refused_by_default(spark):
    # The deliberate superset: Sail answers where Spark refuses. Pinned so the day the default
    # changes -- or the day Spark turns the flag on -- this says so.
    with pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"):
        spark.sql("SELECT TIME'01:02:03' AS result").collect()


# A TIME nobody spells: it only shows up in the OUTPUT SCHEMA. Spark refuses these too, in
# `TimeExpression.checkInputDataTypes` and on `dataframe.schema` in every Connect execution.
UNSPELLED_TIME_EXPRESSIONS = [
    pytest.param("to_time('01:02:03')", id="to_time"),
    pytest.param("try_to_time('01:02:03')", id="try_to_time"),
    pytest.param("make_time(1, 2, 3)", id="make_time"),
    pytest.param("""from_json('{"t": "01:02:03"}', 't TIME')""", id="from_json"),
    pytest.param("""from_json('{"a": {"t": "01:02:03"}}', 'a struct<t: TIME>')""", id="from_json-nested"),
]


@pytest.mark.parametrize("expression", UNSPELLED_TIME_EXPRESSIONS)
def test_a_time_nobody_spelled_is_refused_when_the_flag_is_off(spark, expression):
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.sql(f"SELECT {expression} AS result").collect()


# A TIME literal is not refused on its own: Spark gates `Cast` to TIME, the `TimeExpression`s and
# the result schema, so a literal consumed before the output still answers with the flag off.
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param("SELECT CAST(TIME'01:02:03' AS STRING) AS result", "01:02:03", id="cast-to-string"),
        pytest.param("SELECT TIME'01:02:03' < TIME'04:05:06' AS result", True, id="comparison"),
        pytest.param("SELECT typeof(TIME'01:02:03') AS result", "time(6)", id="typeof"),
        pytest.param("SELECT count(*) AS result FROM VALUES (TIME'01:02:03') AS t(x)", 1, id="values"),
    ],
)
def test_a_time_literal_that_never_reaches_the_output_resolves_when_the_flag_is_off(spark, query, expected):
    with time_type_enabled(spark, "false"):
        assert spark.sql(query).collect()[0][0] == expected


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("CAST(TIME'01:02:03' - TIME'01:00:00' AS STRING)", id="subtract-times"),
        pytest.param("CAST(TIME'01:02:03' + INTERVAL '1' HOUR AS STRING)", id="time-add-interval"),
    ],
)
def test_time_arithmetic_is_refused_when_the_flag_is_off(spark, expression):
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.sql(f"SELECT {expression} AS result").collect()


# TODO: Spark refuses every `TimeExpression` over a TIME with the flag off; Sail only gates the
#   output schema and the arithmetic, so a TIME function returning another type answers.
@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail does not gate `TimeExpression`s")
def test_a_time_function_is_refused_when_the_flag_is_off(spark):
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.sql("SELECT hour(TIME'01:02:03') AS result").collect()


def test_a_file_whose_schema_has_a_time_column_is_refused_when_the_flag_is_off(spark, tmp_path):
    location = str(tmp_path / "time.parquet")
    with time_type_enabled(spark, "true"):
        spark.sql("SELECT TIME'01:02:03' AS t").write.mode("overwrite").parquet(location)
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.read.parquet(location).collect()


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason=(
        "Sail checks the output schema, so a projection that drops the TIME column is not "
        "refused; Spark refuses the read itself in `DataSourceUtils.verifySchema`"
    ),
)
def test_a_time_column_is_refused_even_when_the_projection_drops_it(spark, tmp_path):
    location = str(tmp_path / "time.parquet")
    with time_type_enabled(spark, "true"):
        spark.sql("SELECT TIME'01:02:03' AS t").write.mode("overwrite").parquet(location)
    with (
        time_type_enabled(spark, "false"),
        pytest.raises(AnalysisException, match=r"(?i)the data type TIME is not supported"),
    ):
        spark.read.parquet(location).selectExpr("1 AS x").collect()


@pytest.mark.skipif(not client_decodes_time, reason="a PySpark client before 4.1 cannot read a TIME schema")
def test_time_type_disabled_is_refused_at_execution_not_at_analysis(spark):
    """Spark writes the refusal when it builds the Arrow batches, so the schema answers first."""
    # Read with a default: the key is unset unless an earlier test set it, and a bare `get` of an
    # unset key raises.
    previous = spark.conf.get("spark.sql.timeType.enabled", None)
    spark.conf.set("spark.sql.timeType.enabled", "false")
    try:
        assert spark.sql("SELECT TIME '12:00:00' AS v").schema.simpleString() == "struct<v:time(6)>"
        with pytest.raises(Exception, match=r"(?i)time"):
            spark.sql("SELECT TIME '12:00:00' AS v").collect()
    finally:
        if previous is None:
            spark.conf.unset("spark.sql.timeType.enabled")
        else:
            spark.conf.set("spark.sql.timeType.enabled", previous)
