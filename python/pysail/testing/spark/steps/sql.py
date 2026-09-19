from __future__ import annotations

import io
import json
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from jinja2 import Template
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pytest_bdd import given, parsers, then, when

from pysail.testing.spark.utils.sql import escape_sql_string_literal, parse_show_string


@pytest.fixture
def variables():
    """Per-scenario variables used by `.feature` templates."""
    return {}


@given(parsers.parse("variable {name} for JSON value {definition}"), target_fixture="variables")
def variable_for_json_value(name, definition, variables):
    """Defines a variable with a JSON value."""
    variables[name] = json.loads(definition)
    return variables


class PathWrapper:
    """A wrapper around a path with additional methods for rendering in templates."""

    def __init__(self, path):
        self.path = path

    @property
    def string(self):
        """The string representation of the path."""
        return str(self.path)

    @property
    def sql(self):
        """The corresponding SQL string literal for the path."""
        return f"'{escape_sql_string_literal(str(self.path))}'"

    @property
    def uri(self):
        """The file URI representation of the path."""
        return f"'{self.path.absolute().as_uri()}'"

    @property
    def file_uri(self):
        """The unquoted file URI representation of the path."""
        return self.path.absolute().as_uri()


@given(parsers.parse("variable {name} for temporary directory {directory}"), target_fixture="variables")
def variable_for_temporary_directory(name, directory, tmp_path, variables):
    """Defines a variable for a temporary directory with the given name.

    This step does not create the directory, it only stores its absolute path.
    """
    variables[name] = PathWrapper(tmp_path / directory)
    return variables


@given(parsers.parse("variable {name} for delta log of {location_var}"), target_fixture="variables")
def variable_for_delta_log(name: str, location_var: str, variables: dict) -> dict:
    """Defines a variable pointing to the _delta_log subdirectory of a Delta table location."""
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    variables[name] = PathWrapper(Path(location.path) / "_delta_log")
    return variables


@given(parsers.parse("config {key} = {value}"))
def spark_config_override(key, value, spark, variables):
    """Sets a Spark configuration value. Restores the original value or unsets the value after the scenario."""
    rendered_value = Template(value).render(**variables)
    try:
        old_value = spark.conf.get(key)
    except Exception:  # noqa: BLE001
        old_value = None
    spark.conf.set(key, rendered_value)
    yield
    if old_value is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, old_value)


@given(parsers.re("statement(?P<template>( template)?)"))
def statement(template, docstring, spark, variables):
    """Executes a SQL statement that is expected to succeed."""
    s = Template(docstring).render(**variables) if template else docstring
    spark.sql(s)


@given(parsers.parse("sample with replacement fraction {fraction} seed {seed} as temporary view {view}"))
def sample_with_replacement_as_temporary_view(fraction, seed, view, spark):
    """Create a seeded replacement sample for parity scenarios."""
    sampled = spark.range(10, numPartitions=1).sample(True, float(fraction), int(seed))
    sampled.createOrReplaceTempView(view)


@given(parsers.parse("scalar Python UDF {name} returns {value:d}"))
def scalar_python_udf(name, value, spark):
    """Register a scalar Python UDF that ignores its argument."""
    spark.udf.register(name, lambda _value: value, "long")


@given(parsers.parse("scalar Python UDF {name} raises {message}"))
def failing_scalar_python_udf(name, message, spark):
    """Register a scalar Python UDF that always raises an error."""

    def fail(_value):
        raise RuntimeError(message)

    spark.udf.register(name, fail, "string")


@given(parsers.re(r"statement(?P<template>( template)?) with error (?P<error>.*)"))
def statement_with_error(template, error, docstring, spark, variables):
    """Executes a SQL statement that is expected to fail with an error."""
    s = Template(docstring).render(**variables) if template else docstring
    with pytest.raises(Exception, match=error):
        spark.sql(s)


@given(parsers.re("final statement(?P<template>( template)?)"))
def final_statement(template, docstring, spark, variables):
    """Executes a SQL statement at the end of a scenario."""
    s = Template(docstring).render(**variables) if template else docstring
    yield
    spark.sql(s)


@given(parsers.parse("sleep for {seconds:d} seconds"))
def sleep_for_seconds(seconds: int) -> None:
    time.sleep(seconds)


@when(parsers.re("query(?P<template>( template)?)"), target_fixture="query")
def query(template, docstring, variables):
    """Defines a SQL query (not executed here)."""
    return Template(docstring).render(**variables) if template else docstring


@then("query schema")
def query_schema(docstring, query, spark):
    """Analyze the SQL query and compare schema with expected schema tree string."""
    df = spark.sql(query)
    assert_schema_tree(df, docstring)


def _join_dataframes(spark):
    """Builds the three frames the `DataFrame.join` cases are defined over."""
    df1 = spark.createDataFrame([Row(name="Alice", age=2), Row(name="Bob", age=5)])
    df2 = spark.createDataFrame([Row(name="Tom", height=80), Row(name="Bob", height=85)])
    df3 = spark.createDataFrame(
        [
            Row(name="Alice", age=10, height=80),
            Row(name="Bob", age=5, height=None),
            Row(name="Tom", age=None, height=None),
            Row(name=None, age=None, height=None),
        ]
    )
    return df1, df2, df3


def _join_cases(spark):
    """DataFrame cases for `DataFrame.join`, which SQL cannot express.

    A column object such as `df1.name` is resolved through the plan id of the frame it
    came from, which is a different resolution path from a SQL qualifier, so these cases
    have to keep using the DataFrame API.
    """
    df1, df2, df3 = _join_dataframes(spark)
    return {
        "join on name": lambda: df1.join(df2, "name"),
        "join on name selecting a column of each side": lambda: df1.join(df2, "name").select(df1.name, df2.height),
        "join on name selecting columns by name": lambda: df1.join(df2, "name").select("name", "height"),
        "join on a name equality": lambda: df1.join(df2, df1.name == df2.name),
        "join on a name equality selecting the duplicated name": lambda: df1.join(df2, df1.name == df2.name).select(
            "name", "height"
        ),
        "join on two names": lambda: df1.join(df3, ["name", "age"]),
        "join on two names selecting the left side": lambda: df1.join(df3, ["name", "age"]).select(df1.name, df1.age),
        "outer join on a name equality": lambda: df1.join(df2, df1.name == df2.name, "outer").sort(F.desc(df1.name)),
        "outer join on a name equality selecting a column of each side": lambda: df1.join(
            df2, df1.name == df2.name, "outer"
        )
        .sort(F.desc(df1.name))
        .select(df1.name, df2.height),
        "outer join on a name equality sorted after the projection": lambda: df1.join(
            df2, df1.name == df2.name, "outer"
        )
        .select(df1.name, df2.height)
        .sort(F.desc("name")),
        "outer join on two equalities": lambda: df1.join(df3, [df1.name == df3.name, df1.age == df3.age], "outer")
        .select(df1.name, df3.age)
        .sort(df1.name, df3.age),
        "outer self join selecting the ambiguous name": lambda: df1.join(df1, df1.name == df1.name, "outer").select(
            df1.name
        ),
        "outer self join of two aliases": lambda: df1.alias("a")
        .join(df1.alias("b"), F.col("a.name") == F.col("b.name"), "outer")
        .sort(F.desc("a.name"))
        .select("a.name", "b.age"),
        "outer join on name": lambda: df1.join(df2, "name", "outer").sort(F.desc("name")),
        "outer join on name sorted by the left side": lambda: df1.join(df2, "name", "outer").sort(F.desc(df1.name)),
        "outer join on name sorted by the right side": lambda: df1.join(df2, "name", "outer").sort(F.desc(df2.name)),
        "outer join on name selecting columns by name": lambda: df1.join(df2, "name", "outer")
        .select("name", "height")
        .sort(F.desc("name")),
        "outer join on name selecting the left name": lambda: df1.join(df2, "name", "outer")
        .select(df1.name, "height")
        .sort(F.desc("name")),
        "outer join on name selecting the right name": lambda: df1.join(df2, "name", "outer")
        .select(df2.name, "height")
        .sort(F.desc("name")),
        "outer join on two names": lambda: df1.join(df3, ["name", "age"], "outer").sort("name", "age"),
        "left outer join on name": lambda: df1.join(df2, "name", "left_outer").sort(F.asc("name")),
        "right outer join on name": lambda: df1.join(df2, "name", "right_outer").sort(F.asc("name")),
        "left semi join on name": lambda: df1.join(df2, "name", "left_semi"),
        "left anti join on name": lambda: df1.join(df2, "name", "left_anti"),
    }


@when(parsers.parse("dataframe for {case}"), target_fixture="dataframe")
def dataframe_for(case, spark):
    """Builds a DataFrame for a named BDD case."""
    cases = {
        "null literal": lambda: spark.range(1).select(F.lit(None).alias("result")),
        "null literal alias projection": lambda: (
            spark.range(1).select(F.lit(None).alias("value")).select(F.col("value").alias("result"))
        ),
        "null literal with column": lambda: spark.range(1).withColumn("result", F.lit(None)).select("result"),
        "to_timestamp null literal": lambda: spark.range(1).select(F.to_timestamp(F.lit(None)).alias("result")),
        "to_timestamp null literal with format": lambda: spark.range(1).select(
            F.to_timestamp(F.lit(None), "yyyy-MM-dd").alias("result")
        ),
        "try_to_timestamp null literal with format": lambda: spark.range(1).select(
            F.try_to_timestamp(F.lit(None), F.lit("yyyy-MM-dd")).alias("result")
        ),
        "try_to_timestamp value with null format": lambda: spark.range(1).select(
            F.try_to_timestamp(F.lit("2024-01-02"), F.lit(None)).alias("result")
        ),
        "to_timestamp_ltz null literal with format": lambda: spark.range(1).select(
            F.to_timestamp_ltz(F.lit(None), F.lit("yyyy-MM-dd")).alias("result")
        ),
        "to_timestamp_ltz value with null format": lambda: spark.range(1).select(
            F.to_timestamp_ltz(F.lit("2024-01-02"), F.lit(None)).alias("result")
        ),
        "to_timestamp_ntz null literal with format": lambda: spark.range(1).select(
            F.to_timestamp_ntz(F.lit(None), F.lit("yyyy-MM-dd")).alias("result")
        ),
        "to_timestamp_ntz value with null format": lambda: spark.range(1).select(
            F.to_timestamp_ntz(F.lit("2024-01-02"), F.lit(None)).alias("result")
        ),
    }
    cases.update(_join_cases(spark))
    try:
        return cases[case]()
    except KeyError:
        pytest.fail(f"Unknown DataFrame case: {case}")


@when(
    parsers.parse("dataframe sample without replacement with bounds {lower_bound} and {upper_bound}"),
    target_fixture="dataframe",
)
def dataframe_sample_without_replacement_with_bounds(lower_bound, upper_bound, spark):
    """Build a Spark Connect sample plan with independently controlled bounds."""
    dataframe = spark.range(10, numPartitions=1).sample(False, 0.0, 1)
    dataframe._plan.lower_bound = float(lower_bound)  # noqa: SLF001
    dataframe._plan.upper_bound = float(upper_bound)  # noqa: SLF001
    return dataframe


def _sample_input(case, spark):
    cases = {
        "failing projection": lambda: spark.range(1, numPartitions=1).selectExpr(
            "raise_error('projection-error') AS result"
        ),
        "failing filter": lambda: spark.range(1, numPartitions=1).filter("raise_error('filter-error') IS NULL"),
        "unresolved projection": lambda: spark.range(1, numPartitions=1).select("missing"),
        "zero-step range": lambda: spark.range(0, 1, 0, numPartitions=1),
    }
    try:
        return cases[case]()
    except KeyError:
        pytest.fail(f"Unknown sample input: {case}")


@when(
    parsers.parse("dataframe replacement sample fraction {fraction} over {case}"),
    target_fixture="dataframe",
)
def dataframe_replacement_sample(fraction, case, spark):
    """Build a replacement sample over a named input plan."""
    return _sample_input(case, spark).sample(True, float(fraction), 1)


@when(
    parsers.parse("dataframe sample fraction {fraction} over {case}"),
    target_fixture="dataframe",
)
def dataframe_sample(fraction, case, spark):
    """Build a sample without replacement over a named input plan."""
    return _sample_input(case, spark).sample(False, float(fraction), 1)


@then("dataframe schema")
def dataframe_schema(docstring, dataframe):
    """Compare a DataFrame schema with expected schema tree string."""
    assert_schema_tree(dataframe, docstring)


@then(parsers.parse("dataframe error {error}"))
def dataframe_error(error, dataframe):
    """Collect the DataFrame and expect it to fail with an error."""
    with pytest.raises(Exception, match=error):
        _ = dataframe.collect()


@then("dataframe is empty")
def dataframe_is_empty(dataframe):
    """Collect the DataFrame and verify that it has no rows."""
    assert dataframe.collect() == []


def assert_schema_tree(df, docstring):
    """Compare a DataFrame schema with expected schema tree string."""
    if hasattr(df.schema, "treeString"):
        actual = df.schema.treeString()
    else:
        # PySpark < 4.x has no StructType.treeString(); capture printSchema() output instead.
        buf = io.StringIO()
        with redirect_stdout(buf):
            df.printSchema()
        actual = buf.getvalue()
    assert docstring.strip() == actual.strip()


@then(parsers.re("query result(?P<ordered>( ordered)?)"))
def query_result(datatable, ordered, query, spark):
    """Execute the SQL query and compare result with expected data table."""
    header, *rows = datatable
    df = spark.sql(query)
    [h, *r] = parse_show_string(df._show_string(n=0x7FFFFFFF, truncate=False))  # noqa: SLF001
    assert header == h
    if ordered:
        assert rows == r
    else:
        assert sorted(rows) == sorted(r)


@then(parsers.re("dataframe result(?P<ordered>( ordered)?)"))
def dataframe_result(datatable, ordered, dataframe):
    """Collect the DataFrame and compare result with expected data table."""
    header, *rows = datatable
    [h, *r] = parse_show_string(dataframe._show_string(n=0x7FFFFFFF, truncate=False))  # noqa: SLF001
    assert header == h
    if ordered:
        assert rows == r
    else:
        assert sorted(rows) == sorted(r)


def _format_collected_value(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


@then(parsers.re("query result collected(?P<ordered>( ordered)?)"))
def query_result_collected(datatable, ordered, query, spark):
    """Execute the SQL query with collect() and compare result with expected data table."""
    expected_header, *expected_rows = datatable
    df = spark.sql(query)
    actual = [[_format_collected_value(value) for value in row] for row in df.collect()]
    assert expected_header == df.schema.names
    if ordered:
        assert expected_rows == actual
    else:
        assert sorted(expected_rows) == sorted(actual)


@then(parsers.parse("query error {error}"))
def query_error(error, query, spark):
    """Executes the SQL query and expects it to fail with an error (regex match)."""
    with pytest.raises(Exception, match=error):
        _ = spark.sql(query).collect()


@then(parsers.parse('query result has row where "{match_column}" is "{match_value}"'))
def query_result_has_row(match_column: str, match_value: str, query: str, spark) -> None:
    rows = spark.sql(query).collect()
    assert any(str(row[match_column]) == match_value for row in rows)


@then(
    parsers.parse(
        'query result row where "{match_column}" is "{match_value}" has "{value_column}" containing "{substring}"'
    )
)
def query_result_row_value_contains(
    match_column: str,
    match_value: str,
    value_column: str,
    substring: str,
    query: str,
    spark,
) -> None:
    rows = spark.sql(query).collect()
    matches = [row for row in rows if str(row[match_column]) == match_value]
    assert matches
    assert substring in str(matches[0][value_column])


@then(
    parsers.parse(
        'query result row where "{match_column}" is "{match_value}" has "{value_column}" equal to "{expected}"'
    )
)
def query_result_row_value_equals(
    match_column: str,
    match_value: str,
    value_column: str,
    expected: str,
    query: str,
    spark,
) -> None:
    rows = spark.sql(query).collect()
    matches = [row for row in rows if str(row[match_column]) == match_value]
    assert matches
    assert str(matches[0][value_column]) == expected
