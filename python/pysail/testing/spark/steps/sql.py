from __future__ import annotations

import io
import json
import time
import uuid
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from jinja2 import Template
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


@given(parsers.parse("variable {name} for random identifier with prefix {prefix}"), target_fixture="variables")
def variable_for_random_identifier(name, prefix, variables):
    """Defines a variable with a random SQL identifier suffix for scenario-local object names."""
    variables[name] = f"{prefix}{uuid.uuid4().hex[:8]}"
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
    try:
        return cases[case]()
    except KeyError:
        pytest.fail(f"Unknown DataFrame case: {case}")


@then("dataframe schema")
def dataframe_schema(docstring, dataframe):
    """Compare a DataFrame schema with expected schema tree string."""
    assert_schema_tree(dataframe, docstring)


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


@then(parsers.parse("query error {error}"))
def query_error(error, query, spark):
    """Executes the SQL query and expects it to fail with an error (regex match)."""
    with pytest.raises(Exception, match=error):
        _ = spark.sql(query).collect()


@then(parsers.parse('query result has row where "{match_column}" is "{match_value}"'))
def query_result_has_row(match_column: str, match_value: str, query: str, spark, variables) -> None:
    resolved_match_value = Template(match_value).render(**variables)
    rows = spark.sql(query).collect()
    assert any(str(row[match_column]) == resolved_match_value for row in rows)


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
    variables,
) -> None:
    resolved_match_value = Template(match_value).render(**variables)
    resolved_substring = Template(substring).render(**variables)
    rows = spark.sql(query).collect()
    matches = [row for row in rows if str(row[match_column]) == resolved_match_value]
    assert matches
    assert resolved_substring in str(matches[0][value_column])


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
    variables,
) -> None:
    resolved_match_value = Template(match_value).render(**variables)
    resolved_expected = Template(expected).render(**variables)
    rows = spark.sql(query).collect()
    matches = [row for row in rows if str(row[match_column]) == resolved_match_value]
    assert matches
    assert str(matches[0][value_column]) == resolved_expected
