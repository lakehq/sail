from __future__ import annotations

import io
import json
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from pyspark.sql import Row
from jinja2 import Template
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import IntegerType, StringType, StructType
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
    duplicate_struct_data = [
        Row(Row("a", 1), Row(2, 3, "b", 4, "c")),
        Row(Row("x", 6), Row(7, 8, "y", 9, "z")),
    ]
    duplicate_struct_schema = (
        StructType()
        .add("struct", StructType().add("x", StringType()).add("x", IntegerType()))
        .add(
            "struct",
            StructType()
            .add("a", IntegerType())
            .add("x", IntegerType())
            .add("x", StringType())
            .add("y", IntegerType())
            .add("y", StringType()),
        )
    )
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
        "duplicate top-level columns": lambda: spark.range(3).select("id", "id", "id"),
        "duplicate struct fields": lambda: spark.createDataFrame(
            duplicate_struct_data,
            schema=duplicate_struct_schema,
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


@then("dataframe pandas columns")
def dataframe_pandas_columns(docstring, dataframe):
    """Compare Pandas column labels produced by DataFrame.toPandas()."""
    assert list(dataframe.toPandas().columns) == json.loads(docstring)


@then(parsers.parse("dataframe pandas row count is {count:d}"))
def dataframe_pandas_row_count(count, dataframe):
    """Assert DataFrame.toPandas() succeeds and has the expected number of rows."""
    assert len(dataframe.toPandas()) == count


@then("dataframe collect matches duplicate struct rows")
def dataframe_collect_matches_duplicate_struct_rows(dataframe):
    """Assert duplicate nested struct field names do not break Spark Connect collection."""
    expected = [
        Row(Row("a", 1), Row(2, 3, "b", 4, "c")),
        Row(Row("x", 6), Row(7, 8, "y", 9, "z")),
    ]
    assert dataframe.collect() == expected


@then("dataframe pandas dict structs have deduplicated field names")
def dataframe_pandas_dict_structs_have_deduplicated_field_names(dataframe):
    """Assert duplicate nested struct fields are deduplicated using Spark-compatible suffixes."""
    pdf = dataframe.toPandas()
    assert pdf.iloc[0, 0] == {"x_0": "a", "x_1": 1}
    assert pdf.iloc[0, 1] == {"a": 2, "x_0": 3, "x_1": "b", "y_0": 4, "y_1": "c"}


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
