from __future__ import annotations

import json

import pytest
from jinja2 import Template
from pytest_bdd import given, parsers, then, when

from pysail.tests.spark.utils import escape_sql_string_literal, parse_show_string


def normalize_type_name(type_str: str) -> str:
    """Normalize PySpark type names to canonical form.

    PySpark 3.5.x and 4.x use different type name formats.
    This normalizes to the canonical names used in Spark SQL.
    """
    mapping = {
        "integer": "int",
        "long": "bigint",
        "byte": "tinyint",
        "short": "smallint",
    }
    return mapping.get(type_str, type_str)


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


@given(parsers.parse("variable {name} for temporary directory {directory}"), target_fixture="variables")
def variable_for_temporary_directory(name, directory, tmp_path, variables):
    """Defines a variable for a temporary directory with the given name.

    This step does not create the directory, it only stores its absolute path.
    """
    variables[name] = PathWrapper(tmp_path / directory)
    return variables


@given(parsers.parse("config {key} = {value}"))
def config_set(key, value, spark):
    """Sets a Spark configuration value using spark.conf.set()."""
    spark.conf.set(key, value)


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


@when(parsers.re("query(?P<template>( template)?)"), target_fixture="query")
def query(template, docstring, variables):
    """Defines a SQL query (not executed here)."""
    return Template(docstring).render(**variables) if template else docstring


@then("query schema type")
def query_schema_type(datatable, query, spark):
    """Verify the schema types of query result columns.

    Uses a datatable with columns: column, type, nullable (optional).
    Type names are normalized to handle PySpark 3.5.x/4.x differences.
    """
    df = spark.sql(query)
    schema = df.schema

    for row in datatable[1:]:  # Skip header row
        column_name = row[0]
        expected_type = row[1]
        try:
            expected_nullable = row[2].lower() == "true"
        except IndexError:
            expected_nullable = None

        field = schema[column_name]
        actual_type = normalize_type_name(field.dataType.simpleString())

        assert actual_type == expected_type, (
            f"Column '{column_name}': expected type '{expected_type}', got '{actual_type}'"
        )

        if expected_nullable is not None:
            assert field.nullable == expected_nullable, (
                f"Column '{column_name}': expected nullable={expected_nullable}, got nullable={field.nullable}"
            )


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
