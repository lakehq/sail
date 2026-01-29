from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import pytest
from jinja2 import Template
from pytest_bdd import given, parsers, then, when
from syrupy.extensions.single_file import SingleFileSnapshotExtension

from pysail.tests.spark.steps.plan import normalize_plan_text
from pysail.tests.spark.utils import escape_sql_string_literal, parse_show_string

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


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


@then("query schema")
def query_schema(docstring, query, spark):
    """Analyze the SQL query and compare schema with expected schema tree string."""
    df = spark.sql(query)
    assert docstring.strip() == df.schema.treeString().strip()


@then("query columns")
def query_columns(datatable, query, spark):
    """Execute the SQL query and compare column list."""
    _header, *rows = datatable
    expected = [row[0] for row in rows]
    df = spark.sql(query)
    assert expected == df.columns


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


def normalize_query_output(output: str) -> str:
    text = normalize_plan_text(output)
    text = re.sub(r"(modification_time: )\d+", r"\1<time_ms>", text)
    text = re.sub(r"(deletion_timestamp: )\d+", r"\1<time_ms>", text)
    text = re.sub(
        r"(\{add=\{[^,]+, \{[^}]*\}, \d+, )\d+",
        r"\1<time_ms>",
        text,
    )
    decoder = json.JSONDecoder()
    normalized: list[str] = []
    idx = 0
    while idx < len(text):
        if text[idx] != "{":
            normalized.append(text[idx])
            idx += 1
            continue
        try:
            obj, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            normalized.append(text[idx])
            idx += 1
            continue
        if isinstance(obj, dict):
            for key in ("executionTimeMs", "writeTimeMs"):
                if key in obj:
                    obj[key] = "__TIME__"
            rendered = json.dumps(obj, sort_keys=True, separators=(",", ":")).replace('"__TIME__"', "<time_ms>")
        else:
            rendered = json.dumps(obj, separators=(",", ":"))
        normalized.append(rendered)
        idx = end
    return "".join(normalized)


class QueryOutputSnapshotExtension(SingleFileSnapshotExtension):
    """Snapshot extension that stores normalized query output."""

    file_extension = "out"

    def serialize(self, data, **_: object) -> bytes:
        return normalize_query_output(str(data)).encode()


@then("query output matches snapshot")
def query_output_matches_snapshot(query, spark, snapshot: SnapshotAssertion):
    """Executes the SQL query and compares output with snapshot."""
    df = spark.sql(query)
    output = df._show_string(n=0x7FFFFFFF, truncate=False)  # noqa: SLF001
    assert snapshot(extension_class=QueryOutputSnapshotExtension) == output


@then(parsers.parse("query error {error}"))
def query_error(error, query, spark):
    """Executes the SQL query and expects it to fail with an error (regex match)."""
    with pytest.raises(Exception, match=error):
        _ = spark.sql(query).collect()
