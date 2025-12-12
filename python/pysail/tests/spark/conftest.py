import doctest
import json
import os
import platform
import re
import textwrap
import time

import pyspark.sql.connect.session
import pytest
from _pytest.doctest import DoctestItem
from jinja2 import Template
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, then, when
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.single_file import SingleFileSnapshotExtension
from syrupy.types import SerializableData

from pysail.spark import SparkConnectServer
from pysail.tests.spark.utils import SAIL_ONLY, escape_sql_string_literal, is_jvm_spark, parse_show_string


def normalize_plan_text(plan_text: str) -> str:
    """Normalize plan text by scrubbing non-deterministic fields."""
    text = textwrap.dedent(plan_text).strip()
    text = re.sub(r", metrics=\[[^\]]*\]", "", text)
    text = re.sub(r"Hash\(\[([^\]]+)\], \d+\)", r"Hash([\1], <partitions>)", text)
    text = re.sub(r"RoundRobinBatch\(\d+\)", r"RoundRobinBatch(<partitions>)", text)
    text = re.sub(r"input_partitions=\d+", r"input_partitions=<partitions>", text)
    text = re.sub(r"partition_sizes=\[[^\]]+\]", r"partition_sizes=[<sizes>]", text)
    text = re.sub(r"Bytes=Exact\(\d+\)", r"Bytes=Exact(<bytes>)", text)
    return re.sub(r"Bytes=Inexact\(\d+\)", r"Bytes=Inexact(<bytes>)", text)


def _collect_plan(query: str, spark) -> str:
    """Execute query and extract the single-row plan string."""
    df = spark.sql(query)
    rows = df.collect()
    assert len(rows) == 1, f"expected single row, got {len(rows)}"
    plan = rows[0][0]
    assert isinstance(plan, str), "expected string plan output"
    assert plan, "expected non-empty plan output"
    return plan


class PlanSnapshotExtension(SingleFileSnapshotExtension):
    """Snapshot extension that stores normalized plan text."""

    file_extension = "plan"

    def serialize(self, data: SerializableData, **_: object) -> str:
        return normalize_plan_text(str(data)).encode()


@pytest.fixture(scope="session")
def remote():
    if r := os.environ.get("SPARK_REMOTE"):
        yield r
    else:
        server = SparkConnectServer("127.0.0.1", 0)
        if os.environ.get("SAIL_TEST_INIT_TELEMETRY") == "1":
            server.init_telemetry()
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()


@pytest.fixture(scope="module")
def spark(remote):
    spark = SparkSession.builder.remote(remote).getOrCreate()
    configure_spark_session(spark)
    patch_spark_connect_session(spark)
    yield spark
    spark.stop()


def configure_spark_session(session):
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    # Enable Arrow to avoid data type errors when creating Spark DataFrame from Pandas.
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def patch_spark_connect_session(session: pyspark.sql.connect.session.SparkSession):
    """
    Patch the Spark Connect session to avoid deadlock when closing the session.
    """
    f = session._client.close  # noqa: SLF001

    def close():
        if session._client._closed:  # noqa: SLF001
            return
        return f()

    session._client.close = close  # noqa: SLF001


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace, spark):
    # The Spark session is scoped to each module, so that the registered
    # temporary views and UDFs do not interfere with each other.
    doctest_namespace["spark"] = spark


@pytest.fixture
def session_timezone(spark, request):
    tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", request.param)
    yield
    spark.conf.set("spark.sql.session.timeZone", tz)


@pytest.fixture
def local_timezone(request):
    tz = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if tz is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = tz
    time.tzset()


def pytest_collection_modifyitems(session, config, items):  # noqa: ARG001
    if is_jvm_spark():
        for item in items:
            if isinstance(item, DoctestItem):
                for example in item.dtest.examples:
                    if example.options.get(SAIL_ONLY):
                        example.options[doctest.SKIP] = True


@pytest.fixture
def variables():
    """The variables dictionary for storing variables defined in the steps
    for tests defined in `.feature` files.
    The variables are scoped to a single test that corresponds to a scenario
    in a `.feature` file.
    """
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
    The temporary directory is located inside the temporary directory that pytest creates
    for the test.

    Note that this step does not actually create the directory on the file system,
    but only defines the variable for the path.
    The value is a `PathWrapper` object for the absolute path of the temporary directory.
    """
    variables[name] = PathWrapper(tmp_path / directory)
    return variables


@given(parsers.re("statement(?P<template>( template)?)"))
def statement(template, docstring, spark, variables):
    """Executes a SQL statement that is expected to succeed.

    If the `template` suffix is present, the statement is treated as a Jinja2 template
    and rendered with the variables defined in the previous steps.
    """
    s = Template(docstring).render(**variables) if template else docstring
    spark.sql(s)


@given(parsers.re("statement(?P<template>( template)?) with error {error}"))
def statement_with_error(template, error, docstring, spark, variables):
    """Executes a SQL statement that is expected to fail with an error.

    If the `template` suffix is present, the statement is treated as a Jinja2 template
    and rendered with the variables defined in the previous steps.
    """
    s = Template(docstring).render(**variables) if template else docstring
    with pytest.raises(Exception, match=error):
        spark.sql(s)


@given(parsers.re("final statement(?P<template>( template)?)"))
def final_statement(template, docstring, spark, variables):
    """Executes a SQL statement at the end of a scenario.
    This step should be defined early in the scenario to ensure it is executed
    even when subsequent steps fail.

    If the `template` suffix is present, the statement is treated as a Jinja2 template
    and rendered with the variables defined in the previous steps.
    """
    s = Template(docstring).render(**variables) if template else docstring
    yield
    spark.sql(s)


@when(parsers.re("query(?P<template>( template)?)"), target_fixture="query")
def query(template, docstring, variables):
    """Defines a SQL query.
    The query is not executed in this step.

    If the `template` suffix is present, the query is treated as a Jinja2 template
    and rendered with the variables defined in the previous steps.
    """
    return Template(docstring).render(**variables) if template else docstring


@then("query schema")
def query_schema(docstring, query, spark):
    """Analyzes the SQL query defined in a previous step
    and compares the schema with the expected schema.
    The expected schema is given in the same format as `StructType.treeString()`.
    """
    df = spark.sql(query)
    assert docstring.strip() == df.schema.treeString().strip()


@then(parsers.re("query result(?P<ordered>( ordered)?)"))
def query_result(datatable, ordered, query, spark):
    """Executes the SQL query defined in a previous step
    and compares the result with the expected data table.
    By default, the query result rows can be in any order.

    If the step is defined with the `ordered` suffix,
    the query result rows must match the expected rows in order.
    """
    header, *rows = datatable
    df = spark.sql(query)
    [h, *r] = parse_show_string(df._show_string(n=0x7FFFFFFF, truncate=False))  # noqa: SLF001
    assert header == h
    if ordered:
        assert rows == r
    else:
        assert sorted(rows) == sorted(r)


@then("query plan matches snapshot")
def query_plan_matches_snapshot(query, spark, snapshot: SnapshotAssertion):
    """Executes the SQL query and only asserts against the stored snapshot."""

    plan = _collect_plan(query, spark)
    if platform.system() == "Windows":
        # always read the snapshot since unused snapshots are reported as errors
        try:
            assert snapshot(extension_class=PlanSnapshotExtension) == plan
        except AssertionError:
            pytest.skip("snapshot line break is not compatible on Windows")
    else:
        assert snapshot(extension_class=PlanSnapshotExtension) == plan


@then(parsers.parse("query error {error}"))
def query_error(error, query, spark):
    """Executes the SQL query defined in a previous step
    and expects it to fail with an error.
    The expected error message is given as a regular expression
    that will be used to partially match the actual error message.
    """
    with pytest.raises(Exception, match=error):
        _ = spark.sql(query).collect()
