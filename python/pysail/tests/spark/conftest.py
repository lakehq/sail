from __future__ import annotations

import doctest
import json
import os
import re
import textwrap
import time
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _pytest.doctest import DoctestItem
from jinja2 import Template
from pyspark.sql import SparkSession
from pytest_bdd import given, parsers, then, when
from syrupy.extensions.single_file import SingleFileSnapshotExtension

from pysail.spark import SparkConnectServer
from pysail.tests.spark.utils import SAIL_ONLY, escape_sql_string_literal, is_jvm_spark, parse_show_string

if TYPE_CHECKING:
    import pyspark.sql.connect.session
    from syrupy.assertion import SnapshotAssertion
    from syrupy.types import SerializableData

try:
    from jsonpath_ng import parse as jsonpath_parse  # type: ignore[import-not-found]
    from jsonpath_ng.exceptions import JsonPathParserError  # type: ignore[import-not-found]
except ModuleNotFoundError as e:  # pragma: no cover
    # Delta-log path assertions require `jsonpath-ng` (test extras).
    jsonpath_parse = None  # type: ignore[assignment]
    JsonPathParserError = Exception  # type: ignore[assignment]
    _JSONPATH_IMPORT_ERROR: ModuleNotFoundError | None = e
else:
    _JSONPATH_IMPORT_ERROR = None


def normalize_plan_text(plan_text: str) -> str:
    """Normalize plan text by scrubbing non-deterministic fields."""
    text = textwrap.dedent(plan_text).strip()
    # Make Windows paths match the regexes and snapshots early, so the
    # raw-text substitutions below also work cross-platform.
    text = text.replace("\\", "/")
    text = re.sub(r"([A-Za-z][A-Za-z0-9+.\-]*:)//", r"\1__SCHEME_SLASHSLASH__", text)
    text = re.sub(r"/{2,}", "/", text)
    text = text.replace("__SCHEME_SLASHSLASH__", "//")

    def _normalize_metrics_block(match: re.Match[str]) -> str:
        body = match.group(1)
        body = re.sub(r"=\s*[^,\]]+", "=<metric>", body)
        body = re.sub(r"-?\d+(?:\.\d+)?", "<metric>", body)
        return f", metrics=[{body}]"

    text = re.sub(r", metrics=\[([^\]]*)\]", _normalize_metrics_block, text)
    text = re.sub(r"Hash\(\[([^\]]+)\], \d+\)", r"Hash([\1], <partitions>)", text)
    text = re.sub(r"RoundRobinBatch\(\d+\)", r"RoundRobinBatch(<partitions>)", text)
    text = re.sub(r"input_partitions=\d+", r"input_partitions=<partitions>", text)
    text = re.sub(r"partition_sizes=\[[^\]]+\]", r"partition_sizes=[<sizes>]", text)

    # Normalize temp paths / file URIs that appear in plans.
    pytest_tmp_prefix = re.compile(
        # Match (and scrub) the pytest per-test tmp root prefix, cross-platform.
        #
        # Works for e.g.
        # - macOS: /private/var/folders/.../T/pytest-of-<user>/pytest-1535/test_xxx_0/
        # - Linux: /tmp/pytest-of-runner/pytest-0/test_xxx_0/
        # - Windows (after `\` -> `/`): C:/Users/.../pytest-of-<user>/pytest-0/test_xxx_0/
        #
        # Also matches relative-looking ones (private/var/...) that sometimes
        # show up in formatted plans.
        r"(^|[\s\[\(=,:{\"])"  # delimiter (kept)
        r"(?!\[)"  # avoid starting at the first `[` of `[[...]]`
        # Don't accidentally start matching at identifiers like `file_groups=...`.
        # Require the path to start like an absolute path (`/` or `C:/`) or a
        # known relative tmp prefix (`private/...` or `tmp/...`).
        r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
        r"(?:[^ \t\r\n\),\]]+/)*"
        r"pytest-of-[^/]+/pytest-\d+/[^/]+/",
        re.IGNORECASE,
    )

    def normalize_path(path: str) -> str:
        # Make Windows paths match the regexes and snapshots.
        path = path.replace("\\", "/")
        path = pytest_tmp_prefix.sub(lambda m: f"{m.group(1)}<tmp>/", path)
        return re.sub(
            r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
            "part-<id>.snappy.parquet",
            path,
            flags=re.IGNORECASE,
        )

    text = re.sub(
        r"table_path=file://([^\s\),]+)",
        lambda m: f"table_path=file://{normalize_path(m.group(1))}",
        text,
    )
    text = re.sub(
        r'location: "([^"]+)"',
        lambda m: f'location: "{normalize_path(m.group(1))}"',
        text,
    )
    # Normalize raw path occurrences (e.g. parquet file groups) without destroying structure.
    text = pytest_tmp_prefix.sub(lambda m: f"{m.group(1)}<tmp>/", text)
    text = re.sub(
        r"part-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-c\d+\.snappy\.parquet",
        "part-<id>.snappy.parquet",
        text,
        flags=re.IGNORECASE,
    )

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


def _latest_commit_info(table_location: Path) -> dict:
    log_dir = table_location / "_delta_log"
    logs = sorted(log_dir.glob("*.json"))
    assert logs, f"no delta logs found in {log_dir}"
    latest = logs[-1]
    with latest.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "commitInfo" in obj:
                return obj["commitInfo"]
    msg = f"commitInfo action not found in latest delta log: {latest}"
    raise AssertionError(msg)


def _first_commit_actions(table_location: Path) -> dict:
    """Extract protocol and metaData from the first delta log commit (version 0)."""
    log_dir = table_location / "_delta_log"
    first_log = log_dir / "00000000000000000000.json"
    assert first_log.exists(), f"first delta log not found: {first_log}"
    result = {}
    with first_log.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "protocol" in obj:
                result["protocol"] = obj["protocol"]
            if "metaData" in obj:
                result["metaData"] = obj["metaData"]
    return result


def _first_commit_actions_from_variables(variables: dict) -> dict:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for delta log inspection"
    return _first_commit_actions(Path(location.path))


def _latest_commit_info_from_variables(variables: dict) -> dict:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for delta log inspection"
    return _latest_commit_info(Path(location.path))


def _recursive_parse_json_strings(value):
    """Parse JSON-looking strings recursively (e.g. commitInfo.operationParameters)."""
    if isinstance(value, dict):
        return {k: _recursive_parse_json_strings(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_recursive_parse_json_strings(v) for v in value]
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return value
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return value
        return _recursive_parse_json_strings(parsed)
    return value


def _normalize_delta_commit_info_for_snapshot(commit_info: dict) -> dict:
    """Normalize volatile / version-specific fields but keep the keys in the snapshot."""
    normalized = dict(commit_info)

    # Timestamp is volatile.
    if "timestamp" in normalized:
        normalized["timestamp"] = "<timestamp>"

    # Engine/client versions are volatile.
    cv = normalized.get("clientVersion")
    if isinstance(cv, str) and cv.startswith("sail-delta-lake."):
        normalized["clientVersion"] = "sail-delta-lake.x.x.x"

    ei = normalized.get("engineInfo")
    if isinstance(ei, str) and ei.startswith("sail-delta-lake:"):
        normalized["engineInfo"] = "sail-delta-lake:x.x.x"

    # Time-related metrics are nondeterministic.
    op_metrics = normalized.get("operationMetrics")
    if isinstance(op_metrics, dict):
        scrubbed = dict(op_metrics)
        for k in list(scrubbed.keys()):
            if k.endswith(("TimeMs", "DurationMs", "timeMs")):
                scrubbed[k] = "<time_ms>"
        normalized["operationMetrics"] = scrubbed

    return normalized


def _get_by_path(obj: object, path: str) -> object:
    expr = _compile_jsonpath(path)
    matches = expr.find(obj)
    if not matches:
        raise KeyError(path)
    if len(matches) != 1:
        raise KeyError(path)
    return matches[0].value


def _pick_paths(obj: object, paths: list[str]) -> object:
    """Project a subset of fields by JSONPath; missing/ambiguous paths raise KeyError."""
    out: object = {}
    for p in paths:
        expr = _compile_jsonpath(p)
        v = _get_by_path(obj, p)
        try:
            expr.update_or_create(out, v)
        except Exception as e:
            raise KeyError(p) from e
    return out


def _parse_expected_value(raw: str) -> object:
    """Parse a datatable value as JSON when possible; otherwise keep it as a string."""
    s = raw.strip()
    if not s:
        return ""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return raw


@pytest.fixture
def delta_log_cache() -> dict[str, dict]:
    """Per-scenario cache for normalized delta log objects."""
    return {}


def _delta_log_compute(which: str, variables: dict, delta_log_cache: dict[str, dict]) -> dict:
    """Compute + normalize the requested delta-log object (cached per-scenario)."""
    cached = delta_log_cache.get(which)
    if isinstance(cached, dict):
        return cached

    if which == "latest commit info":
        obj = _latest_commit_info_from_variables(variables)
        obj = _normalize_delta_commit_info_for_snapshot(obj)
        if "operationParameters" in obj:
            obj["operationParameters"] = _recursive_parse_json_strings(obj["operationParameters"])
    else:
        obj = _first_commit_actions_from_variables(variables)
        assert "protocol" in obj, "protocol action not found in first commit"
        assert "metaData" in obj, "metaData action not found in first commit"
        obj["metaData"] = _normalize_delta_metadata_for_snapshot(obj["metaData"])

    delta_log_cache[which] = obj
    return obj


@then(
    parsers.re(
        r"delta log (?P<which>latest commit info|first commit protocol and metadata) "
        r"(?P<mode>matches snapshot(?: for paths)?|contains)"
    )
)
def delta_log_assert(
    which: str,
    mode: str,
    variables,
    delta_log_cache: dict[str, dict],
    snapshot: SnapshotAssertion,
    datatable=None,
):
    """Delta log assertions: snapshot whole/subset, or assert specific paths."""
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    obj = _delta_log_compute(which, variables, delta_log_cache)

    if mode == "contains":
        assert datatable is not None, "expected a datatable: | path | value |"
        header, *rows = datatable
        assert header, "expected datatable header"
        assert header[:2] == ["path", "value"], "expected datatable header: path | value"
        for row in rows:
            if not row or not row[0].strip():
                continue
            path, raw = row[0], row[1] if len(row) > 1 else ""
            expected = _parse_expected_value(raw)
            actual = _get_by_path(obj, path)
            assert actual == expected, f"path {path!r} expected {expected!r}, got {actual!r}"
        return

    if mode == "matches snapshot for paths":
        assert datatable is not None, "expected a datatable: | path |"
        header, *rows = datatable
        assert header, "expected datatable header"
        assert header[0] == "path", "expected datatable with single header column: path"
        paths = [r[0] for r in rows if r and r[0].strip()]
        obj = _pick_paths(obj, paths)

    assert obj == snapshot


class _PathExpressionError(ValueError):
    def __init__(self, *, code: str, path: str, raw: str | None = None) -> None:
        self.code = code
        self.path = path
        self.raw = raw
        super().__init__()

    def __str__(self) -> str:
        if self.raw is None:
            return f"{self.code} (path={self.path!r})"
        return f"{self.code} (raw={self.raw!r}, path={self.path!r})"

    @classmethod
    def empty(cls, *, path: str) -> _PathExpressionError:
        return cls(code="empty path", path=path)

    @classmethod
    def invalid(cls, *, path: str, raw: str) -> _PathExpressionError:
        return cls(code="invalid path expression", path=path, raw=raw)


class _JsonPathNgRequiredError(RuntimeError):
    MESSAGE = (
        "jsonpath-ng is required for delta-log path assertions. "
        "Install test dependencies (e.g. `hatch env create` / `pip install -e '.[test]'`)."
    )

    def __init__(self) -> None:
        super().__init__(self.MESSAGE)


_JSONPATH_DQ_KEY_RE = re.compile(r'\["((?:[^"\\]|\\.)*)"\]')


def _normalize_to_jsonpath(path: str) -> str:
    """Normalize input into jsonpath-ng-friendly JSONPath."""
    p = path.strip()
    if not p:
        raise _PathExpressionError.empty(path=path)
    if not p.startswith("$"):
        p = "$." + p

    def repl(m: re.Match[str]) -> str:
        try:
            val = json.loads(f'"{m.group(1)}"')
        except json.JSONDecodeError:
            return m.group(0)
        val = val.replace("\\", "\\\\").replace("'", "\\'")
        return f"['{val}']"

    return _JSONPATH_DQ_KEY_RE.sub(repl, p)


@lru_cache(maxsize=1024)
def _compile_jsonpath(path: str):
    if jsonpath_parse is None:  # pragma: no cover
        raise _JsonPathNgRequiredError from _JSONPATH_IMPORT_ERROR
    expr_str = _normalize_to_jsonpath(path)
    try:
        return jsonpath_parse(expr_str)
    except (JsonPathParserError, Exception) as e:
        raise _PathExpressionError.invalid(path=path, raw=expr_str) from e


def _normalize_column_mapping_schema(schema: dict) -> dict:
    """Normalize schemaString for snapshot stability (scrub physicalName UUIDs)."""
    if not isinstance(schema, dict):
        return schema

    normalized = dict(schema)

    if "fields" in normalized and isinstance(normalized["fields"], list):
        for i, field in enumerate(normalized["fields"]):
            if isinstance(field, dict) and "metadata" in field:
                meta = field.get("metadata", {})
                phys = meta.get("delta.columnMapping.physicalName", "")
                if isinstance(phys, str) and phys.startswith("col-"):
                    meta["delta.columnMapping.physicalName"] = f"<physical_name_{i}>"
                if "type" in field and isinstance(field["type"], dict):
                    field["type"] = _normalize_column_mapping_schema(field["type"])
    return normalized


def _normalize_delta_metadata_for_snapshot(metadata: dict) -> dict:
    """Normalize metaData action for stable snapshots."""
    normalized = dict(metadata)

    if "id" in normalized:
        normalized["id"] = "<table_id>"

    if "createdTime" in normalized:
        normalized["createdTime"] = "<created_time>"

    if "schemaString" in normalized:
        try:
            schema = json.loads(normalized["schemaString"])
            schema = _normalize_column_mapping_schema(schema)
            normalized["schemaString"] = schema
        except json.JSONDecodeError:
            pass

    return normalized
