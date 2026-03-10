from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pytest_bdd import parsers, then

from pysail.tests.spark.utils import is_jvm_spark

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion

try:
    from jsonpath_ng import parse as jsonpath_parse  # type: ignore[import-not-found]
    from jsonpath_ng.exceptions import JsonPathParserError  # type: ignore[import-not-found]
except ModuleNotFoundError as e:  # pragma: no cover
    jsonpath_parse = None  # type: ignore[assignment]
    JsonPathParserError = Exception  # type: ignore[assignment]
    _JSONPATH_IMPORT_ERROR: ModuleNotFoundError | None = e
else:
    _JSONPATH_IMPORT_ERROR = None


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

    if "timestamp" in normalized:
        normalized["timestamp"] = "<timestamp>"

    cv = normalized.get("clientVersion")
    if isinstance(cv, str) and cv.startswith("sail-delta-lake."):
        normalized["clientVersion"] = "sail-delta-lake.x.x.x"

    ei = normalized.get("engineInfo")
    if isinstance(ei, str) and ei.startswith("sail-delta-lake:"):
        normalized["engineInfo"] = "sail-delta-lake:x.x.x"

    op_metrics = normalized.get("operationMetrics")
    if isinstance(op_metrics, dict):
        scrubbed = dict(op_metrics)
        for k in list(scrubbed.keys()):
            if k.endswith(("TimeMs", "DurationMs", "timeMs")):
                scrubbed[k] = "<time_ms>"
        normalized["operationMetrics"] = scrubbed

    return normalized


class _PathExpressionError(ValueError):
    def __init__(
        self,
        *,
        code: str,
        path: str,
        raw_expression: str | None = None,
        raw: str | None = None,
    ) -> None:
        self.code = code
        self.path = path
        if raw_expression is not None and raw is not None:
            raise ValueError("Specify only one of 'raw_expression' or legacy 'raw'.")  # noqa: TRY003, EM101
        if raw is not None:
            raw_expression = raw
        self.raw_expression = raw_expression
        self.raw = raw_expression
        super().__init__()

    def __str__(self) -> str:
        if self.raw_expression is None:
            return f"{self.code} (path={self.path!r})"
        return f"{self.code} (raw={self.raw_expression!r}, path={self.path!r})"

    @classmethod
    def empty(cls, *, path: str) -> _PathExpressionError:
        return cls(code="empty path", path=path)

    @classmethod
    def invalid(
        cls,
        *,
        path: str,
        raw_expression: str | None = None,
        raw: str | None = None,
    ) -> _PathExpressionError:
        if raw_expression is not None and raw is not None:
            raise ValueError("Specify only one of 'raw_expression' or legacy 'raw'.")  # noqa: TRY003, EM101
        if raw is not None:
            raw_expression = raw
        if raw_expression is None:
            raise ValueError("A raw JSONPath expression must be provided.")  # noqa: TRY003, EM101
        return cls(code="invalid path expression", path=path, raw_expression=raw_expression)


class _JsonPathNgRequiredError(RuntimeError):
    MESSAGE = (
        "jsonpath-ng is required for delta-log path assertions. "
        "Install test dependencies (e.g. `hatch env create` / `pip install -e '.[test]'`)."
    )

    def __init__(self) -> None:
        super().__init__(self.MESSAGE)


_JSONPATH_DQ_KEY_RE = re.compile(r'\["((?:[^"\\]|\\.)*)"\]')


def _normalize_to_jsonpath(path: str) -> str:
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
        raise _PathExpressionError.invalid(path=path, raw_expression=expr_str) from e


def _get_by_path(obj: object, path: str) -> object:
    expr = _compile_jsonpath(path)
    matches = expr.find(obj)
    if not matches or len(matches) != 1:
        raise KeyError(path)
    return matches[0].value


def _pick_paths(obj: object, paths: list[str]) -> object:
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
    s = raw.strip()
    if not s:
        return ""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return raw


def _normalize_column_mapping_schema(schema: object, field_path: str = "") -> object:
    if not isinstance(schema, dict):
        return schema
    normalized = dict(schema)
    if "fields" in normalized and isinstance(normalized["fields"], list):
        for i, field in enumerate(normalized["fields"]):
            current_path = f"{field_path}.{i}" if field_path else str(i)
            if isinstance(field, dict) and "metadata" in field:
                meta = field.get("metadata", {})
                phys = meta.get("delta.columnMapping.physicalName", "")
                if isinstance(phys, str) and phys.startswith("col-"):
                    meta["delta.columnMapping.physicalName"] = f"<physical_name_{current_path}>"
            if isinstance(field, dict) and "type" in field and isinstance(field["type"], dict):
                field["type"] = _normalize_column_mapping_schema(field["type"], field_path=current_path)
    return normalized


def _normalize_delta_metadata_for_snapshot(metadata: dict) -> dict:
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


@pytest.fixture
def delta_log_cache() -> dict[str, dict]:
    """Per-scenario cache for normalized delta log objects."""
    return {}


def _delta_log_compute(which: str, variables: dict, delta_log_cache: dict[str, dict]) -> dict:
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
        assert header, "expected datatable header: path | value"
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
        assert header, "expected datatable with single header column: path"
        assert header[0] == "path", "expected datatable with single header column: path"
        paths = [r[0] for r in rows if r and r[0].strip()]
        obj = _pick_paths(obj, paths)

    assert obj == snapshot
