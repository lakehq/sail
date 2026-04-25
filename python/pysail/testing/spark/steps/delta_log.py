from __future__ import annotations

import json
import os
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pytest_bdd import given, parsers, then

from pysail.testing.spark.utils.common import is_jvm_spark

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
    if "inCommitTimestamp" in normalized:
        normalized["inCommitTimestamp"] = "<in_commit_timestamp>"

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


def _normalize_delta_log_json_file_for_snapshot(filename: str, obj: object) -> object:
    if not isinstance(obj, dict):
        return obj
    if filename.endswith(".crc"):
        normalized = dict(obj)
        if "inCommitTimestampOpt" in normalized:
            normalized["inCommitTimestampOpt"] = "<in_commit_timestamp>"
        metadata = normalized.get("metadata")
        if isinstance(metadata, dict):
            normalized["metadata"] = _normalize_delta_metadata_for_snapshot(metadata)
        return normalized
    return obj


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
    elif which == "latest effective protocol and metadata":
        obj = _latest_effective_protocol_and_metadata_from_variables(variables)
        assert "protocol" in obj, "protocol action not found in delta log"
        assert "metaData" in obj, "metaData action not found in delta log"
        obj["metaData"] = _normalize_delta_metadata_for_snapshot(obj["metaData"])
    else:
        obj = _first_commit_actions_from_variables(variables)
        assert "protocol" in obj, "protocol action not found in first commit"
        assert "metaData" in obj, "metaData action not found in first commit"
        obj["metaData"] = _normalize_delta_metadata_for_snapshot(obj["metaData"])

    delta_log_cache[which] = obj
    return obj


def _read_delta_log_json_file(location: Path, filename: str) -> object:
    file_path = location / filename
    assert file_path.exists(), f"delta log file does not exist: {file_path}"
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_version_list(raw: str) -> list[int]:
    versions = []
    for raw_part in raw.split(","):
        version_text = raw_part.strip()
        if not version_text:
            continue
        versions.append(int(version_text))
    return versions


def _parse_i64_list(raw: str) -> list[int]:
    values = []
    for raw_part in raw.split(","):
        value_text = raw_part.strip()
        if not value_text:
            continue
        values.append(int(value_text))
    return values


@then(
    parsers.re(
        r"delta log (?P<which>latest commit info|first commit protocol and metadata|latest effective protocol and metadata) "
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


@then(parsers.parse("delta log JSON file {filename} in {location_var} matches snapshot"))
def delta_log_json_file_matches_snapshot(
    filename: str,
    location_var: str,
    variables: dict,
    snapshot: SnapshotAssertion,
) -> None:
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    obj = _read_delta_log_json_file(Path(location.path), filename)
    obj = _normalize_delta_log_json_file_for_snapshot(filename, obj)
    assert obj == snapshot


@then(parsers.parse("delta log JSON file {filename} in {location_var} contains"))
def delta_log_json_file_contains(
    filename: str,
    location_var: str,
    variables: dict,
    datatable,
) -> None:
    """Assert that specific fields in a delta log JSON file match expected values.

    The datatable must have two columns: ``path`` and ``value``.
    ``path`` is a JSONPath expression (without leading ``$``).
    ``value`` is a JSON-encoded expected value.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    obj = _read_delta_log_json_file(Path(location.path), filename)

    assert datatable is not None, "expected a datatable: | path | value |"
    header, *rows = datatable
    assert len(header) == 2 and header[0] == "path" and header[1] == "value", (  # noqa: PLR2004 PT018
        "expected datatable with columns: | path | value |"
    )
    for row in rows:
        if not row or len(row) < 2:  # noqa: PLR2004
            continue
        path, raw_value = row[0], row[1]
        actual = _get_by_path(obj, path)
        expected = _parse_expected_value(raw_value)
        assert actual == expected, f"field {path!r}: expected {expected!r}, got {actual!r}"


@given(
    parsers.parse("delta log JSON files for versions {versions} in {location_var} are backdated by {seconds:d} seconds")
)
def delta_log_json_files_are_backdated(
    versions: str,
    location_var: str,
    seconds: int,
    variables: dict,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    target_timestamp = time.time() - seconds
    log_dir = Path(location.path)
    parsed_versions = _parse_version_list(versions)
    assert parsed_versions, "expected at least one Delta log version to backdate"

    for version in parsed_versions:
        log_file = log_dir / f"{version:020}.json"
        assert log_file.exists(), f"Delta log JSON file does not exist: {log_file}"
        os.utime(log_file, (target_timestamp, target_timestamp))


@given(
    parsers.parse(
        "delta log JSON file timestamps for versions {versions} in {location_var} are {timestamps} seconds since epoch"
    )
)
def delta_log_json_file_timestamps_are_set(
    versions: str,
    location_var: str,
    timestamps: str,
    variables: dict,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    parsed_versions = _parse_version_list(versions)
    parsed_timestamps = _parse_i64_list(timestamps)
    assert parsed_versions, "expected at least one Delta log version to rewrite"
    assert len(parsed_versions) == len(parsed_timestamps), "expected the same number of versions and timestamps"

    log_dir = Path(location.path)
    for version, timestamp in zip(parsed_versions, parsed_timestamps, strict=True):
        log_file = log_dir / f"{version:020}.json"
        assert log_file.exists(), f"Delta log JSON file does not exist: {log_file}"
        os.utime(log_file, (timestamp, timestamp))


@given(
    parsers.parse(
        "delta log commit and checksum timestamps for versions {versions} in {location_var} "
        "are {timestamps} milliseconds since epoch"
    )
)
def delta_log_commit_timestamps_are_rewritten(
    versions: str,
    location_var: str,
    timestamps: str,
    variables: dict,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    parsed_versions = _parse_version_list(versions)
    parsed_timestamps = _parse_i64_list(timestamps)
    assert parsed_versions, "expected at least one Delta log version to rewrite"
    assert len(parsed_versions) == len(parsed_timestamps), "expected the same number of versions and timestamps"

    log_dir = Path(location.path)

    for version, timestamp_ms in zip(parsed_versions, parsed_timestamps, strict=True):
        log_path = log_dir / f"{version:020}.json"
        assert log_path.exists(), f"Delta log JSON file does not exist: {log_path}"

        rewritten = []
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if "commitInfo" in obj:
                    obj["commitInfo"]["inCommitTimestamp"] = timestamp_ms
                rewritten.append(json.dumps(obj, separators=(",", ":")))

        with log_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(rewritten))

        crc_path = log_dir / f"{version:020}.crc"
        assert crc_path.exists(), f"Delta log checksum file does not exist: {crc_path}"
        with crc_path.open("r", encoding="utf-8") as f:
            crc_obj = json.load(f)
        crc_obj["inCommitTimestampOpt"] = timestamp_ms
        with crc_path.open("w", encoding="utf-8") as f:
            json.dump(crc_obj, f, separators=(",", ":"))


def _latest_effective_protocol_and_metadata_from_variables(variables: dict) -> dict:
    """Replay all delta log commits to determine the latest effective protocol and metadata."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for delta log inspection"
    log_dir = Path(location.path) / "_delta_log"
    log_files = sorted(f for f in log_dir.glob("*.json") if not f.stem.endswith(".compacted"))
    assert log_files, f"no delta logs found in {log_dir}"

    result: dict = {}
    for log_file in log_files:
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if "protocol" in obj:
                    result["protocol"] = obj["protocol"]
                if "metaData" in obj:
                    result["metaData"] = obj["metaData"]
    return result


def _checkpoint_row_to_dict(table, row_index: int) -> dict:
    """Convert a single row of a pyarrow checkpoint Table to nested Python dicts."""
    row = {}
    for col_name in table.column_names:
        col = table.column(col_name)
        value = col[row_index].as_py()
        row[col_name] = value
    return row


def _load_checkpoint_parquet(location: Path, filename: str) -> list[dict]:
    """Load a checkpoint parquet file and return a list of nested dict rows."""
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415
    except ModuleNotFoundError as e:  # pragma: no cover
        msg = "pyarrow is required for checkpoint parquet assertions"
        raise RuntimeError(msg) from e

    log_dir = location / "_delta_log"
    cp_path = log_dir / filename
    assert cp_path.exists(), f"checkpoint parquet not found: {cp_path}"
    table = pq.read_table(cp_path)
    rows: list[dict] = []
    for i in range(table.num_rows):
        rows.append(_checkpoint_row_to_dict(table, i))  # noqa: PERF401
    return rows


@then(parsers.parse("checkpoint parquet file {filename} in {location_var} contains add fields"))
def checkpoint_parquet_contains_add_fields(
    filename: str,
    location_var: str,
    variables: dict,
    datatable,
) -> None:
    """Assert fields inside the ``add`` struct of a checkpoint parquet file match expected values.

    The datatable must have two columns ``path`` and ``value``. ``path`` is a JSONPath
    expression into the ``add`` struct — for example ``stats_parsed.minValues.id``,
    ``partitionValues_parsed.year``, ``baseRowId``, or ``defaultRowCommitVersion``.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    rows = _load_checkpoint_parquet(Path(location.path), filename)
    add_rows = [r.get("add") for r in rows if r.get("add") is not None]

    assert datatable is not None, "expected a datatable: | path | value |"
    header, *dtable_rows = datatable
    assert len(header) == 2 and header[0] == "path" and header[1] == "value", (  # noqa: PLR2004 PT018
        "expected datatable with columns: | path | value |"
    )
    for drow in dtable_rows:
        if not drow or len(drow) < 2:  # noqa: PLR2004
            continue
        path, raw_value = drow[0], drow[1]
        expected = _parse_expected_value(raw_value)
        # Try each add row until one satisfies the path; report last actual on failure.
        last_actual: object = None
        matched = False
        for add in add_rows:
            try:
                actual = _get_by_path(add, path)
            except KeyError:
                actual = None
            last_actual = actual
            if actual == expected:
                matched = True
                break
        assert matched, f"field {path!r}: expected {expected!r}, got {last_actual!r} across {len(add_rows)} add rows"


@then(parsers.parse("checkpoint parquet file {filename} in {location_var} does not contain add sub-field {field}"))
def checkpoint_parquet_add_missing_field(
    filename: str,
    location_var: str,
    field: str,
    variables: dict,
) -> None:
    """Assert that the ``add`` struct does NOT have a direct sub-field named ``field``.

    Useful to check that ``stats_parsed`` / ``partitionValues_parsed`` are absent when
    the corresponding property is off.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    try:
        import pyarrow.parquet as pq  # noqa: PLC0415
    except ModuleNotFoundError as e:  # pragma: no cover
        msg = "pyarrow is required for checkpoint parquet assertions"
        raise RuntimeError(msg) from e

    log_dir = Path(location.path) / "_delta_log"
    cp_path = log_dir / filename
    assert cp_path.exists(), f"checkpoint parquet not found: {cp_path}"
    schema = pq.read_schema(cp_path)
    add_field = schema.field("add") if "add" in schema.names else None
    assert add_field is not None, "checkpoint schema must have an 'add' column"
    sub_names = {f.name for f in add_field.type}
    assert field not in sub_names, (
        f"expected 'add' struct to NOT have sub-field {field!r}; found fields: {sorted(sub_names)}"
    )


def _load_delta_log_actions(location: Path, filename: str) -> list[dict]:
    """Parse an NDJSON Delta commit log file and return the list of action objects."""
    file_path = location / filename
    if not file_path.exists():
        candidate = location / "_delta_log" / filename
        if candidate.exists():
            file_path = candidate
    assert file_path.exists(), f"delta log file does not exist: {file_path}"
    actions: list[dict] = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            actions.append(json.loads(stripped))
    return actions


@then(parsers.parse("delta log commit {filename} in {location_var} contains action"))
def delta_log_commit_action_contains(
    filename: str,
    location_var: str,
    variables: dict,
    datatable,
) -> None:
    """Assert that at least one action object in the NDJSON commit log has a specific
    JSONPath-addressable field equal to an expected value.

    The datatable must have two columns ``path`` and ``value``. ``path`` is a JSONPath
    expression applied to each action object — for example ``add.baseRowId`` or
    ``domainMetadata.configuration``. The assertion passes when ANY action in the file
    yields a match; use separate rows to assert multiple invariants.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    actions = _load_delta_log_actions(Path(location.path), filename)

    assert datatable is not None, "expected a datatable: | path | value |"
    header, *rows = datatable
    assert len(header) == 2 and header[0] == "path" and header[1] == "value", (  # noqa: PLR2004 PT018
        "expected datatable with columns: | path | value |"
    )
    for drow in rows:
        if not drow or len(drow) < 2:  # noqa: PLR2004
            continue
        path, raw_value = drow[0], drow[1]
        expected = _parse_expected_value(raw_value)
        last_actual: object = None
        matched = False
        for action in actions:
            try:
                actual = _get_by_path(action, path)
            except KeyError:
                continue
            last_actual = actual
            if actual == expected:
                matched = True
                break
        assert matched, (
            f"field {path!r}: expected {expected!r}, no action matched (last seen {last_actual!r} over {len(actions)} actions)"
        )


@then(parsers.parse("delta log commit {filename} in {location_var} has no action with sub-field {field} set"))
def delta_log_commit_no_action_with_subfield_set(
    filename: str,
    location_var: str,
    field: str,
    variables: dict,
) -> None:
    """Assert no NDJSON action has the given dot-path set to a non-null value.

    ``field`` is a dotted path such as ``add.baseRowId``.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    actions = _load_delta_log_actions(Path(location.path), filename)
    parts = field.split(".")
    for action in actions:
        cursor: object = action
        found = True
        for part in parts:
            if isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                found = False
                break
        if found and cursor is not None:
            msg = f"expected no action to carry {field!r} set; found value {cursor!r} in action {action!r}"
            raise AssertionError(msg)


@then(parsers.parse("delta log commit {filename} in {location_var} has rowTracking high-water-mark {hwm:d}"))
def delta_log_commit_row_tracking_hwm(
    filename: str,
    location_var: str,
    hwm: int,
    variables: dict,
) -> None:
    """Assert an NDJSON commit contains a ``delta.rowTracking`` domainMetadata action whose
    (JSON-encoded) ``configuration.rowIdHighWaterMark`` equals the given integer.
    """
    if is_jvm_spark():
        pytest.skip("Delta log assertions are Sail-only")

    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    actions = _load_delta_log_actions(Path(location.path), filename)
    for action in actions:
        dm = action.get("domainMetadata") if isinstance(action, dict) else None
        if not isinstance(dm, dict):
            continue
        if dm.get("domain") != "delta.rowTracking":
            continue
        cfg = dm.get("configuration")
        if isinstance(cfg, str):
            try:
                cfg_obj = json.loads(cfg)
            except json.JSONDecodeError as e:
                msg = f"domainMetadata.configuration is not valid JSON: {cfg!r}"
                raise AssertionError(msg) from e
        else:
            cfg_obj = cfg
        actual = cfg_obj.get("rowIdHighWaterMark") if isinstance(cfg_obj, dict) else None
        assert actual == hwm, f"rowIdHighWaterMark: expected {hwm!r}, got {actual!r} (configuration={cfg!r})"
        return
    msg = f"no domainMetadata action with domain 'delta.rowTracking' found in {filename}"
    raise AssertionError(msg)
