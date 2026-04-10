"""Iceberg metadata inspection steps for BDD tests."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import Row
from pytest_bdd import given, parsers, then

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


# Normalize temp paths created by pytest (`.../pytest-of-*/pytest-<run>/<case>/...`)
_PYTEST_TMP_PREFIX = re.compile(
    r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
    r"(?:[^ \t\r\n\),\]/]+/)*"
    r"pytest-of-[^/]+/pytest-\d+/[^/]+/",
    re.IGNORECASE,
)


def _normalize_pytest_tmp_path(value: str) -> str:
    value = value.replace("\\", "/")
    return _PYTEST_TMP_PREFIX.sub("<tmp>/", value)


def _find_latest_metadata(table_location: Path) -> dict:
    """Find and parse the latest metadata.json file in an Iceberg table."""
    metadata_dir = table_location / "metadata"
    if not metadata_dir.exists():
        msg = f"metadata directory not found: {metadata_dir}"
        raise AssertionError(msg)

    # Find all metadata files (*.metadata.json)
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    if not metadata_files:
        msg = f"no metadata files found in {metadata_dir}"
        raise AssertionError(msg)

    latest = metadata_files[-1]
    with latest.open("r", encoding="utf-8") as f:
        return json.load(f)


def _latest_metadata_path(table_location: Path) -> Path:
    metadata_dir = table_location / "metadata"
    if not metadata_dir.exists():
        msg = f"metadata directory not found: {metadata_dir}"
        raise AssertionError(msg)

    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    if not metadata_files:
        msg = f"no metadata files found in {metadata_dir}"
        raise AssertionError(msg)
    return metadata_files[-1]


def _ordered_snapshots(metadata: dict) -> list[dict]:
    snapshots_by_id = {
        snapshot["snapshot-id"]: snapshot for snapshot in metadata.get("snapshots", []) if "snapshot-id" in snapshot
    }
    ordered = []
    for entry in metadata.get("snapshot-log", []):
        snapshot = snapshots_by_id.get(entry.get("snapshot-id"))
        if snapshot is not None:
            ordered.append(snapshot)
    if ordered:
        return ordered
    return sorted(
        snapshots_by_id.values(),
        key=lambda snapshot: (snapshot.get("timestamp-ms", 0), snapshot["snapshot-id"]),
    )


def _write_metadata(table_location: Path, metadata: dict) -> None:
    path = _latest_metadata_path(table_location)
    with path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, separators=(",", ":"))


def _find_latest_snapshot(table_location: Path) -> dict | None:
    """Find the latest snapshot from an Iceberg table's metadata."""
    metadata = _find_latest_metadata(table_location)
    current_snapshot_id = metadata.get("current-snapshot-id")
    if current_snapshot_id is None:
        return None

    snapshots = metadata.get("snapshots", [])
    for snapshot in snapshots:
        if snapshot.get("snapshot-id") == current_snapshot_id:
            return snapshot

    msg = f"current snapshot {current_snapshot_id} not found in snapshots list"
    raise AssertionError(msg)


@given(
    parsers.parse("variable {name} for iceberg snapshot ids in {location_var}"),
    target_fixture="variables",
)
def variable_for_iceberg_snapshot_ids(name: str, location_var: str, variables: dict) -> dict:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    metadata = _find_latest_metadata(Path(location.path))
    variables[name] = [snapshot["snapshot-id"] for snapshot in _ordered_snapshots(metadata)]
    return variables


@given(
    parsers.parse(
        "iceberg snapshot timestamps in {location_var} are rewritten to consecutive seconds starting {seconds_ago:d} seconds ago"
    )
)
def rewrite_iceberg_snapshot_timestamps(location_var: str, seconds_ago: int, variables: dict) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)
    ordered = _ordered_snapshots(metadata)
    assert ordered, "no iceberg snapshots found"

    base_timestamp_ms = int(time.time() * 1000) - seconds_ago * 1000
    timestamp_by_id = {
        snapshot["snapshot-id"]: base_timestamp_ms + index * 1000 for index, snapshot in enumerate(ordered)
    }

    for snapshot in metadata.get("snapshots", []):
        snapshot_id = snapshot.get("snapshot-id")
        if snapshot_id in timestamp_by_id:
            snapshot["timestamp-ms"] = timestamp_by_id[snapshot_id]

    for entry in metadata.get("snapshot-log", []):
        snapshot_id = entry.get("snapshot-id")
        if snapshot_id in timestamp_by_id:
            entry["timestamp-ms"] = timestamp_by_id[snapshot_id]

    metadata["last-updated-ms"] = max(timestamp_by_id.values())
    _write_metadata(table_path, metadata)


@given(
    parsers.parse("variable {name} for iceberg snapshot timestamp strings in {location_var}"),
    target_fixture="variables",
)
def variable_for_iceberg_snapshot_timestamps(name: str, location_var: str, variables: dict) -> dict:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    metadata = _find_latest_metadata(Path(location.path))
    timestamps = []
    for snapshot in _ordered_snapshots(metadata):
        ts_ms = snapshot["timestamp-ms"]
        timestamps.append(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat())
    variables[name] = timestamps
    return variables


@given(parsers.parse("iceberg tag {tag_name} in {location_var} points to snapshot index {index:d}"))
def iceberg_tag_points_to_snapshot_index(
    tag_name: str,
    location_var: str,
    index: int,
    variables: dict,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)
    ordered = _ordered_snapshots(metadata)
    assert 0 <= index < len(ordered), f"snapshot index {index} out of range"
    snapshot_id = ordered[index]["snapshot-id"]
    metadata.setdefault("refs", {})[tag_name] = {
        "snapshot-id": snapshot_id,
        "type": "tag",
    }
    _write_metadata(table_path, metadata)


# FIXME: Remove this workaround once we get a proper solution.
@given(parsers.parse("append JSON row {row_json} to iceberg table in {location_var} with mergeSchema"))
def append_json_row_to_iceberg_table_with_merge_schema(
    row_json: str,
    location_var: str,
    variables: dict,
    spark,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    row = json.loads(row_json)
    spark.createDataFrame([Row(**row)]).write.format("iceberg").mode("append").option(
        "mergeSchema",
        "true",
    ).save(location.path.absolute().as_uri())


def _sanitize_iceberg_metadata(metadata: dict) -> dict:
    """Sanitize volatile fields in Iceberg metadata for snapshot comparison."""
    sanitized = dict(metadata)

    # Replace volatile IDs with placeholders
    if "current-snapshot-id" in sanitized:
        sanitized["current-snapshot-id"] = "<snapshot-id>"

    # Replace UUIDs with placeholders
    if "table-uuid" in sanitized:
        sanitized["table-uuid"] = "<uuid>"

    # Replace timestamps
    if "last-updated-ms" in sanitized:
        sanitized["last-updated-ms"] = "<timestamp>"

    # Sanitize location paths (replace absolute paths with relative markers)
    if "location" in sanitized:
        location = sanitized["location"]
        if isinstance(location, str):
            # `location` is a temp directory in tests (pytest run ids etc.), so only keep the table
            # directory name to make snapshots stable across runs.
            m = re.match(r"file://.*/([^/]+)/?$", location)
            if m:
                table_dir = m.group(1)
                sanitized["location"] = f"file://<root>/{table_dir}/"
            else:
                sanitized["location"] = "file://<root>/"

    # Sanitize snapshots
    if "snapshots" in sanitized:
        sanitized["snapshots"] = [_sanitize_iceberg_snapshot(s) for s in sanitized["snapshots"]]

    # Sanitize snapshot log
    if "snapshot-log" in sanitized:
        sanitized["snapshot-log"] = [
            {
                **entry,
                "timestamp-ms": "<timestamp>",
                "snapshot-id": "<snapshot-id>",
            }
            for entry in sanitized["snapshot-log"]
        ]

    # Sanitize refs (e.g. main branch points at a run-specific snapshot-id)
    if "refs" in sanitized and isinstance(sanitized["refs"], dict):
        refs = {}
        for name, ref in sanitized["refs"].items():
            if isinstance(ref, dict) and "snapshot-id" in ref:
                refs[name] = {**ref, "snapshot-id": "<snapshot-id>"}
            else:
                refs[name] = ref
        sanitized["refs"] = refs

    # Sanitize metadata log
    if "metadata-log" in sanitized:
        sanitized["metadata-log"] = [
            {
                **entry,
                "timestamp-ms": "<timestamp>",
                "metadata-file": _normalize_pytest_tmp_path(
                    re.sub(
                        r"\d+-[a-f0-9-]+\.metadata\.json",
                        "<version>-<uuid>.metadata.json",
                        entry.get("metadata-file", ""),
                    )
                ),
            }
            for entry in sanitized["metadata-log"]
        ]

    # Sanitize schemas (keep field-id but sanitize other volatile data)
    if "schemas" in sanitized:
        sanitized["schemas"] = [_sanitize_schema(s) for s in sanitized["schemas"]]

    # Sanitize partition-specs
    if "partition-specs" in sanitized:
        sanitized["partition-specs"] = [_sanitize_partition_spec(spec) for spec in sanitized["partition-specs"]]

    # Sanitize sort-orders
    if "sort-orders" in sanitized:
        sanitized["sort-orders"] = sanitized["sort-orders"]  # Keep as-is, usually stable

    return sanitized


def _sanitize_schema(schema: dict) -> dict:
    """Preserve an Iceberg schema for snapshot comparison.

    Field IDs and structure are kept intact for validation.
    """
    return dict(schema)


def _sanitize_partition_spec(spec: dict) -> dict:
    """Preserve a partition spec for snapshot comparison.

    Spec-id and fields are kept intact for validation.
    """
    return dict(spec)


def _sanitize_iceberg_snapshot(snapshot: dict) -> dict:
    """Sanitize a single Iceberg snapshot."""
    sanitized = dict(snapshot)

    # Replace IDs with placeholders
    if "snapshot-id" in sanitized:
        sanitized["snapshot-id"] = "<snapshot-id>"
    if "parent-snapshot-id" in sanitized:
        sanitized["parent-snapshot-id"] = "<parent-snapshot-id>"

    # Replace timestamps
    if "timestamp-ms" in sanitized:
        sanitized["timestamp-ms"] = "<timestamp>"

    # Replace manifest list path (keep structure but sanitize filename)
    if "manifest-list" in sanitized:
        manifest_list = sanitized["manifest-list"]
        # Extract just the filename pattern
        if isinstance(manifest_list, str):
            # Replace the actual filename with a pattern
            sanitized["manifest-list"] = re.sub(
                r"snap-\d+-\d+-[a-f0-9-]+\.avro",
                "snap-<id>-<seq>-<hash>.avro",
                manifest_list,
            )
            sanitized["manifest-list"] = re.sub(
                r"snap-\d+\.avro",
                "snap-<id>.avro",
                sanitized["manifest-list"],
            )
            # Also sanitize full paths
            sanitized["manifest-list"] = re.sub(
                r"file://.*/metadata/",
                "file://<root>/metadata/",
                sanitized["manifest-list"],
            )
            sanitized["manifest-list"] = _normalize_pytest_tmp_path(sanitized["manifest-list"])

    # Sanitize summary statistics (keep structure but normalize timing data)
    if "summary" in sanitized:
        summary = dict(sanitized["summary"])
        for key in list(summary.keys()):
            if key.endswith(("-ms", "-time-ms", "-duration-ms")):
                summary[key] = "<time-ms>"
            # Sanitize file paths in summary
            elif "path" in key.lower():
                summary[key] = re.sub(
                    r"file://(?:[^/]+)?(/.*)",
                    r"file://<root>\1",
                    str(summary[key]),
                )
        sanitized["summary"] = summary

    # Keep schema-id for schema evolution validation
    if "schema-id" in sanitized:
        pass  # Keep schema-id for validation

    return sanitized


def _sanitize_iceberg_snapshot_summary(snapshot: dict) -> dict:
    """Extract and sanitize just the summary from a snapshot."""
    if "summary" not in snapshot:
        return {}

    summary = dict(snapshot["summary"])
    for key in list(summary.keys()):
        if key.endswith(("-ms", "-time-ms", "-duration-ms")):
            summary[key] = "<time-ms>"

    return summary


@then("iceberg metadata contains current snapshot")
def check_iceberg_metadata_has_snapshot(variables):
    """Check that the Iceberg table has a current snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    assert "current-snapshot-id" in metadata, "no current-snapshot-id in metadata"
    assert metadata["current-snapshot-id"] is not None, "current-snapshot-id is null"


@then("iceberg metadata matches snapshot")
def check_iceberg_metadata_matches_snapshot(variables, snapshot: SnapshotAssertion):
    """Check that the Iceberg metadata matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)
    sanitized = _sanitize_iceberg_metadata(metadata)

    assert snapshot == sanitized


@then("iceberg current snapshot matches snapshot")
def check_iceberg_current_snapshot_matches(variables, snapshot: SnapshotAssertion):
    """Check that the current Iceberg snapshot matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg snapshot inspection"

    table_path = Path(location.path)
    current_snapshot = _find_latest_snapshot(table_path)

    assert current_snapshot is not None, "no current snapshot found"
    sanitized = _sanitize_iceberg_snapshot(current_snapshot)

    assert snapshot == sanitized


@then("iceberg current snapshot summary matches snapshot")
def check_iceberg_snapshot_summary_matches(variables, snapshot: SnapshotAssertion):
    """Check that the current snapshot's summary matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg snapshot inspection"

    table_path = Path(location.path)
    current_snapshot = _find_latest_snapshot(table_path)

    assert current_snapshot is not None, "no current snapshot found"
    summary = _sanitize_iceberg_snapshot_summary(current_snapshot)

    assert snapshot == summary


@then(parsers.parse("iceberg snapshot operation is {operation}"))
def check_iceberg_snapshot_operation(variables, operation: str):
    """Check that the current snapshot's operation matches the expected value."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg snapshot inspection"

    table_path = Path(location.path)
    current_snapshot = _find_latest_snapshot(table_path)

    assert current_snapshot is not None, "no current snapshot found"

    summary = current_snapshot.get("summary", {})
    actual_operation = summary.get("operation")

    assert actual_operation == operation, f"expected operation {operation!r}, got {actual_operation!r}"


@then(parsers.parse("iceberg snapshot count is {count:d}"))
def check_iceberg_snapshot_count(variables, count: int):
    """Check that the table has the expected number of snapshots."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg snapshot inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    snapshots = metadata.get("snapshots", [])
    actual_count = len(snapshots)

    assert actual_count == count, f"expected {count} snapshots, found {actual_count}"


@then(parsers.parse("iceberg schema contains {column_count:d} columns"))
def check_iceberg_schema_column_count(variables, column_count: int):
    """Check that the current schema has the expected number of columns."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg schema inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    current_schema_id = metadata.get("current-schema-id")
    schemas = metadata.get("schemas", [])

    current_schema = None
    for schema in schemas:
        if schema.get("schema-id") == current_schema_id:
            current_schema = schema
            break

    assert current_schema is not None, f"current schema with id {current_schema_id} not found"

    fields = current_schema.get("fields", [])
    actual_count = len(fields)

    assert actual_count == column_count, f"expected {column_count} columns, found {actual_count}"


@then("iceberg partition spec matches snapshot")
def check_iceberg_partition_spec_matches_snapshot(variables, snapshot: SnapshotAssertion):
    """Check that the partition spec matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg partition spec inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    partition_specs = metadata.get("partition-specs", [])
    default_spec_id = metadata.get("default-spec-id")

    partition_spec_info = {
        "default-spec-id": default_spec_id,
        "partition-specs": partition_specs,
    }

    assert snapshot == partition_spec_info


@then(parsers.parse("iceberg last column id is {column_id:d}"))
def check_iceberg_last_column_id(variables, column_id: int):
    """Check that the last-column-id matches the expected value."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg column id inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    last_column_id = metadata.get("last-column-id")

    assert last_column_id == column_id, f"expected last-column-id {column_id}, found {last_column_id}"


@then("iceberg schema history matches snapshot")
def check_iceberg_schema_history_matches_snapshot(variables, snapshot: SnapshotAssertion):
    """Check that the schema history matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg schema history inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)

    schemas = metadata.get("schemas", [])
    current_schema_id = metadata.get("current-schema-id")
    last_column_id = metadata.get("last-column-id")

    schema_history_info = {
        "current-schema-id": current_schema_id,
        "last-column-id": last_column_id,
        "schemas": schemas,
    }

    assert snapshot == schema_history_info
