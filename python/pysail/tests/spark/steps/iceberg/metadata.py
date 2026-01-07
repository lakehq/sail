"""Iceberg metadata inspection steps for BDD tests."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from pytest_bdd import parsers, then

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


# Normalize temp paths created by pytest (`.../pytest-of-*/pytest-<run>/<case>/...`)
_PYTEST_TMP_PREFIX = re.compile(
    r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
    r"(?:[^ \t\r\n\),\]]+/)*"
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
            m = re.match(r"file://(?:[^/]+)?(/.*)", location)
            if m:
                path = m.group(1).rstrip("/")
                table_dir = path.split("/")[-1] if path else ""
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
    """Sanitize an Iceberg schema, preserving field IDs and structure."""
    # schema-id is usually stable across same schema versions, keep it
    # fields structure should be preserved with field-id intact
    return dict(schema)


def _sanitize_partition_spec(spec: dict) -> dict:
    """Sanitize a partition spec, preserving spec-id and field structure."""
    # spec-id and fields should be preserved for validation
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
                r"file://(?:[^/]+)?(/.*)/metadata/",
                r"file://<root>/metadata/",
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
