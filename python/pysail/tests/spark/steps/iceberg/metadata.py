"""Iceberg metadata inspection steps for BDD tests."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

from pytest_bdd import parsers, then

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


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

    # Replace UUIDs with placeholders
    if "table-uuid" in sanitized:
        sanitized["table-uuid"] = "<uuid>"

    # Replace timestamps
    if "last-updated-ms" in sanitized:
        sanitized["last-updated-ms"] = "<timestamp>"

    # Sanitize snapshots
    if "snapshots" in sanitized:
        sanitized["snapshots"] = [_sanitize_iceberg_snapshot(s) for s in sanitized["snapshots"]]

    # Sanitize snapshot log
    if "snapshot-log" in sanitized:
        sanitized["snapshot-log"] = [{**entry, "timestamp-ms": "<timestamp>"} for entry in sanitized["snapshot-log"]]

    # Sanitize metadata log
    if "metadata-log" in sanitized:
        sanitized["metadata-log"] = [{**entry, "timestamp-ms": "<timestamp>"} for entry in sanitized["metadata-log"]]

    return sanitized


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

    # Sanitize summary statistics (keep structure but normalize timing data)
    if "summary" in sanitized:
        summary = dict(sanitized["summary"])
        for key in list(summary.keys()):
            if key.endswith(("-ms", "-time-ms", "-duration-ms")):
                summary[key] = "<time-ms>"
        sanitized["summary"] = summary

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
