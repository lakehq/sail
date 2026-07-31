"""Iceberg metadata inspection steps for BDD tests."""

from __future__ import annotations

import gzip
import json
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from urllib.request import url2pathname

from pyiceberg.avro.file import AvroFile
from pyiceberg.conversions import from_bytes
from pyiceberg.io.pyarrow import PyArrowFile, PyArrowFileIO
from pyiceberg.manifest import (
    DATA_FILE_TYPE,
    MANIFEST_ENTRY_SCHEMAS,
    MANIFEST_LIST_FILE_SCHEMAS,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    PartitionFieldSummary,
)
from pyiceberg.typedef import Record
from pyiceberg.types import IcebergType, PrimitiveType
from pyspark.sql import Row
from pytest_bdd import given, parsers, then

from pysail.testing.spark.steps.delta import _get_by_path, _parse_expected_value

if TYPE_CHECKING:
    from syrupy.assertion import SnapshotAssertion


# Normalize temp paths created by pytest (`.../pytest-of-*/pytest-<run>/<case>/...`)
_PYTEST_TMP_PREFIX = re.compile(
    r"(?:(?:[A-Za-z]:)?/|private/|tmp/)"
    r"(?:[^ \t\r\n\),\]/]+/)*"
    r"pytest-of-[^/]+/pytest-\d+/[^/]+/",
    re.IGNORECASE,
)

_MANIFEST_LIST_FIRST_ROW_ID_POSITION = 15
_MANIFEST_READ_VERSION = 2


def _normalize_pytest_tmp_path(value: str) -> str:
    value = value.replace("\\", "/")
    return _PYTEST_TMP_PREFIX.sub("<tmp>/", value)


def _metadata_file_stem(name: str) -> tuple[str, bool] | None:
    if name.endswith(".metadata.json.gz"):
        return name[: -len(".metadata.json.gz")], True
    if not name.endswith(".metadata.json"):
        return None

    stem = name[: -len(".metadata.json")]
    if stem.endswith(".gz"):
        return stem[: -len(".gz")], True
    return stem, False


def _metadata_file_version(path: Path) -> int | None:
    stem_and_codec = _metadata_file_stem(path.name)
    if stem_and_codec is None:
        return None

    stem, _ = stem_and_codec
    if stem.startswith("v") and stem[1:].isdigit():
        return int(stem[1:])

    version, separator, _ = stem.partition("-")
    if separator and version.isdigit():
        return int(version)
    return None


def _metadata_files(metadata_dir: Path) -> list[Path]:
    files = [path for path in metadata_dir.iterdir() if path.is_file() and _metadata_file_version(path) is not None]
    return sorted(files, key=lambda path: (_metadata_file_version(path), path.name))


def _load_metadata_file(path: Path) -> dict:
    stem_and_codec = _metadata_file_stem(path.name)
    assert stem_and_codec is not None, f"invalid metadata file name: {path.name!r}"
    _, compressed = stem_and_codec
    opener = gzip.open if compressed else Path.open
    with opener(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _write_metadata_file(path: Path, metadata: dict) -> None:
    stem_and_codec = _metadata_file_stem(path.name)
    assert stem_and_codec is not None, f"invalid metadata file name: {path.name!r}"
    _, compressed = stem_and_codec
    opener = gzip.open if compressed else Path.open
    with opener(path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f, separators=(",", ":"))


def _find_latest_metadata(table_location: Path) -> dict:
    """Find and parse the latest metadata.json file in an Iceberg table."""
    metadata_dir = table_location / "metadata"
    if not metadata_dir.exists():
        msg = f"metadata directory not found: {metadata_dir}"
        raise AssertionError(msg)

    metadata_files = _metadata_files(metadata_dir)
    if not metadata_files:
        msg = f"no metadata files found in {metadata_dir}"
        raise AssertionError(msg)

    return _load_metadata_file(metadata_files[-1])


def _latest_metadata_path(table_location: Path) -> Path:
    metadata_dir = table_location / "metadata"
    if not metadata_dir.exists():
        msg = f"metadata directory not found: {metadata_dir}"
        raise AssertionError(msg)

    metadata_files = _metadata_files(metadata_dir)
    if not metadata_files:
        msg = f"no metadata files found in {metadata_dir}"
        raise AssertionError(msg)
    return metadata_files[-1]


def _version_hint(table_location: Path) -> str:
    hint_path = table_location / "metadata" / "version-hint.text"
    if not hint_path.exists():
        msg = f"version hint not found: {hint_path}"
        raise AssertionError(msg)
    return hint_path.read_text(encoding="utf-8").strip()


def _current_snapshot(metadata: dict) -> dict:
    current_snapshot_id = metadata.get("current-snapshot-id")
    assert current_snapshot_id is not None, "no current-snapshot-id in metadata"
    for snapshot in metadata.get("snapshots", []):
        if snapshot.get("snapshot-id") == current_snapshot_id:
            return snapshot
    msg = f"current snapshot {current_snapshot_id} not found in snapshots list"
    raise AssertionError(msg)


def _pyarrow_input_file(io: PyArrowFileIO, location: str) -> PyArrowFile:
    parsed = urlparse(location)
    if parsed.scheme != "file":
        return io.new_input(location)
    path = url2pathname(f"//{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path)
    return PyArrowFile(location=location, path=path, fs=io.fs_by_scheme("file", ""))


def _current_manifest_files(metadata: dict) -> list:
    snapshot = _current_snapshot(metadata)
    manifest_list = snapshot.get("manifest-list")
    assert isinstance(manifest_list, str), f"current snapshot has no manifest-list: {snapshot!r}"
    format_version = metadata.get("format-version", 2)
    assert format_version in MANIFEST_LIST_FILE_SCHEMAS, f"unsupported manifest list version: {format_version!r}"

    io = PyArrowFileIO()
    with AvroFile(
        _pyarrow_input_file(io, manifest_list),
        MANIFEST_LIST_FILE_SCHEMAS[format_version],
        read_types={-1: ManifestFile, 508: PartitionFieldSummary},
        read_enums={517: ManifestContent},
    ) as reader:
        return list(reader)


def _current_manifest_list(metadata: dict) -> dict:
    manifests = [_manifest_record_to_dict(record) for record in _current_manifest_files(metadata)]
    return {"manifests": manifests}


def _manifest_entries(
    io: PyArrowFileIO,
    manifest: ManifestFile,
    format_version: int,
) -> list[ManifestEntry]:
    with AvroFile(
        _pyarrow_input_file(io, manifest.manifest_path),
        MANIFEST_ENTRY_SCHEMAS[format_version],
        read_types={-1: ManifestEntry, 2: DataFile},
        read_enums={
            0: ManifestEntryStatus,
            101: FileFormat,
            134: DataFileContent,
        },
    ) as reader:
        return list(reader)


def _manifest_record_to_dict(record) -> dict:
    data = record._data  # noqa: SLF001 - pyiceberg exposes manifest-list V3 fields only via Record data.
    content = data[3]
    if isinstance(content, ManifestContent):
        content = content.name.lower()
    manifest = {
        "manifest-path": data[0],
        "manifest-length": data[1],
        "partition-spec-id": data[2],
        "content": content,
        "sequence-number": data[4],
        "min-sequence-number": data[5],
        "added-snapshot-id": data[6],
        "added-files-count": data[7],
        "existing-files-count": data[8],
        "deleted-files-count": data[9],
        "added-rows-count": data[10],
        "existing-rows-count": data[11],
        "deleted-rows-count": data[12],
        "partitions": data[13],
        "key-metadata": data[14],
    }
    if len(data) > _MANIFEST_LIST_FIRST_ROW_ID_POSITION:
        manifest["first-row-id"] = data[_MANIFEST_LIST_FIRST_ROW_ID_POSITION]
    return manifest


def _sanitize_manifest_file_path(path: str) -> str:
    path = re.sub(
        r"manifest-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.avro",
        "manifest-<uuid>.avro",
        path,
    )
    path = re.sub(r"file://.*/metadata/", "file://<root>/metadata/", path)
    return _normalize_pytest_tmp_path(path)


def _sanitize_manifest_list(manifest_list: dict) -> dict:
    manifests = []
    for manifest in manifest_list.get("manifests", []):
        sanitized = dict(manifest)
        if isinstance(sanitized.get("manifest-path"), str):
            sanitized["manifest-path"] = _sanitize_manifest_file_path(sanitized["manifest-path"])
        if "manifest-length" in sanitized:
            sanitized["manifest-length"] = "<bytes>"
        if "added-snapshot-id" in sanitized:
            sanitized["added-snapshot-id"] = "<snapshot-id>"
        manifests.append(sanitized)
    return {"manifests": manifests}


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
    _write_metadata_file(path, metadata)


def _rewrite_latest_metadata_file(table_location: Path, *, uuid_prefixed: bool, compressed: bool) -> Path:
    old_path = _latest_metadata_path(table_location)
    version = _metadata_file_version(old_path)
    assert version is not None, f"cannot determine metadata version from {old_path.name!r}"
    metadata = _load_metadata_file(old_path)

    suffix = ".gz.metadata.json" if compressed else ".metadata.json"
    file_name = f"{version:05d}-{uuid.UUID(int=version)}{suffix}" if uuid_prefixed else f"v{version}{suffix}"

    new_path = old_path.parent / file_name
    _write_metadata_file(new_path, metadata)
    if new_path != old_path:
        old_path.unlink()

    hint = new_path.name if uuid_prefixed else str(version)
    (table_location / "metadata" / "version-hint.text").write_text(hint, encoding="utf-8")
    return new_path


@given("iceberg latest metadata file uses UUID-prefixed naming")
def rewrite_latest_metadata_to_uuid_prefix(variables: dict) -> None:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata rewrite"
    _rewrite_latest_metadata_file(Path(location.path), uuid_prefixed=True, compressed=False)


@given("iceberg latest metadata file uses UUID-prefixed gzip naming")
def rewrite_latest_metadata_to_uuid_prefix_gzip(variables: dict) -> None:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata rewrite"
    _rewrite_latest_metadata_file(Path(location.path), uuid_prefixed=True, compressed=True)


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


@given(parsers.parse("append query to iceberg table in {location_var} with mergeSchema"))
def append_query_to_iceberg_table_with_merge_schema(
    docstring: str,
    location_var: str,
    variables: dict,
    spark,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"
    spark.sql(docstring).write.format("iceberg").mode("append").option(
        "mergeSchema",
        "true",
    ).save(location.path.absolute().as_uri())


def _snapshot_id_labels(metadata: dict) -> dict[int, str]:
    return {
        snapshot["snapshot-id"]: f"<snapshot-{index}>"
        for index, snapshot in enumerate(_ordered_snapshots(metadata), start=1)
    }


def _snapshot_id_label(
    snapshot_id: int | None,
    labels: dict[int, str] | None,
    fallback: str,
) -> int | str | None:
    if snapshot_id is None:
        return None
    if labels is None:
        return fallback
    return labels.get(snapshot_id, "<unknown-snapshot>")


def _sanitize_iceberg_metadata(
    metadata: dict,
    snapshot_labels: dict[int, str] | None = None,
) -> dict:
    """Sanitize volatile fields in Iceberg metadata for snapshot comparison."""
    sanitized = dict(metadata)

    # Replace volatile IDs with placeholders
    if "current-snapshot-id" in sanitized:
        sanitized["current-snapshot-id"] = _snapshot_id_label(
            sanitized["current-snapshot-id"],
            snapshot_labels,
            "<snapshot-id>",
        )

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
        sanitized["snapshots"] = [
            _sanitize_iceberg_snapshot(snapshot, snapshot_labels) for snapshot in sanitized["snapshots"]
        ]

    # Sanitize snapshot log
    if "snapshot-log" in sanitized:
        sanitized["snapshot-log"] = [
            {
                **entry,
                "timestamp-ms": "<timestamp>",
                "snapshot-id": _snapshot_id_label(
                    entry.get("snapshot-id"),
                    snapshot_labels,
                    "<snapshot-id>",
                ),
            }
            for entry in sanitized["snapshot-log"]
        ]

    # Sanitize refs (e.g. main branch points at a run-specific snapshot-id)
    if "refs" in sanitized and isinstance(sanitized["refs"], dict):
        refs = {}
        for name, ref in sanitized["refs"].items():
            if isinstance(ref, dict) and "snapshot-id" in ref:
                refs[name] = {
                    **ref,
                    "snapshot-id": _snapshot_id_label(
                        ref.get("snapshot-id"),
                        snapshot_labels,
                        "<snapshot-id>",
                    ),
                }
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


def _sanitize_iceberg_snapshot(
    snapshot: dict,
    snapshot_labels: dict[int, str] | None = None,
) -> dict:
    """Sanitize a single Iceberg snapshot."""
    sanitized = dict(snapshot)

    # Replace IDs with placeholders
    if "snapshot-id" in sanitized:
        sanitized["snapshot-id"] = _snapshot_id_label(
            sanitized["snapshot-id"],
            snapshot_labels,
            "<snapshot-id>",
        )
    if "parent-snapshot-id" in sanitized:
        sanitized["parent-snapshot-id"] = _snapshot_id_label(
            sanitized["parent-snapshot-id"],
            snapshot_labels,
            "<parent-snapshot-id>",
        )

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


def _snapshot_value(value):
    if isinstance(value, Enum):
        return value.name.lower()
    if isinstance(value, bytes):
        return f"0x{value.hex()}"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Record):
        return [_snapshot_value(item) for item in value._data]  # noqa: SLF001
    if isinstance(value, dict):
        return {key: _snapshot_value(item) for key, item in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_snapshot_value(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _sanitize_data_file_path(path: str) -> str:
    sanitized = _normalize_pytest_tmp_path(path)
    sanitized = re.sub(r"file://.*/data/", "file://<root>/data/", sanitized)
    return re.sub(
        r"(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:-\d+)?"
        r"\.(parquet|avro|orc)",
        r"data-<uuid>.\1",
        sanitized,
    )


def _schema_types_by_id(metadata: dict) -> dict[int, PrimitiveType]:
    types_by_id: dict[int, PrimitiveType] = {}

    def visit_field(field: dict) -> None:
        field_type_json = field.get("type")
        try:
            field_type = IcebergType.model_validate(field_type_json)
        except Exception:  # noqa: BLE001 - PyIceberg may not yet know newer Iceberg primitive types.
            field_type = None
        if isinstance(field_type, PrimitiveType):
            types_by_id.setdefault(field["id"], field_type)
            return
        if not isinstance(field_type_json, dict):
            return
        if field_type_json.get("type") == "struct":
            for child in field_type_json.get("fields", []):
                visit_field(child)
        elif field_type_json.get("type") == "list":
            visit_field(
                {
                    "id": field_type_json["element-id"],
                    "type": field_type_json["element"],
                }
            )
        elif field_type_json.get("type") == "map":
            visit_field(
                {
                    "id": field_type_json["key-id"],
                    "type": field_type_json["key"],
                }
            )
            visit_field(
                {
                    "id": field_type_json["value-id"],
                    "type": field_type_json["value"],
                }
            )

    for schema_json in metadata.get("schemas", []):
        for field in schema_json.get("fields", []):
            visit_field(field)
    return types_by_id


def _decode_bound(
    field_id: int,
    value: bytes,
    types_by_id: dict[int, PrimitiveType],
):
    field_type = types_by_id.get(field_id)
    if field_type is None:
        return f"0x{value.hex()}"
    try:
        return _snapshot_value(from_bytes(field_type, value))
    except Exception:  # noqa: BLE001 - keep malformed historical bytes visible in the snapshot.
        return f"0x{value.hex()}"


def _metric_map(values: dict[int, int] | None) -> dict[int, int]:
    return dict(sorted((values or {}).items()))


def _bound_map(
    values: dict[int, bytes] | None,
    types_by_id: dict[int, PrimitiveType],
) -> dict[int, object]:
    return {field_id: _decode_bound(field_id, value, types_by_id) for field_id, value in sorted((values or {}).items())}


def _partition_values(data_file, partition_spec: dict) -> dict[str, object]:
    values = data_file.partition._data  # noqa: SLF001 - PyIceberg Record has no public positional iterator.
    fields = partition_spec.get("fields", [])
    assert len(values) == len(fields), (
        f"partition value count {len(values)} does not match spec field count {len(fields)}"
    )
    return {field["name"]: _snapshot_value(value) for field, value in zip(fields, values, strict=True)}


def _sanitize_manifest_entry(
    entry,
    manifest_file: ManifestFile,
    partition_spec: dict,
    snapshot_labels: dict[int, str],
    types_by_id: dict[int, PrimitiveType],
    format_version: int,
) -> dict:
    data_file = entry.data_file
    snapshot_id = entry.snapshot_id or manifest_file.added_snapshot_id
    sequence_number = entry.sequence_number
    file_sequence_number = entry.file_sequence_number
    if manifest_file.sequence_number == 0 or entry.status == ManifestEntryStatus.ADDED:
        sequence_number = sequence_number if sequence_number is not None else manifest_file.sequence_number
        file_sequence_number = (
            file_sequence_number if file_sequence_number is not None else manifest_file.sequence_number
        )
    column_sizes = dict.fromkeys(sorted((data_file.column_sizes or {}).keys()), "<bytes>")
    split_offsets = ["<offset>" for _ in (data_file.split_offsets or [])]
    sanitized = {
        "status": _snapshot_value(entry.status),
        "snapshot-id": _snapshot_id_label(
            snapshot_id,
            snapshot_labels,
            "<snapshot-id>",
        ),
        "sequence-number": sequence_number,
        "file-sequence-number": file_sequence_number,
        "data-file": {
            "content": _snapshot_value(data_file.content),
            "file-path": _sanitize_data_file_path(data_file.file_path),
            "file-format": _snapshot_value(data_file.file_format),
            "partition": _partition_values(data_file, partition_spec),
            "record-count": data_file.record_count,
            "file-size-in-bytes": "<bytes>",
            "column-sizes": column_sizes,
            "value-counts": _metric_map(data_file.value_counts),
            "null-value-counts": _metric_map(data_file.null_value_counts),
            "nan-value-counts": _metric_map(data_file.nan_value_counts),
            "lower-bounds": _bound_map(data_file.lower_bounds, types_by_id),
            "upper-bounds": _bound_map(data_file.upper_bounds, types_by_id),
            "key-metadata": _snapshot_value(data_file.key_metadata),
            "split-offsets": split_offsets,
            "equality-ids": data_file.equality_ids,
            "sort-order-id": data_file.sort_order_id,
        },
    }
    if format_version == 3:  # noqa: PLR2004
        data_file_values = dict(
            zip(
                (field.name for field in DATA_FILE_TYPE[3].fields),
                data_file._data,  # noqa: SLF001 - V3 fields have no PyIceberg accessors.
                strict=True,
            )
        )
        sanitized["data-file"].update(
            {
                "first-row-id": data_file_values["first_row_id"],
                "referenced-data-file": _sanitize_data_file_path(data_file_values["referenced_data_file"])
                if data_file_values["referenced_data_file"]
                else None,
                "content-offset": data_file_values["content_offset"],
                "content-size-in-bytes": data_file_values["content_size_in_bytes"],
            }
        )
    return sanitized


def _current_snapshot_graph(table_location: Path) -> dict:
    metadata = _find_latest_metadata(table_location)
    format_version = metadata.get("format-version", _MANIFEST_READ_VERSION)
    snapshot_labels = _snapshot_id_labels(metadata)
    types_by_id = _schema_types_by_id(metadata)
    specs_by_id = {spec["spec-id"]: spec for spec in metadata.get("partition-specs", [])}
    io = PyArrowFileIO()
    manifests = []
    manifest_files = sorted(
        _current_manifest_files(metadata),
        key=lambda manifest: (
            _snapshot_value(manifest.content),
            manifest.sequence_number,
            manifest.manifest_path,
        ),
    )
    for index, manifest_file in enumerate(manifest_files, start=1):
        manifest = _manifest_record_to_dict(manifest_file)
        manifest["manifest-path"] = f"file://<root>/metadata/manifest-{index}.avro"
        manifest["manifest-length"] = "<bytes>"
        manifest["added-snapshot-id"] = _snapshot_id_label(
            manifest.get("added-snapshot-id"),
            snapshot_labels,
            "<snapshot-id>",
        )
        manifest["key-metadata"] = _snapshot_value(manifest.get("key-metadata"))
        manifest["partitions"] = _snapshot_value(manifest.get("partitions"))

        partition_spec = specs_by_id[manifest_file.partition_spec_id]
        entries = [
            _sanitize_manifest_entry(
                entry,
                manifest_file,
                partition_spec,
                snapshot_labels,
                types_by_id,
                format_version,
            )
            for entry in _manifest_entries(io, manifest_file, format_version)
        ]
        entries.sort(key=lambda entry: entry["data-file"]["file-path"])
        manifests.append({"manifest": manifest, "entries": entries})

    return {
        "table-metadata": _sanitize_iceberg_metadata(metadata, snapshot_labels),
        "current-snapshot": _sanitize_iceberg_snapshot(
            _current_snapshot(metadata),
            snapshot_labels,
        ),
        "manifests": manifests,
    }


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


@then(parsers.parse("iceberg latest metadata file is {filename}"))
def check_iceberg_latest_metadata_file(filename: str, variables) -> None:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    actual = _latest_metadata_path(Path(location.path)).name
    assert actual == filename, f"expected latest metadata file {filename!r}, got {actual!r}"


@then(parsers.parse("iceberg version hint is {value}"))
def check_iceberg_version_hint(value: str, variables) -> None:
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    actual = _version_hint(Path(location.path))
    assert actual == value, f"expected version hint {value!r}, got {actual!r}"


@then("iceberg metadata matches snapshot")
def check_iceberg_metadata_matches_snapshot(variables, snapshot: SnapshotAssertion):
    """Check that the Iceberg metadata matches the saved snapshot."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)
    sanitized = _sanitize_iceberg_metadata(metadata)

    assert snapshot == sanitized


@then("iceberg metadata contains")
def check_iceberg_metadata_contains(variables, datatable):
    """Assert that specific fields in the latest Iceberg table metadata match expected values."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg metadata inspection"

    metadata = _find_latest_metadata(Path(location.path))

    assert datatable is not None, "expected a datatable: | path | value |"
    header, *rows = datatable
    assert header[:2] == ["path", "value"], "expected datatable header: path | value"
    for row in rows:
        if not row or not row[0].strip():
            continue
        path, raw = row[0], row[1] if len(row) > 1 else ""
        expected = _parse_expected_value(raw)
        actual = _get_by_path(metadata, path)
        assert actual == expected, f"path {path!r} expected {expected!r}, got {actual!r}"


@then("iceberg current manifest list matches snapshot")
def check_iceberg_current_manifest_list_matches_snapshot(variables, snapshot: SnapshotAssertion):
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg manifest list inspection"

    table_path = Path(location.path)
    metadata = _find_latest_metadata(table_path)
    manifest_list = _current_manifest_list(metadata)
    sanitized = _sanitize_manifest_list(manifest_list)

    assert snapshot == sanitized


@then("iceberg current snapshot graph matches snapshot")
def check_iceberg_current_snapshot_graph_matches(variables, snapshot: SnapshotAssertion):
    """Check table metadata, the current manifest list, and every manifest entry."""
    location = variables.get("location")
    assert location is not None, "expected variable `location` to be defined for iceberg snapshot inspection"

    graph = _current_snapshot_graph(Path(location.path))
    assert snapshot == graph


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
