import pyarrow as pa
import pytest
from pydantic_core import to_json
from pyiceberg.avro.file import AvroOutputFile
from pyiceberg.manifest import (
    DATA_FILE_TYPE,
    DEFAULT_READ_VERSION,
    MANIFEST_ENTRY_SCHEMAS,
    data_file_with_partition,
    manifest_entry_schema_with_data_file,
)
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


def _set_record_field(record, struct_type, field_name: str, value) -> None:
    field_index = next(index for index, field in enumerate(struct_type.fields) if field.name == field_name)
    record._data[field_index] = value  # noqa: SLF001


def _rewrite_current_manifest(
    table,
    *,
    entry_change: tuple[str, object] | None = None,
    data_file_change: tuple[str, object] | None = None,
    metadata_changes: dict[str, str | None] | None = None,
) -> None:
    manifest = table.current_snapshot().manifests(table.io)[0]
    entry = manifest.fetch_manifest_entry(table.io, discard_deleted=False)[0]
    if entry_change is not None:
        _set_record_field(entry, MANIFEST_ENTRY_SCHEMAS[2].as_struct(), *entry_change)
    if data_file_change is not None:
        _set_record_field(entry.data_file, DATA_FILE_TYPE[2], *data_file_change)

    partition_type = table.spec().partition_type(table.schema())
    data_file_type = data_file_with_partition(
        partition_type=partition_type,
        format_version=2,
    )
    manifest_schema = manifest_entry_schema_with_data_file(2, data_file_type)
    read_schema = manifest_entry_schema_with_data_file(
        DEFAULT_READ_VERSION,
        data_file_type,
    )
    metadata = {
        "schema": table.schema().model_dump_json(),
        "schema-id": str(table.schema().schema_id),
        "partition-spec": to_json(table.spec().fields).decode(),
        "partition-spec-id": str(table.spec().spec_id),
        "format-version": "2",
        "content": "data",
    }
    for key, value in (metadata_changes or {}).items():
        if value is None:
            metadata.pop(key, None)
        else:
            metadata[key] = value

    with AvroOutputFile(
        output_file=table.io.new_output(manifest.manifest_path),
        file_schema=manifest_schema,
        record_schema=read_schema,
        schema_name="manifest_entry",
        metadata=metadata,
    ) as writer:
        writer.write_block([entry])


@pytest.fixture
def manifest_table(sql_catalog):
    identifier = "default.manifest_validation"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(NestedField(1, "id", LongType(), required=False)),
    )
    table.append(pa.table({"id": pa.array([1], type=pa.int64())}))
    try:
        yield table
    finally:
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize(
    ("record", "field_name", "invalid_value", "message"),
    [
        (
            "data-file",
            "record_count",
            -1,
            r"(?i)(record_count.*-1|-1.*record_count)",
        ),
        (
            "data-file",
            "file_size_in_bytes",
            -1,
            r"(?i)(file_size_in_bytes.*-1|-1.*file_size_in_bytes)",
        ),
        ("data-file", "content", 99, r"(?i)(content.*99|99.*content)"),
        ("data-file", "file_format", "CSV", r"(?i)(file_format.*CSV|CSV.*file_format)"),
        ("entry", "status", 99, r"(?i)(status.*99|99.*status)"),
    ],
    ids=[
        "negative-record-count",
        "negative-file-size",
        "invalid-content",
        "invalid-file-format",
        "invalid-entry-status",
    ],
)
def test_invalid_manifest_entries_are_rejected(
    spark,
    manifest_table,
    record,
    field_name,
    invalid_value,
    message,
):
    changes = {f"{record.replace('-', '_')}_change": (field_name, invalid_value)}
    _rewrite_current_manifest(manifest_table, **changes)

    with pytest.raises(Exception, match=message):
        spark.read.format("iceberg").load(manifest_table.location()).collect()


@pytest.mark.parametrize(
    ("key", "invalid_value", "message"),
    [
        ("schema-id", "not-an-id", r"(?i)schema.?id"),
        ("partition-spec-id", "not-an-id", r"(?i)partition.?spec.?id"),
        ("format-version", "9", r"(?i)format.?version"),
        ("content", "unknown", r"(?i)content"),
    ],
)
def test_malformed_manifest_headers_are_rejected(
    spark,
    manifest_table,
    key,
    invalid_value,
    message,
):
    _rewrite_current_manifest(
        manifest_table,
        metadata_changes={key: invalid_value},
    )

    with pytest.raises(Exception, match=message):
        spark.read.format("iceberg").load(manifest_table.location()).collect()


def test_missing_legacy_manifest_headers_use_v1_defaults(spark, manifest_table):
    _rewrite_current_manifest(
        manifest_table,
        metadata_changes={
            "schema-id": None,
            "partition-spec-id": None,
            "format-version": None,
            "content": None,
        },
    )

    rows = spark.read.format("iceberg").load(manifest_table.location()).collect()
    assert [tuple(row) for row in rows] == [(1,)]
