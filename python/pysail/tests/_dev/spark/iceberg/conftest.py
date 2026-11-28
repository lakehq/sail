# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pyiceberg.avro.decoder import StreamingBinaryDecoder
from pyiceberg.avro.file import AvroFile, AvroOutputFile
from pyiceberg.avro.resolver import construct_writer, resolve_reader
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import Record
from pyiceberg.types import BinaryType, FixedType, NestedField, StructType
from pytest_bdd import given, then

from pysail.testing._dev.spark.steps.iceberg import _current_snapshot, _find_latest_metadata, _write_metadata
from pysail.tests._dev.spark.iceberg.utils import _local_file_path, create_sql_catalog


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    root = Path(__file__).parent
    for item in items:
        if item.path.is_relative_to(root):
            item.add_marker(pytest.mark.spark(skip_if_jvm=True, reason="Tests Sail's Iceberg integration"))


@pytest.fixture
def sql_catalog(tmp_path):
    return create_sql_catalog(tmp_path)


@pytest.fixture
def iceberg_table(sql_catalog, request) -> Table:
    params = request.param
    schema: Schema = params["schema"]
    identifier: str = params["identifier"]
    partition_spec = params.get("partition_spec")
    table = sql_catalog.create_table(identifier=identifier, schema=schema, partition_spec=partition_spec)
    try:
        yield table
    finally:
        sql_catalog.drop_table(identifier)


def _partition_stats_physical_schema(schema):
    # Iceberg Java writes FIXED partition values as length-prefixed ByteBuffers.
    # UUID and DECIMAL also declare fixed Avro types but use fixed-width encoding.
    partition, *metrics = schema.fields
    fields = [
        field.model_copy(update={"field_type": BinaryType()}) if isinstance(field.field_type, FixedType) else field
        for field in partition.field_type.fields
    ]
    return Schema(partition.model_copy(update={"field_type": StructType(*fields)}), *metrics)


@given("an Iceberg table with two appends and Avro partition statistics for the first snapshot")
def table_with_partition_statistics(sql_catalog, variables):
    # Matches PartitionStatsHandlerTestBase.testAllDatatypePartitionWriting in Apache Iceberg 1.12.0.
    columns = [
        ("b", "boolean", True),
        ("i", "int", 42),
        ("l", "long", 42),
        ("f", "float", 3.14),
        ("d", "double", 3.1415926),
        ("date", "date", 18993),
        ("ts", "timestamp", 1512123175038194),
        ("s", "string", "string"),
        ("uuid", "uuid", UUID("00112233-4455-6677-8899-aabbccddeeff")),
        ("fixed", "fixed[7]", bytes(range(7))),
        ("bytes", "binary", bytes([1, 2, 3])),
        ("dec_9_0", "decimal(9, 0)", Decimal(123456789)),
        ("dec_11_2", "decimal(11, 2)", Decimal("1234567.89")),
        ("dec_38_10", "decimal(38, 10)", Decimal("12345678901234567890.1234567890")),
        ("time", "time", 36610000000),
    ]
    schema = Schema(*(NestedField(i, name, typ) for i, (name, typ, _) in enumerate(columns, 1)))
    spec = PartitionSpec(
        *(PartitionField(f.field_id, 1000 + i, "identity", f.name) for i, f in enumerate(schema.fields))
    )
    path = Path(variables["location"].path)
    table = sql_catalog.create_table(
        "default.typed_stats",
        schema,
        location=path.as_uri(),
        partition_spec=spec,
        properties={"format-version": "2", "write.format.default": "avro"},
    )
    partition = Record(*(value for _, _, value in columns))
    for count in (1, 2):
        file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=(path / f"part-{count}.parquet").as_uri(),
            file_format=FileFormat.PARQUET,
            partition=partition,
            record_count=count,
            file_size_in_bytes=15,
            spec_id=0,
        )
        with table.transaction() as tx, tx.update_snapshot().fast_append() as append:
            append.append_data_file(file)

    parent, _ = table.snapshots()
    metrics = [
        ("spec_id", "int"),
        ("data_record_count", "long"),
        ("data_file_count", "int"),
        ("total_data_file_size_in_bytes", "long"),
        ("position_delete_record_count", "long"),
        ("position_delete_file_count", "int"),
        ("equality_delete_record_count", "long"),
        ("equality_delete_file_count", "int"),
        ("total_record_count", "long"),
        ("last_updated_at", "long"),
        ("last_updated_snapshot_id", "long"),
    ]
    stats_schema = Schema(
        NestedField(1, "partition", table.spec().partition_type(table.schema()), required=True),
        *(NestedField(i, name, typ, required=i <= 5) for i, (name, typ) in enumerate(metrics, 2)),
    )
    stats = path / "metadata" / "parent-stats.avro"
    with AvroOutputFile(table.io.new_output(stats.as_uri()), stats_schema, "table") as writer:
        writer.writer = construct_writer(_partition_stats_physical_schema(stats_schema))
        writer.write_block(
            [Record(partition, 0, 1, 1, 15, None, None, None, None, None, parent.timestamp_ms, parent.snapshot_id)]
        )
    metadata = _find_latest_metadata(path)
    metadata["partition-statistics"] = [
        {
            "snapshot-id": parent.snapshot_id,
            "statistics-path": stats.as_uri(),
            "file-size-in-bytes": stats.stat().st_size,
        }
    ]
    _write_metadata(path, metadata)
    # Force the incremental path. A full scan would require the parent manifest.
    _local_file_path(parent.manifests(table.io)[0].manifest_path).unlink()


@then("the registered Avro partition statistics match the snapshot")
def partition_statistics_match_snapshot(query, spark, variables, snapshot, monkeypatch):
    (result,) = spark.sql(query).collect()
    metadata = _find_latest_metadata(Path(variables["location"].path))
    current = _current_snapshot(metadata)
    stats = next(f for f in metadata["partition-statistics"] if f["snapshot-id"] == current["snapshot-id"])
    assert result.partition_statistics_file == stats["statistics-path"]
    path = _local_file_path(stats["statistics-path"])
    assert path.stat().st_size == stats["file-size-in-bytes"]
    # PyIceberg 0.11's Cython decoder narrows doubles to f32.
    monkeypatch.setattr("pyiceberg.avro.file.new_decoder", StreamingBinaryDecoder)
    with AvroFile(PyArrowFileIO().new_input(path.as_uri())) as reader:
        schema = reader.schema
        physical = _partition_stats_physical_schema(schema)
        reader.reader = resolve_reader(physical, physical)
        (row,) = reader
    partition, *metrics = schema.fields
    values = {}
    for field, value in zip(partition.field_type.fields, row[0], strict=True):
        if isinstance(value, bytes):
            display_value = value.hex()
        elif isinstance(value, (UUID, Decimal)):
            display_value = str(value)
        else:
            display_value = value
        values[field.name] = {"id": field.field_id, "type": str(field.field_type), "value": display_value}
    counts = {field.name: row[i] for i, field in enumerate(metrics, 1)}
    assert counts.pop("last_updated_snapshot_id") == current["snapshot-id"]
    assert counts.pop("last_updated_at") == current["timestamp-ms"]
    assert snapshot == {"partition": values, "statistics": counts}


@then("the upgraded partition statistics match the snapshot")
def upgraded_partition_statistics_match_snapshot(query, spark, snapshot):
    (result,) = spark.sql(query).collect()
    stats = pq.read_table(_local_file_path(result.partition_statistics_file))
    (row,) = stats.to_pylist()
    assert snapshot == {
        field.name: {"required": not field.nullable, "value": row[field.name]}
        for field in stats.schema
        if field.name in {"data_record_count", "dv_count", "total_record_count"} or "_delete_" in field.name
    }
