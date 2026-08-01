import json
from datetime import date
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pytest
from pyiceberg.manifest import DATA_FILE_TYPE, DataFile, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform, TruncateTransform, VoidTransform
from pyiceberg.types import (
    BinaryType,
    DateType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from pysail.tests.spark.iceberg.utils import pyiceberg_file_io_properties


def _static_table(location: str) -> StaticTable:
    return StaticTable.from_metadata(location, properties=pyiceberg_file_io_properties())


def _copy_data_file(data_file: DataFile, **changes) -> DataFile:
    field_names = [field.name for field in DATA_FILE_TYPE[2].fields]
    values = dict(zip(field_names, data_file._data, strict=True))  # noqa: SLF001
    values.update(changes)
    return DataFile.from_args(**values)


def _append_data_files(table, data_files: list[DataFile]) -> None:
    with (
        table.transaction() as transaction,
        transaction._append_snapshot_producer({}) as append_files,  # noqa: SLF001
    ):
        for data_file in data_files:
            append_files.append_data_file(data_file)


def _promote_metadata_to_v3(table, field_defaults: dict[int, dict[str, object]]) -> None:
    metadata_path = Path(unquote(urlparse(table.metadata_location).path))
    metadata = json.loads(metadata_path.read_text())
    metadata["format-version"] = 3
    current_schema = next(
        schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"]
    )

    def update_type(field_type) -> None:
        if not isinstance(field_type, dict):
            return
        if field_type["type"] == "struct":
            update_fields(field_type["fields"])
        elif field_type["type"] == "list":
            update_type(field_type["element"])
        elif field_type["type"] == "map":
            update_type(field_type["key"])
            update_type(field_type["value"])

    def update_fields(fields) -> None:
        for field in fields:
            field.update(field_defaults.get(field["id"], {}))
            update_type(field["type"])

    update_fields(current_schema["fields"])
    metadata_path.write_text(json.dumps(metadata, separators=(",", ":")))


def test_field_id_reads_defaults_statistics_and_filter_projection(spark, sql_catalog):
    identifier = "default.field_id_correctness"
    schema = Schema(
        NestedField(1, "part", LongType(), required=True),
        NestedField(2, "old_value", StringType(), required=True),
        NestedField(
            3,
            "payload",
            StructType(NestedField(4, "old_nested", IntegerType(), required=True)),
            required=True,
        ),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=IdentityTransform(),
            name="part",
        )
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema, partition_spec=spec)
    try:
        payload_type = pa.struct([pa.field("old_nested", pa.int32(), nullable=False)])
        table.append(
            pa.Table.from_arrays(
                [
                    pa.array([10], type=pa.int64()),
                    pa.array(["kept"], type=pa.string()),
                    pa.array([{"old_nested": 11}], type=payload_type),
                ],
                schema=pa.schema(
                    [
                        pa.field("part", pa.int64(), nullable=False),
                        pa.field("old_value", pa.string(), nullable=False),
                        pa.field("payload", payload_type, nullable=False),
                    ]
                ),
            )
        )
        with table.update_schema() as update:
            update.rename_column("old_value", "renamed_value")
            update.rename_column(("payload", "old_nested"), "renamed_nested")
            update.add_column("added", IntegerType(), required=True, default_value=42)
            update.add_column(
                ("payload", "added_nested"),
                IntegerType(),
                required=True,
                default_value=7,
            )
        _promote_metadata_to_v3(
            table,
            {
                table.schema().find_field("added").field_id: {"initial-default": 42},
                table.schema().find_field("payload.added_nested").field_id: {"initial-default": 7},
            },
        )

        result = (
            spark.read.format("iceberg")
            .load(table.location())
            .where("part = 10 AND renamed_value = 'kept'")
            .selectExpr(
                "renamed_value",
                "added",
                "payload.renamed_nested AS renamed_nested",
                "payload.added_nested AS added_nested",
            )
        )

        assert result.columns == [
            "renamed_value",
            "added",
            "renamed_nested",
            "added_nested",
        ]
        assert [tuple(row) for row in result.collect()] == [("kept", 42, 11, 7)]
    finally:
        sql_catalog.drop_table(identifier)


def test_write_defaults_apply_recursively_to_struct_list_and_map(spark, sql_catalog):
    identifier = "default.recursive_write_defaults"
    value_struct = StructType(
        NestedField(14, "existing", IntegerType(), required=False),
        NestedField(
            15,
            "filled",
            IntegerType(),
            required=True,
            initial_default=15,
            write_default=150,
        ),
    )
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "initial_only",
            StringType(),
            required=False,
            initial_default="legacy",
        ),
        NestedField(
            3,
            "status",
            StringType(),
            required=True,
            initial_default="legacy",
            write_default="new",
        ),
        NestedField(
            4,
            "payload",
            StructType(
                NestedField(5, "existing", IntegerType(), required=False),
                NestedField(
                    6,
                    "filled",
                    IntegerType(),
                    required=True,
                    initial_default=6,
                    write_default=60,
                ),
            ),
            required=True,
        ),
        NestedField(
            7,
            "items",
            ListType(
                element_id=8,
                element=StructType(
                    NestedField(9, "existing", IntegerType(), required=False),
                    NestedField(
                        10,
                        "filled",
                        IntegerType(),
                        required=True,
                        initial_default=10,
                        write_default=100,
                    ),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            11,
            "attributes",
            MapType(
                key_id=12,
                key_type=StringType(),
                value_id=13,
                value_type=value_struct,
                value_required=True,
            ),
            required=True,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        table_schema = table.schema()
        _promote_metadata_to_v3(
            table,
            {
                table_schema.find_field("initial_only").field_id: {"initial-default": "legacy"},
                table_schema.find_field("status").field_id: {
                    "initial-default": "legacy",
                    "write-default": "new",
                },
                table_schema.find_field("payload.filled").field_id: {
                    "initial-default": 6,
                    "write-default": 60,
                },
                table_schema.find_field("items.element.filled").field_id: {
                    "initial-default": 10,
                    "write-default": 100,
                },
                table_schema.find_field("attributes.value.filled").field_id: {
                    "initial-default": 15,
                    "write-default": 150,
                },
            },
        )
        source = spark.createDataFrame(
            [(1, (5,), [(9,)], {"a": (14,)})],
            schema=(
                "id LONG, "
                "payload STRUCT<existing: INT>, "
                "items ARRAY<STRUCT<existing: INT>>, "
                "attributes MAP<STRING, STRUCT<existing: INT>>"
            ),
        )
        source.write.format("iceberg").mode("append").save(table.location())

        row = spark.read.format("iceberg").load(table.location()).collect()[0]
        assert row.initial_only is None
        assert row.status == "new"
        assert tuple(row.payload) == (5, 60)
        assert [tuple(item) for item in row.items] == [(9, 100)]
        assert {key: tuple(value) for key, value in row.attributes.items()} == {"a": (14, 150)}
    finally:
        sql_catalog.drop_table(identifier)


def test_overwrite_schema_preserves_compatible_identity_and_replaces_incompatible_identity(
    spark,
    sql_catalog,
):
    identifier = "default.overwrite_schema_identity"
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(
            2,
            "value",
            IntegerType(),
            required=True,
            doc="stable meaning",
            initial_default=23,
            write_default=34,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        _promote_metadata_to_v3(
            table,
            {2: {"initial-default": 23, "write-default": 34}},
        )
        spark.createDataFrame([(1, 100)], "id LONG, value INT").write.format("iceberg").mode("overwrite").save(
            table.location()
        )

        spark.createDataFrame([(2, 200)], "id LONG, value LONG").write.format("iceberg").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(table.location())

        promoted = _static_table(table.location()).schema().find_field("value")
        assert promoted.field_id == 2  # noqa: PLR2004
        assert isinstance(promoted.field_type, LongType)
        assert promoted.required is False
        assert promoted.doc == "stable meaning"
        assert promoted.initial_default == 23  # noqa: PLR2004
        assert promoted.write_default == 34  # noqa: PLR2004

        spark.createDataFrame([(3, "three")], "id LONG, value STRING").write.format("iceberg").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(table.location())

        replaced = _static_table(table.location()).schema().find_field("value")
        assert replaced.field_id != 2  # noqa: PLR2004
        assert isinstance(replaced.field_type, StringType)
    finally:
        sql_catalog.drop_table(identifier)


def test_missing_null_count_keeps_a_file_eligible_for_is_null(spark, sql_catalog):
    source_identifier = "default.missing_null_count_source"
    target_identifier = "default.missing_null_count_target"
    schema = Schema(NestedField(1, "value", StringType(), required=False))
    source = sql_catalog.create_table(identifier=source_identifier, schema=schema)
    target = sql_catalog.create_table(identifier=target_identifier, schema=schema)
    try:
        source.append(pa.table({"value": pa.array([None, "present"], type=pa.string())}))
        source_file = source.scan().plan_files()[0].file
        _append_data_files(
            target,
            [_copy_data_file(source_file, null_value_counts=None)],
        )

        rows = spark.read.format("iceberg").load(target.location()).where("value IS NULL").collect()
        assert len(rows) == 1
        assert rows[0].value is None
    finally:
        sql_catalog.drop_table(target_identifier)
        sql_catalog.drop_table(source_identifier)


def test_logical_date_bounds_and_ranged_equality_do_not_prune_matching_rows(
    spark,
    sql_catalog,
):
    identifier = "default.logical_bounds"
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "event_date", DateType(), required=True),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        table.append(
            pa.Table.from_arrays(
                [
                    pa.array([1, 2], type=pa.int64()),
                    pa.array(
                        [date(2024, 1, 1), date(2024, 1, 2)],
                        type=pa.date32(),
                    ),
                ],
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("event_date", pa.date32(), nullable=False),
                    ]
                ),
            )
        )

        rows = (
            spark.read.format("iceberg")
            .load(table.location())
            .where("id = 1 AND event_date = DATE '2024-01-01'")
            .collect()
        )
        assert [tuple(row) for row in rows] == [(1, date(2024, 1, 1))]
    finally:
        sql_catalog.drop_table(identifier)


def test_non_parquet_manifest_entry_fails_before_parquet_scan(spark, sql_catalog):
    source_identifier = "default.unsupported_format_source"
    target_identifier = "default.unsupported_format_target"
    schema = Schema(NestedField(1, "id", LongType(), required=True))
    source = sql_catalog.create_table(identifier=source_identifier, schema=schema)
    target = sql_catalog.create_table(identifier=target_identifier, schema=schema)
    try:
        source.append(
            pa.Table.from_arrays(
                [pa.array([1], type=pa.int64())],
                schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            )
        )
        source_file = source.scan().plan_files()[0].file
        _append_data_files(
            target,
            [_copy_data_file(source_file, file_format=FileFormat.AVRO)],
        )

        with pytest.raises(
            Exception,
            match=r"(?i)Avro.*(?:unsupported|not supported|only Parquet.*supported)",
        ):
            spark.read.format("iceberg").load(target.location()).collect()
    finally:
        sql_catalog.drop_table(target_identifier)
        sql_catalog.drop_table(source_identifier)


def test_void_partition_writes_null_values(spark, sql_catalog):
    identifier = "default.void_partition"
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=VoidTransform(),
            name="id_void",
        )
    )
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
    )
    try:
        spark.createDataFrame([(1,), (2,)], "id LONG").write.format("iceberg").mode("append").save(table.location())

        static_table = _static_table(table.location())
        tasks = static_table.scan().plan_files()
        assert len(tasks) == 1
        assert tasks[0].file.partition[0] is None
        assert "/id_void=null/" in tasks[0].file.file_path
        assert [tuple(row) for row in spark.read.format("iceberg").load(table.location()).orderBy("id").collect()] == [
            (1,),
            (2,),
        ]
    finally:
        sql_catalog.drop_table(identifier)


def test_binary_truncate_writes_prefix_partitions(spark, sql_catalog):
    identifier = "default.binary_truncate"
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "payload", BinaryType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=TruncateTransform(2),
            name="payload_trunc",
        )
    )
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
    )
    try:
        spark.createDataFrame(
            [
                (1, bytearray(b"\x01\x02\x03")),
                (2, bytearray(b"\x01\x02\xff")),
                (3, bytearray(b"\x03\x04\x05")),
            ],
            "id LONG, payload BINARY",
        ).write.format("iceberg").mode("append").save(table.location())

        partitions = {task.file.partition[0] for task in _static_table(table.location()).scan().plan_files()}
        assert partitions == {b"\x01\x02", b"\x03\x04"}
    finally:
        sql_catalog.drop_table(identifier)


def test_unknown_partition_transform_fails_before_data_write(spark, sql_catalog):
    identifier = "default.unknown_partition"
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=IdentityTransform(),
            name="id",
        )
    )
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
    )
    try:
        metadata_path = Path(unquote(urlparse(table.metadata_location).path))
        metadata = json.loads(metadata_path.read_text())
        metadata["partition-specs"][0]["fields"][0]["transform"] = "future-transform"
        metadata_path.write_text(json.dumps(metadata, separators=(",", ":")))

        with pytest.raises(Exception, match=r"(?i)(unknown|unsupported).*transform"):
            spark.createDataFrame([(1,)], "id LONG").write.format("iceberg").mode("append").save(table.location())

        table_path = Path(unquote(urlparse(table.location()).path))
        assert not list(table_path.glob("data/**/*.parquet"))
        assert json.loads(metadata_path.read_text())["snapshots"] == []
    finally:
        sql_catalog.drop_table(identifier)


def test_v1_partition_replacement_keeps_removed_fields_as_void(spark, sql_catalog):
    identifier = "default.v1_partition_replacement"
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "category", StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=IdentityTransform(),
            name="category",
        )
    )
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
        properties={"format-version": "1"},
    )
    try:
        spark.createDataFrame(
            [(1, "A", "east"), (2, "B", "west")],
            "id LONG, category STRING, region STRING",
        ).write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").partitionBy("region").save(
            table.location()
        )

        table_path = Path(unquote(urlparse(table.location()).path))
        metadata_path = sorted(table_path.joinpath("metadata").glob("*.metadata.json"))[-1]
        metadata = json.loads(metadata_path.read_text())
        assert metadata["format-version"] == 1
        assert metadata["partition-specs"][0]["fields"] == [
            {
                "name": "category",
                "transform": "identity",
                "source-id": 2,
                "field-id": 1000,
            }
        ]
        default_spec = next(
            spec for spec in metadata["partition-specs"] if spec["spec-id"] == metadata["default-spec-id"]
        )
        fields = default_spec["fields"]
        assert fields[0] == {
            "name": "category",
            "transform": "void",
            "source-id": 2,
            "field-id": 1000,
        }
        assert fields[1]["name"] == "region"
        assert fields[1]["transform"] == "identity"
    finally:
        sql_catalog.drop_table(identifier)
