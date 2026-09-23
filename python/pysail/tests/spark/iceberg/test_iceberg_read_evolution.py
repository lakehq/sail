import copy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, ListType, LongType, MapType, NestedField, StringType, StructType

from pysail.testing.spark.steps.iceberg import (
    _find_latest_metadata,
    _latest_metadata_path,
    _metadata_file_version,
    _write_metadata_file,
)
from pysail.tests.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot


@pytest.mark.parametrize("list_type", [pa.list_, pa.large_list], ids=["list", "large_list"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_list_offset_widths_preserve_values_and_field_ids(spark, sql_catalog, tmp_path, list_type, metadata_as_data):
    identifier = "default.list_offsets"
    table = sql_catalog.create_table(
        identifier,
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "tags", ListType(3, StringType(), element_required=False), required=False),
            NestedField(
                4,
                "items",
                ListType(5, StructType(NestedField(6, "old", LongType(), required=False)), element_required=False),
                required=False,
            ),
        ),
    )
    try:
        schema = table.schema().as_arrow()
        schema = pa.schema(
            [schema.field("id")]
            + [
                schema.field(name).with_type(list_type(schema.field(name).type.value_field))
                for name in ["tags", "items"]
            ]
        )
        data = pa.Table.from_pylist(
            [
                {"id": 1, "tags": ["alpha", None, "中文"], "items": [{"old": 10}, None, {"old": None}]},
                {"id": 2, "tags": [], "items": []},
                {"id": 3, "tags": None, "items": None},
                {"id": 4, "tags": [None], "items": [None]},
            ],
            schema=schema,
        )
        imported = tmp_path / "lists.parquet"
        pq.write_table(data, imported)
        assert pq.read_schema(imported).equals(schema, check_metadata=True)
        table.add_files([imported.as_uri()])
        with table.update_schema() as update:
            update.rename_column(("items", "element", "old"), "value")

        frame = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(table.location())
        )
        assert frame.schema.simpleString() == "struct<id:bigint,tags:array<string>,items:array<struct<value:bigint>>>"
        assert [row.asDict(recursive=True) for row in frame.orderBy("id").collect()] == [
            {"id": 1, "tags": ["alpha", None, "中文"], "items": [{"value": 10}, None, {"value": None}]},
            {"id": 2, "tags": [], "items": []},
            {"id": 3, "tags": None, "items": None},
            {"id": 4, "tags": [None], "items": [None]},
        ]
        assert [tuple(row) for row in frame.selectExpr("id", "CAST(tags AS STRING)").orderBy("id").collect()] == [
            (1, "[alpha, NULL, 中文]"),
            (2, "[]"),
            (3, None),
            (4, "[NULL]"),
        ]
        assert [row.id for row in frame.filter("array_contains(tags, 'alpha')").collect()] == [1]
        assert frame.selectExpr("get(items, 0).value AS value").filter("value = 10").collect()[0].value == 10  # noqa: PLR2004
    finally:
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize(("format_version", "with_deletes"), [(2, False), (2, True), (3, False)])
def test_identity_partition_defaults_survive_reads_and_cow(spark, sql_catalog, tmp_path, format_version, with_deletes):
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestContent
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.typedef import Record

    from pysail.tests.spark.iceberg.test_iceberg_merge import _current_manifest_entries, _local_file_path

    identifier = "default.identity_defaults"
    name = "identity_defaults"
    table = sql_catalog.create_table(
        identifier,
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "p", StringType(), required=False),
            NestedField(3, "value", LongType(), required=False),
        ),
        partition_spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "p")),
    )
    try:
        for index, (partition, ids, values, physical_partition) in enumerate(
            [("x", [1, 2], [10, 20], False), ("y", [3], [30], True), (None, [4, 5], [40, 50], False)]
        ):
            fields = [pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"})]
            arrays = [pa.array(ids)]
            if physical_partition:
                fields.append(pa.field("p", pa.string(), metadata={b"PARQUET:field_id": b"2"}))
                arrays.append(pa.array([partition] * len(ids)))
            fields.append(pa.field("value", pa.int64(), metadata={b"PARQUET:field_id": b"3"}))
            arrays.append(pa.array(values))
            imported = tmp_path / f"imported-{index}.parquet"
            pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), imported)
            data_file = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=imported.as_uri(),
                file_format=FileFormat.PARQUET,
                partition=Record(partition),
                record_count=len(ids),
                file_size_in_bytes=imported.stat().st_size,
                spec_id=table.spec().spec_id,
            )
            with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
                append.append_data_file(data_file)
        with table.update_schema() as update:
            update.rename_column("p", "part")
        path = _local_file_path(table.location())
        if with_deletes:
            _append_equality_delete_snapshot(table, pa.table({"id": [5]}), [1], partition=Record(None))
        spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{path.as_uri()}'")
        if format_version == 3:  # noqa: PLR2004
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('format-version'='3')")
        expected = [(1, "x", 10), (2, "x", 20), (3, "y", 30), (4, None, 40)]
        if not with_deletes:
            expected.append((5, None, 50))
        for metadata_as_data in [False, True]:
            if with_deletes and metadata_as_data:
                continue
            frame = (
                spark.read.format("iceberg")
                .option("metadataAsDataRead", str(metadata_as_data).lower())
                .load(path.as_uri())
                .select("id", "part", "value")
            )
            assert [tuple(row) for row in frame.orderBy("id").collect()] == expected
            assert [row.id for row in frame.filter("part = 'x'").orderBy("id").collect()] == [1, 2]
            assert frame.filter("part IS NULL").count() == (1 if with_deletes else 2)
            assert frame.limit(1).count() == 1
        before = _find_latest_metadata(path)
        spark.sql(f"UPDATE {name} SET value = value + 1 WHERE id IN (1, 4)").collect()  # noqa: S608
        expected = [(i, p, v + (i in (1, 4))) for i, p, v in expected]
        assert [
            tuple(row) for row in spark.table(name).select("id", "part", "value").orderBy("id").collect()
        ] == expected
        after = _find_latest_metadata(path)
        assert after["snapshots"][:-1] == before["snapshots"]
        for entry in _current_manifest_entries(path, ManifestContent.DATA):
            if entry.data_file.file_path.startswith(path.as_uri()):
                rows = pq.ParquetFile(_local_file_path(entry.data_file.file_path)).read().to_pylist()
                assert rows
                assert all(row["part"] == ("x" if row["id"] in (1, 2) else None) for row in rows)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        sql_catalog.drop_table(identifier)


def _evolve_schema(path, fields):
    metadata = _find_latest_metadata(path)
    schema_id = max(schema["schema-id"] for schema in metadata["schemas"]) + 1
    metadata["schemas"].append({"type": "struct", "schema-id": schema_id, "fields": fields})
    metadata["current-schema-id"] = schema_id
    metadata["last-column-id"] = max(metadata["last-column-id"], 100)
    version = _metadata_file_version(_latest_metadata_path(path)) + 1
    _write_metadata_file(path / "metadata" / f"v{version}.metadata.json", metadata)


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_missing_fields_use_initial_defaults_and_writes_use_only_write_defaults(spark, tmp_path, metadata_as_data):
    path = tmp_path / "defaults"
    spark.createDataFrame([(1, (10,)), (2, None)], "id LONG, payload STRUCT<x: LONG>").write.format("iceberg").option(
        "format-version", "3"
    ).save(path.as_uri())
    metadata = _find_latest_metadata(path)
    fields = copy.deepcopy(metadata["schemas"][-1]["fields"])
    fields[1]["type"]["fields"].append(
        {"id": 4, "name": "extra", "required": True, "type": "long", "initial-default": 13, "write-default": 17}
    )
    fields.extend(
        [
            {
                "id": 5,
                "name": "required_value",
                "required": True,
                "type": "long",
                "initial-default": 7,
                "write-default": 9,
            },
            {"id": 6, "name": "read_only", "required": False, "type": "long", "initial-default": 11},
            {
                "id": 7,
                "name": "point",
                "required": False,
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "id": 8,
                            "name": "x",
                            "required": True,
                            "type": "long",
                            "initial-default": 19,
                            "write-default": 23,
                        }
                    ],
                },
                "initial-default": {},
                "write-default": {},
            },
            {
                "id": 9,
                "name": "bytes",
                "required": False,
                "type": "binary",
                "initial-default": "00ff",
                "write-default": "ff00",
            },
            {
                "id": 10,
                "name": "tags",
                "required": False,
                "type": {"type": "list", "element-id": 11, "element": "long", "element-required": False},
                "initial-default": [1, 2],
                "write-default": [3],
            },
            {
                "id": 12,
                "name": "explicit_null",
                "required": False,
                "type": "long",
                "initial-default": 31,
                "write-default": None,
            },
            {
                "id": 13,
                "name": "lookup",
                "required": False,
                "type": {
                    "type": "map",
                    "key-id": 14,
                    "key": "string",
                    "value-id": 15,
                    "value": "long",
                    "value-required": False,
                },
                "initial-default": {"keys": ["a"], "values": [2]},
                "write-default": {"keys": ["b"], "values": [3]},
            },
        ]
    )
    _evolve_schema(path, fields)

    def read():
        return (
            spark.read.format("iceberg").option("metadataAsDataRead", str(metadata_as_data).lower()).load(path.as_uri())
        )

    rows = read().orderBy("id").collect()
    assert rows[0].payload.extra == 13  # noqa: PLR2004
    assert rows[1].payload is None
    assert [row.required_value for row in rows] == [7, 7]
    assert [row.read_only for row in rows] == [11, 11]
    assert [row.point.x for row in rows] == [19, 19]
    assert [bytes(row.bytes) for row in rows] == [b"\x00\xff", b"\x00\xff"]
    assert [row.tags for row in rows] == [[1, 2], [1, 2]]
    assert [row.lookup for row in rows] == [{"a": 2}, {"a": 2}]
    assert read().filter("required_value = 7").count() == 2  # noqa: PLR2004
    assert read().filter("required_value IS NULL").count() == 0
    assert read().filter("payload.extra = 13").select("id").collect()[0].id == 1
    spark.createDataFrame([(3, (30,)), (4, None)], "id LONG, payload STRUCT<x: LONG>").write.format("iceberg").mode(
        "append"
    ).save(path.as_uri())
    rows = read().orderBy("id").collect()
    assert rows[2].payload.extra == 17  # noqa: PLR2004
    assert rows[3].payload is None
    assert [row.required_value for row in rows] == [7, 7, 9, 9]
    assert [row.read_only for row in rows] == [11, 11, None, None]
    assert [row.point.x for row in rows] == [19, 19, 23, 23]
    assert [row.tags for row in rows] == [[1, 2], [1, 2], [3], [3]]
    assert [row.lookup for row in rows] == [{"a": 2}, {"a": 2}, {"b": 3}, {"b": 3}]
    assert [row.explicit_null for row in rows] == [31, 31, None, None]
    assert bytes(rows[2].bytes) == b"\xff\x00"
    written_schema = _find_latest_metadata(path)["schemas"][-1]
    null_field = next(field for field in written_schema["fields"] if field["name"] == "explicit_null")
    assert "write-default" in null_field
    assert null_field["write-default"] is None


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_name_mapping_reads_renamed_imported_nested_columns(spark, sql_catalog, tmp_path, metadata_as_data):
    table = sql_catalog.create_table(
        "default.imported_names",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "payload", StructType(NestedField(3, "old", LongType(), required=False)), required=False),
        ),
    )
    try:
        imported = tmp_path / "imported.parquet"
        pq.write_table(
            pa.table({"id": [1, 2], "payload": pa.array([{"old": 10}, None], pa.struct([("old", pa.int64())]))}),
            imported,
        )
        table.add_files([imported.as_uri()])
        with table.update_schema() as update:
            update.rename_column(("payload", "old"), "value")
            update.rename_column("payload", "renamed")
            update.rename_column("id", "key")
        mapping = json.loads(table.properties["schema.name-mapping.default"])
        assert any("id" in field["names"] and "key" in field["names"] for field in mapping)
        result = (
            spark.read.format("iceberg")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(table.location())
        )
        rows = result.orderBy("key").collect()
        assert [(row.key, None if row.renamed is None else row.renamed.value) for row in rows] == [(1, 10), (2, None)]
        assert result.filter("renamed.value = 10").select("key").collect()[0].key == 1
    finally:
        sql_catalog.drop_table("default.imported_names")


def test_name_mapping_handles_list_elements_and_map_values(spark, sql_catalog, tmp_path):
    table = sql_catalog.create_table(
        "default.imported_containers",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(
                2,
                "items",
                ListType(3, StructType(NestedField(4, "old", LongType(), required=False)), element_required=False),
                required=False,
            ),
            NestedField(
                5,
                "lookup",
                MapType(
                    6,
                    StringType(),
                    7,
                    StructType(NestedField(8, "old", LongType(), required=False)),
                    value_required=False,
                ),
                required=False,
            ),
        ),
    )
    try:
        path = tmp_path / "containers.parquet"
        child = pa.struct([("old", pa.int64())])
        pq.write_table(
            pa.table(
                {
                    "id": [1, 2],
                    "items": pa.array([[{"old": 10}, None], None], pa.list_(child)),
                    "lookup": pa.array([[("a", {"old": 20}), ("b", None)], None], pa.map_(pa.string(), child)),
                }
            ),
            path,
        )
        table.add_files([path.as_uri()])
        with table.update_schema() as update:
            update.rename_column(("items", "element", "old"), "value")
            update.rename_column(("lookup", "value", "old"), "value")
        rows = spark.read.format("iceberg").load(table.location()).orderBy("id").collect()
        assert (rows[0].items[0].value, rows[0].items[1]) == (10, None)
        assert (rows[0].lookup["a"].value, rows[0].lookup["b"]) == (20, None)
        assert (rows[1].items, rows[1].lookup) == (None, None)
    finally:
        sql_catalog.drop_table("default.imported_containers")


@pytest.mark.parametrize(
    "mapping",
    [
        [{"field-id": 1, "names": ["id"]}, {"field-id": 2, "names": ["id"]}],
        [{"field-id": 1, "names": ["id"]}, {"field-id": 1, "names": ["old"]}],
    ],
)
def test_invalid_name_mapping_is_rejected(spark, tmp_path, mapping):
    path = tmp_path / "invalid_mapping"
    spark.createDataFrame([(1,)], "id LONG").write.format("iceberg").save(path.as_uri())
    metadata = _find_latest_metadata(path)
    metadata["properties"]["schema.name-mapping.default"] = json.dumps(mapping)
    version = _metadata_file_version(_latest_metadata_path(path)) + 1
    head = path / "metadata" / f"v{version}.metadata.json"
    _write_metadata_file(head, metadata)
    with pytest.raises(Exception, match="Iceberg name mapping"):
        spark.read.format("iceberg").load(path.as_uri()).collect()
    assert _latest_metadata_path(path) == head


@pytest.mark.parametrize("drop_key", [False, True, "parent"])
def test_equality_delete_nested_keys_preserve_parent_nulls_and_dropped_fields(spark, sql_catalog, drop_key):
    table = sql_catalog.create_table(
        "default.nested_equality",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "key", LongType(), required=False),
                    NestedField(4, "kept", LongType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    try:
        table.append(
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "payload": pa.array(
                        [{"key": 7, "kept": 1}, {"key": 8, "kept": 2}, None, {"key": None, "kept": 4}],
                        pa.struct([("key", pa.int64()), ("kept", pa.int64())]),
                    ),
                }
            )
        )
        key_id = table.schema().find_field("payload.key").field_id
        parent_id = table.schema().find_field("payload").field_id
        key = pa.field("old_key", pa.int64(), metadata={b"PARQUET:field_id": str(key_id).encode()})
        parent = pa.field("old_payload", pa.struct([key]), metadata={b"PARQUET:field_id": str(parent_id).encode()})
        deletes = pa.Table.from_pylist(
            [{"old_payload": {"old_key": 7}}, {"old_payload": None}], schema=pa.schema([parent])
        )
        path = _append_equality_delete_snapshot(table, deletes, [key_id], assign_root_ids=False)
        fields = copy.deepcopy(_find_latest_metadata(path)["schemas"][-1]["fields"])
        if drop_key == "parent":
            fields = fields[:1]
        elif drop_key:
            fields[1]["type"]["fields"] = [field for field in fields[1]["type"]["fields"] if field["id"] != key_id]
        else:
            fields[1]["type"]["fields"][0]["name"] = "renamed_key"
        _evolve_schema(path, fields)
        result = spark.read.format("iceberg").load(path.as_uri())
        assert [row.id for row in result.select("id").collect()] == [2]
        if drop_key == "parent":
            assert result.columns == ["id"]
        else:
            assert result.collect()[0].payload.kept == 2  # noqa: PLR2004
        spark.sql(f"CREATE TABLE nested_eq_target USING iceberg LOCATION '{path.as_uri()}'")
        try:
            spark.sql("UPDATE nested_eq_target SET id = 20 WHERE id = 2").collect()
            assert [row.id for row in spark.table("nested_eq_target").collect()] == [20]
        finally:
            spark.sql("DROP TABLE nested_eq_target")
    finally:
        sql_catalog.drop_table("default.nested_equality")


def test_equality_delete_dropped_key_does_not_bind_reused_name(spark, sql_catalog):
    table = sql_catalog.create_table(
        "default.dropped_equality",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "key", LongType(), required=False),
        ),
    )
    try:
        table.append(pa.table({"id": [1, 2, 3], "key": [7, 8, None]}))
        path = _append_equality_delete_snapshot(table, pa.table({"key": [7]}), [2])
        fields = copy.deepcopy(_find_latest_metadata(path)["schemas"][-1]["fields"])
        fields[1]["id"] = 3
        _evolve_schema(path, fields)
        rows = spark.read.format("iceberg").load(path.as_uri()).orderBy("id").collect()
        assert [tuple(row) for row in rows] == [(2, None), (3, None)]
    finally:
        sql_catalog.drop_table("default.dropped_equality")


def test_equality_delete_added_key_matches_null_in_older_files(spark, sql_catalog):
    table = sql_catalog.create_table("default.added_equality", Schema(NestedField(1, "id", LongType(), required=False)))
    try:
        table.append(pa.table({"id": [1, 2]}))
        with table.update_schema() as update:
            update.add_column("key", LongType())
        table.append(pa.table({"id": [3, 4], "key": [30, 40]}))
        key_id = table.schema().find_field("key").field_id
        path = _append_equality_delete_snapshot(table, pa.table({"key": pa.array([None, 30], pa.int64())}), [key_id])
        assert [row.id for row in spark.read.format("iceberg").load(path.as_uri()).collect()] == [4]
    finally:
        sql_catalog.drop_table("default.added_equality")


def test_equality_delete_promoted_composite_keys_follow_ids_and_delete_key_order(spark, sql_catalog):
    table = sql_catalog.create_table(
        "default.promoted_equality",
        Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "key", IntegerType(), required=False),
            NestedField(3, "label", StringType(), required=False),
        ),
    )
    try:
        table.append(pa.table({"id": [1, 2, 3], "key": pa.array([7, 7, 8], pa.int32()), "label": ["a", "b", "a"]}))
        delete_schema = pa.schema(
            [
                pa.field("old_key", pa.int32(), metadata={b"PARQUET:field_id": b"2"}),
                pa.field("old_label", pa.string(), metadata={b"PARQUET:field_id": b"3"}),
            ]
        )
        deletes = pa.Table.from_pylist([{"old_key": 7, "old_label": "a"}], schema=delete_schema)
        path = _append_equality_delete_snapshot(table, deletes, [3, 2], assign_root_ids=False)
        fields = copy.deepcopy(_find_latest_metadata(path)["schemas"][-1]["fields"])
        fields[1].update(name="promoted", type="long")
        _evolve_schema(path, fields)
        rows = spark.read.format("iceberg").load(path.as_uri()).orderBy("id").collect()
        assert [tuple(row) for row in rows] == [(2, 7, "b"), (3, 8, "a")]
    finally:
        sql_catalog.drop_table("default.promoted_equality")


def test_partitioned_write_fills_default_before_partitioning(spark, tmp_path):
    path = tmp_path / "partition_defaults"
    spark.createDataFrame([(1, 7)], "id LONG, category LONG").write.format("iceberg").option(
        "format-version", "3"
    ).partitionBy("category").save(path.as_uri())
    fields = copy.deepcopy(_find_latest_metadata(path)["schemas"][-1]["fields"])
    fields[1].update({"initial-default": 7, "write-default": 9})
    _evolve_schema(path, fields)
    spark.createDataFrame([(2,)], "id LONG").write.format("iceberg").mode("append").save(path.as_uri())
    assert [tuple(row) for row in spark.read.format("iceberg").load(path.as_uri()).orderBy("id").collect()] == [
        (1, 7),
        (2, 9),
    ]
    assert list((path / "data" / "category=9").glob("*.parquet"))


@pytest.mark.parametrize("default", [None, "wrong type"])
def test_invalid_required_default_rejected_before_commit(spark, tmp_path, default):
    path = tmp_path / "invalid_default"
    spark.createDataFrame([(1,)], "id LONG").write.format("iceberg").option("format-version", "3").save(path.as_uri())
    fields = copy.deepcopy(_find_latest_metadata(path)["schemas"][-1]["fields"])
    fields.append(
        {
            "id": 2,
            "name": "required_value",
            "type": "long",
            "required": True,
            "initial-default": default,
            "write-default": 9,
        }
    )
    _evolve_schema(path, fields)
    head = _latest_metadata_path(path)
    content = head.read_bytes()
    with pytest.raises(Exception, match="data did not match"):
        spark.read.format("iceberg").load(path.as_uri()).collect()
    with pytest.raises(Exception, match="data did not match"):
        spark.createDataFrame([(2,)], "id LONG").write.format("iceberg").mode("append").save(path.as_uri())
    assert _latest_metadata_path(path) == head
    assert head.read_bytes() == content


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_identity_partition_pruning_after_int_to_long_promotion(spark, tmp_path, metadata_as_data):
    path = tmp_path / "promoted_partition"
    spark.createDataFrame([(1, 7)], "id INT, p INT").write.format("iceberg").option("format-version", "3").partitionBy(
        "p"
    ).save(path.as_uri())
    metadata = _find_latest_metadata(path)
    fields = copy.deepcopy(metadata["schemas"][-1]["fields"])
    fields[1]["type"] = "long"
    _evolve_schema(path, fields)
    table = spark.read.format("iceberg").option("metadataAsDataRead", str(metadata_as_data).lower()).load(path.as_uri())
    assert [tuple(row) for row in table.filter("p = 7L").collect()] == [(1, 7)]
