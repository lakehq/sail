# ruff: noqa: PLR2004

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _write_delta_with_row_groups(path, column_mapping, include_stats):
    path.mkdir()
    mapped = column_mapping != "none"
    physical_names = ["physical-id", "physical-value"] if mapped else ["id", "value"]
    schema_fields = []
    parquet_fields = []
    for index, (logical, physical, dtype) in enumerate(
        zip(["id", "value"], physical_names, [pa.int64(), pa.string()], strict=False)
    ):
        metadata = {"delta.columnMapping.id": index + 1, "delta.columnMapping.physicalName": physical}
        schema_fields.append(
            {
                "name": logical,
                "type": "long" if index == 0 else "string",
                "nullable": True,
                "metadata": metadata if mapped else {},
            }
        )
        parquet_fields.append(pa.field(physical, dtype, metadata={"PARQUET:field_id": str(index + 1)}))
    features = ["deletionVectors"] + (["columnMapping"] if mapped else [])
    configuration = {"delta.enableDeletionVectors": "true"}
    if mapped:
        configuration.update({"delta.columnMapping.mode": column_mapping, "delta.columnMapping.maxColumnId": "2"})
    actions = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": features,
                "writerFeatures": features,
            }
        },
        {
            "metaData": {
                "id": "dv-scan-regression",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": schema_fields}),
                "partitionColumns": [],
                "configuration": configuration,
                "createdTime": 0,
            }
        },
    ]
    for file_index, (start, end) in enumerate([(0, 4096), (4096, 4352)]):
        table = pa.Table.from_arrays(
            [pa.array(range(start, end), type=pa.int64()), pa.array([f"value-{i}" for i in range(start, end)])],
            schema=pa.schema(parquet_fields),
        )
        filename = path / f"part-{file_index}.parquet"
        pq.write_table(
            table, filename, row_group_size=128, data_page_size=256, write_batch_size=32, write_page_index=True
        )
        add = {
            "path": filename.name,
            "partitionValues": {},
            "size": filename.stat().st_size,
            "modificationTime": 0,
            "dataChange": True,
        }
        add["stats"] = json.dumps({"numRecords": end - start})
        if include_stats:
            add["stats"] = json.dumps(
                {
                    "numRecords": end - start,
                    "minValues": {physical_names[0]: start},
                    "maxValues": {physical_names[0]: end - 1},
                    "nullCount": {physical_names[0]: 0},
                }
            )
        actions.append({"add": add})
    log = path / "_delta_log"
    log.mkdir()
    (log / "00000000000000000000.json").write_text("".join(json.dumps(action) + "\n" for action in actions))


@pytest.mark.parametrize("metadata_as_data", [False, True])
@pytest.mark.parametrize(("column_mapping", "include_stats"), [("none", True), ("name", False), ("id", True)])
def test_dv_scan_pruning_projection_and_repeated_dml(spark, tmp_path, metadata_as_data, column_mapping, include_stats):
    path = tmp_path / "dv_scan"
    _write_delta_with_row_groups(path, column_mapping, include_stats)
    target = f"delta.`{path}`"
    spark.sql(f"DELETE FROM {target} WHERE id < 4096 AND id % 7 = 0")  # noqa: S608
    actions = [
        json.loads(line) for line in (path / "_delta_log" / "00000000000000000001.json").read_text().splitlines()
    ]
    assert any(action.get("add", {}).get("deletionVector", {}).get("cardinality", 0) > 0 for action in actions)

    def read():
        return spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower()).load(str(path))

    live = [i for i in range(4352) if i >= 4096 or i % 7 != 0]
    assert [row.id for row in read().where("id >= 512 AND id < 768").select("id").orderBy("id").collect()] == [
        i for i in live if 512 <= i < 768
    ]
    assert [row.id for row in read().where("id >= 4000").select("id").orderBy("id").collect()] == [
        i for i in live if i >= 4000
    ]
    assert len(read().selectExpr("1 AS present").limit(1).collect()) == 1
    assert read().where("id = 0").selectExpr("1 AS present").limit(1).collect() == []
    assert read().limit(0).collect() == []

    spark.sql(f"DELETE FROM {target} WHERE id >= 512 AND id < 528")  # noqa: S608
    live = [i for i in live if not 512 <= i < 528]
    spark.sql(f"DELETE FROM {target} WHERE value IN ('value-4097', 'value-4101')")  # noqa: S608
    live = [i for i in live if i not in (4097, 4101)]
    spark.sql(f"UPDATE {target} SET value = 'updated' WHERE id >= 640 AND id < 656")  # noqa: S608
    assert [(row.id, row.value) for row in read().where("id >= 500 AND id < 700").orderBy("id").collect()] == [
        (i, "updated" if 640 <= i < 656 else f"value-{i}") for i in live if 500 <= i < 700
    ]
    assert read().count() == len(live)
    assert spark.read.format("delta").option("versionAsOf", 0).load(str(path)).count() == 4352
