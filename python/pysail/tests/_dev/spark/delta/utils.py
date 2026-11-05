import json

import pyarrow as pa
import pyarrow.parquet as pq


def write_delta_with_row_groups(path, column_mapping, include_stats, *, value_padding=0):
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
            [
                pa.array(range(start, end), type=pa.int64()),
                pa.array([f"value-{i}" + "x" * value_padding for i in range(start, end)]),
            ],
            schema=pa.schema(parquet_fields),
        )
        filename = path / f"part-{file_index}.parquet"
        pq.write_table(
            table,
            filename,
            row_group_size=128,
            data_page_size=256,
            write_batch_size=32,
            write_page_index=True,
            compression="NONE" if value_padding else "snappy",
            use_dictionary=not value_padding,
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
