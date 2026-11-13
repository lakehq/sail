from __future__ import annotations

import json
import os
from contextlib import contextmanager
from urllib.parse import quote
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Row


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


def _commits(path):
    return {
        int(file.stem): [json.loads(line) for line in file.read_text().splitlines()]
        for file in (path / "_delta_log").glob("*.json")
        if file.stem.isdigit()
    }


def _active_files(path):
    # These bounded fixtures retain every JSON commit, so file state can be checked directly.
    def identity(action):
        dv = action.get("deletionVector")
        return action["path"], None if dv is None else (dv["storageType"], dv["pathOrInlineDv"], dv.get("offset"))

    files = {}
    for _, actions in sorted(_commits(path).items()):
        for action in actions:
            if "remove" in action:
                files.pop(identity(action["remove"]), None)
        for action in actions:
            if "add" in action:
                files[identity(action["add"])] = action["add"]
    return [Row(**{"deletionVector": None, **add}) for add in files.values()]


def _paths(rows):
    # Hadoop Path strings are physical paths; preserve literal percent escapes during OS conversion.
    return {os.path.normpath(url2pathname(quote(row.path.removeprefix("file:"), safe="/:"))) for row in rows}


_TABLE_NOT_VIEW = (
    r"(?i)(EXPECT_TABLE_NOT_VIEW|expects a table|requires a table|"
    r"not supported for (?:temporary )?views?\b|\bis (?:a )?(?:temporary )?view\b|found (?:a )?view\b)"
)


def _tree(path):
    return {str(file.relative_to(path)): file.read_bytes() for file in path.rglob("*") if file.is_file()}


@contextmanager
def _settings(spark, **settings):
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _effective(path, key):
    return next(
        action[key]
        for _, actions in sorted(_commits(path).items(), reverse=True)
        for action in reversed(actions)
        if key in action
    )


def _assert_unchanged(spark, name, path, before, rows, schema):
    assert _tree(path) == before
    assert spark.table(name).schema == schema
    assert spark.table(name).orderBy("id").collect() == rows
