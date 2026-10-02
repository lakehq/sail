# ruff: noqa: PLR2004

import json
from collections import Counter
from itertools import pairwise

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory


def _write_delta_with_row_groups(path, column_mapping, include_stats, *, value_padding=0):
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
    metadata_rows = (
        read()
        .where("id >= 4000")
        .selectExpr("id", "input_file_name()", "input_file_block_start()", "input_file_block_length()")
        .orderBy("id")
        .collect()
    )
    data_files = [path / "part-0.parquet", path / "part-1.parquet"]
    assert [tuple(row) for row in metadata_rows] == [
        (i, data_file.as_uri(), 0, data_file.stat().st_size)
        for i in live
        if i >= 4000
        for data_file in [data_files[0 if i < 4096 else 1]]
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


@pytest.mark.parametrize("column_mapping", ["none", "name", "id"])
def test_dv_input_file_metadata_preserves_scan_splits(tmp_path, column_mapping):
    path = tmp_path / "dv_metadata_splits"
    _write_delta_with_row_groups(path, column_mapping, include_stats=True, value_padding=512)
    assert (path / "part-0.parquet").stat().st_size > 1024 * 1024

    with (
        spark_connect_server(envs={"SAIL_EXECUTION__DEFAULT_PARALLELISM": "4"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        spark.sql(f"DELETE FROM delta.`{path}` WHERE id < 4096 AND id % 7 = 0")  # noqa: S608
        actions = [
            json.loads(line) for line in (path / "_delta_log" / "00000000000000000001.json").read_text().splitlines()
        ]
        assert any(action.get("add", {}).get("deletionVector", {}).get("cardinality", 0) > 0 for action in actions)
        frame = spark.read.format("delta").option("metadataAsDataRead", "false").load(str(path))
        rows = (
            frame.selectExpr(
                "id",
                "input_file_name() AS file_name",
                "input_file_block_start() AS block_start",
                "input_file_block_length() AS block_length",
                "length(value) + input_file_block_start() AS value_with_offset",
            )
            .orderBy("id")
            .collect()
        )
        file_names = [row.file_name for row in frame.selectExpr("input_file_name() AS file_name").collect()]

    live = [i for i in range(4352) if i >= 4096 or i % 7 != 0]
    assert [row.id for row in rows] == live
    assert Counter(file_names) == Counter(row.file_name for row in rows)
    ranges_by_file = {index: set() for index in range(2)}
    for row in rows:
        file_index = 0 if row.id < 4096 else 1
        assert row.file_name == (path / f"part-{file_index}.parquet").as_uri()
        assert row.value_with_offset == len(f"value-{row.id}") + 512 + row.block_start
        ranges_by_file[file_index].add((row.block_start, row.block_length))
    assert len(ranges_by_file[0]) > 1
    for file_index, file_ranges in ranges_by_file.items():
        ranges = sorted(file_ranges)
        assert ranges[0][0] == 0
        assert all(length > 0 for _, length in ranges)
        assert all(start + length == next_start for (start, length), (next_start, _) in pairwise(ranges))
        assert sum(length for _, length in ranges) == (path / f"part-{file_index}.parquet").stat().st_size


@pytest.mark.parametrize("metadata_as_data", [False, True])
def test_fragmented_dv_excludes_deleted_values_before_predicate_evaluation(tmp_path, metadata_as_data):
    path = tmp_path / "fragmented_dv"
    with (
        spark_connect_server(envs={"SAIL_PARQUET__PUSHDOWN_FILTERS": "true"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        (
            spark.range(10000, numPartitions=1)
            .selectExpr("id", "CASE WHEN id % 2 = 0 THEN 'bad' ELSE '10' END AS value")
            .coalesce(1)
            .write.format("delta")
            .option("delta.enableDeletionVectors", "true")
            .save(str(path))
        )
        spark.sql(f"DELETE FROM delta.`{path}` WHERE id % 2 = 0")  # noqa: S608
        actions = [
            json.loads(line) for line in (path / "_delta_log" / "00000000000000000001.json").read_text().splitlines()
        ]
        adds = [action["add"] for action in actions if "add" in action]
        assert len(adds) == 1
        assert adds[0]["deletionVector"]["cardinality"] == 5000
        frame = (
            spark.read.format("delta")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(str(path))
            .where("CAST(value AS BIGINT) > 0")
            .select("id")
        )
        assert sorted(row.id for row in frame.collect()) == list(range(1, 10000, 2))
        assert frame.limit(1).first().id % 2 == 1
