# ruff: noqa: PLR2004

import json

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.tests.spark.delta.utils import write_delta_with_row_groups


@pytest.mark.parametrize("metadata_as_data", [False, True])
@pytest.mark.parametrize(("column_mapping", "include_stats"), [("none", True), ("name", False), ("id", True)])
def test_dv_scan_pruning_projection_and_repeated_dml(spark, tmp_path, metadata_as_data, column_mapping, include_stats):
    path = tmp_path / "dv_scan"
    write_delta_with_row_groups(path, column_mapping, include_stats)
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
    metadata_frame = (
        read()
        .where("id >= 4000")
        .selectExpr("id", "input_file_name()", "input_file_block_start()", "input_file_block_length()")
    )
    metadata_rows = metadata_frame.orderBy("id").collect()
    data_files = [path / "part-0.parquet", path / "part-1.parquet"]
    assert [tuple(row) for row in metadata_rows] == [
        (i, data_file.as_uri(), 0, data_file.stat().st_size)
        for i in live
        if i >= 4000
        for data_file in [data_files[0 if i < 4096 else 1]]
    ]
    limited = metadata_frame.limit(3).collect()
    assert len(limited) == len({row.id for row in limited}) == 3
    for row in limited:
        assert row.id in live
        assert row.id >= 4000
        data_file = data_files[0 if row.id < 4096 else 1]
        assert tuple(row) == (row.id, data_file.as_uri(), 0, data_file.stat().st_size)
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
