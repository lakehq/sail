# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

from __future__ import annotations

import itertools
import json
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import _commits, _effective

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration

_ROW_TRACKING = "'delta.enableRowTracking' = 'true'"
_DELETION_VECTORS = "'delta.enableDeletionVectors' = 'true'"


def _row_tracking_state(path: Path) -> dict:
    """Replays the commits of the table and checks the writer requirements for row tracking of
    the Delta protocol. Returns the active files, the row ID high-water mark, and the value of
    the `delta.rowTracking.preserved` tag of each commit."""
    active: dict[str, dict] = {}
    high_water_mark = -1
    preserved = {}
    for version, actions in sorted(_commits(path).items()):
        removes = [action["remove"] for action in actions if "remove" in action]
        adds = [action["add"] for action in actions if "add" in action]
        domains = [
            action["domainMetadata"]
            for action in actions
            if "domainMetadata" in action and action["domainMetadata"]["domain"] == "delta.rowTracking"
        ]
        commit_info = next(action["commitInfo"] for action in actions if "commitInfo" in action)
        preserved[version] = (commit_info.get("tags") or {}).get("delta.rowTracking.preserved")

        removed = {}
        for remove in removes:
            previous = active.pop(remove["path"])
            removed[remove["path"]] = previous
            # A Remove action has the row tracking information of the Add action of the file.
            assert remove.get("baseRowId") == previous["baseRowId"], (version, remove)
            assert remove.get("defaultRowCommitVersion") == previous["defaultRowCommitVersion"], (version, remove)

        committed_high_water_mark = high_water_mark
        for add in adds:
            assert add.get("baseRowId") is not None, (version, add)
            assert add.get("defaultRowCommitVersion") is not None, (version, add)
            count = json.loads(add["stats"])["numRecords"]
            previous = removed.get(add["path"])
            if previous is not None:
                # A file that is committed again keeps its row tracking information.
                assert add["baseRowId"] == previous["baseRowId"], (version, add)
                assert add["defaultRowCommitVersion"] == previous["defaultRowCommitVersion"], (version, add)
            else:
                # A new file has row IDs above the high-water mark, and the version of its commit.
                assert add["baseRowId"] > committed_high_water_mark, (version, add)
                assert add["defaultRowCommitVersion"] == version, (version, add)
                high_water_mark = max(high_water_mark, add["baseRowId"] + count - 1)
            active[add["path"]] = add

        if high_water_mark > committed_high_water_mark:
            assert len(domains) == 1, (version, domains)
        for domain in domains:
            assert domain["removed"] is False
            assert json.loads(domain["configuration"])["rowIdHighWaterMark"] >= high_water_mark, (version, domain)
            high_water_mark = json.loads(domain["configuration"])["rowIdHighWaterMark"]

    # The row IDs of the active files do not overlap.
    ranges = sorted(
        (add["baseRowId"], add["baseRowId"] + json.loads(add["stats"])["numRecords"] - 1) for add in active.values()
    )
    for (_, end), (start, _) in itertools.pairwise(ranges):
        assert end < start, ranges
    return {"active": active, "high_water_mark": high_water_mark, "preserved": preserved}


@pytest.fixture
def row_tracking_table(spark, tmp_path, request):
    properties = getattr(request, "param", _ROW_TRACKING)
    name = "row_tracking_writes"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ({properties})"
    ).collect()
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def test_row_tracking_table_creation(spark, row_tracking_table):
    name, path = row_tracking_table
    protocol = _effective(path, "protocol")
    assert protocol["minWriterVersion"] == 7
    assert {"rowTracking", "domainMetadata"} <= set(protocol["writerFeatures"])
    assert "rowTracking" not in (protocol.get("readerFeatures") or [])
    configuration = _effective(path, "metaData")["configuration"]
    assert configuration["delta.enableRowTracking"] == "true"
    row_id_column = configuration["delta.rowTracking.materializedRowIdColumnName"]
    row_commit_version_column = configuration["delta.rowTracking.materializedRowCommitVersionColumnName"]
    assert row_id_column.startswith("_row-id-col-")
    assert row_commit_version_column.startswith("_row-commit-version-col-")
    assert spark.table(name).collect() == []
    assert _row_tracking_state(path) == {"active": {}, "high_water_mark": -1, "preserved": {0: "true"}}


@pytest.mark.parametrize(
    "row_tracking_table",
    [_ROW_TRACKING, f"{_ROW_TRACKING}, {_DELETION_VECTORS}"],
    ids=["copy-on-write", "deletion-vectors"],
    indirect=True,
)
def test_row_tracking_writes(spark, row_tracking_table):
    name, path = row_tracking_table
    deletion_vectors = "delta.enableDeletionVectors" in _effective(path, "metaData")["configuration"]

    spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b'), (3, 'c')").collect()
    spark.sql(f"INSERT INTO {name} VALUES (4, 'd'), (5, 'e')").collect()
    state = _row_tracking_state(path)
    assert state["high_water_mark"] == 4
    assert state["preserved"] == {0: "true", 1: "true", 2: "true"}
    assert sorted(add["defaultRowCommitVersion"] for add in state["active"].values()) == [1, 2]

    spark.sql(f"DELETE FROM {name} WHERE id = 2").collect()
    state = _row_tracking_state(path)
    if deletion_vectors:
        # The file with the deleted row is committed again with a deletion vector.
        assert state["high_water_mark"] == 4
        assert state["preserved"][3] == "true"
    else:
        # The rows that the delete copies to a new file get fresh row IDs.
        assert state["high_water_mark"] == 6
        assert state["preserved"][3] == "false"

    spark.sql(f"UPDATE {name} SET value = 'x' WHERE id = 4").collect()
    state = _row_tracking_state(path)
    assert state["preserved"][4] == "false"

    spark.sql(
        f"MERGE INTO {name} AS t USING (SELECT 5 AS id, 'y' AS value UNION ALL SELECT 6, 'f') AS s "
        "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *"
    ).collect()
    state = _row_tracking_state(path)
    assert state["preserved"][5] == "false"
    assert spark.table(name).orderBy("id").collect() == [
        Row(id=1, value="a"),
        Row(id=3, value="c"),
        Row(id=4, value="x"),
        Row(id=5, value="y"),
        Row(id=6, value="f"),
    ]

    # The rows of an overwrite are new rows.
    spark.sql(f"INSERT OVERWRITE {name} VALUES (7, 'g'), (8, 'h')").collect()
    state = _row_tracking_state(path)
    assert state["preserved"][6] == "true"
    spark.sql(f"DELETE FROM {name}").collect()
    state = _row_tracking_state(path)
    assert state["active"] == {}
    assert state["preserved"][7] == "true"
    spark.sql(f"INSERT INTO {name} VALUES (9, 'i')").collect()
    state = _row_tracking_state(path)
    assert [add["baseRowId"] for add in state["active"].values()] == [state["high_water_mark"]]
    assert spark.table(name).collect() == [Row(id=9, value="i")]


def test_row_tracking_dataframe_writes(spark, tmp_path):
    path = tmp_path / "row_tracking"
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, value STRING")
    df.write.format("delta").option("delta.enableRowTracking", "true").save(str(path))
    df.write.format("delta").mode("append").save(str(path))
    state = _row_tracking_state(path)
    assert state["high_water_mark"] == 3
    df.write.format("delta").mode("overwrite").save(str(path))
    state = _row_tracking_state(path)
    assert state["high_water_mark"] == 5
    assert sum(json.loads(add["stats"])["numRecords"] for add in state["active"].values()) == 2
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [
        Row(id=1, value="a"),
        Row(id=2, value="b"),
    ]


def test_row_tracking_replace_where(spark, tmp_path):
    path = tmp_path / "row_tracking"
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, value STRING").coalesce(1)
    df.write.format("delta").option("delta.enableRowTracking", "true").save(str(path))
    assert len(_row_tracking_state(path)["active"]) == 1
    # The row that does not match the predicate is copied to a new file with a new row ID.
    (
        spark.createDataFrame([(1, "x")], "id BIGINT, value STRING")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "id = 1")
        .save(str(path))
    )
    state = _row_tracking_state(path)
    assert state["preserved"] == {0: "true", 1: "false"}
    assert state["high_water_mark"] == 3
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [
        Row(id=1, value="x"),
        Row(id=2, value="b"),
    ]


def test_row_tracking_write_to_overwrite(spark, tmp_path):
    name = "row_tracking_write_to_overwrite"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ({_ROW_TRACKING})"
    ).collect()
    try:
        spark.createDataFrame([(1, "a"), (2, "b")], "id BIGINT, value STRING").coalesce(1).writeTo(name).append()
        assert len(_row_tracking_state(path)["active"]) == 1
        # The condition has no SQL text, and the row that does not match it is copied to a
        # new file with a new row ID.
        spark.createDataFrame([(1, "x")], "id BIGINT, value STRING").writeTo(name).overwrite(F.col("id") == 1)
        state = _row_tracking_state(path)
        assert state["preserved"] == {0: "true", 1: "true", 2: "false"}
        assert state["high_water_mark"] == 3
        assert spark.table(name).orderBy("id").collect() == [
            Row(id=1, value="x"),
            Row(id=2, value="b"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def test_row_tracking_with_checkpoints(spark, tmp_path):
    name = "row_tracking_checkpoints"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ({_ROW_TRACKING}, 'delta.checkpointInterval' = '2')"
    ).collect()
    try:
        for i in range(4):
            spark.sql(f"INSERT INTO {name} VALUES ({i}, 'v')").collect()
        checkpoints = sorted((path / "_delta_log").glob("*.checkpoint*.parquet"))
        assert checkpoints
        # The checkpoint has the row tracking information of the files and the metadata domain.
        checkpoint = pq.read_table(checkpoints[-1]).to_pylist()
        adds = [row["add"] for row in checkpoint if row.get("add") is not None]
        assert adds
        assert all(add["baseRowId"] is not None and add["defaultRowCommitVersion"] is not None for add in adds)
        domains = [row["domainMetadata"] for row in checkpoint if row.get("domainMetadata") is not None]
        assert [domain["domain"] for domain in domains] == ["delta.rowTracking"]
        # The files that are read from the checkpoint are removed with their row tracking information,
        # and new row IDs are above the high-water mark of the checkpoint.
        spark.sql(f"UPDATE {name} SET value = 'w' WHERE id < 2").collect()
        spark.sql(f"DELETE FROM {name} WHERE id = 3").collect()
        spark.sql(f"INSERT INTO {name} VALUES (10, 'v')").collect()
        _row_tracking_state(path)
        assert spark.table(name).orderBy("id").collect() == [
            Row(id=0, value="w"),
            Row(id=1, value="w"),
            Row(id=2, value="v"),
            Row(id=10, value="v"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def test_row_tracking_cannot_be_enabled_on_existing_table(spark, tmp_path):
    name = "row_tracking_existing"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1)").collect()
        with pytest.raises(Exception, match=r"(?i)(unsupported|not supported|unknown configuration)"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({_ROW_TRACKING})").collect()
        with pytest.raises(Exception, match=r"(?i)(unsupported|not supported)"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.rowTracking' = 'supported')").collect()
        assert "writerFeatures" not in _effective(path, "protocol")
        spark.sql(f"INSERT INTO {name} VALUES (2)").collect()
        assert spark.table(name).orderBy("id").collect() == [Row(id=1), Row(id=2)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def _jvm_row_tracking(delta_jvm_spark: SparkSession, path: Path) -> list[Row]:
    return delta_jvm_spark.sql(
        "SELECT id, value, _metadata.row_id AS row_id, _metadata.row_commit_version AS row_commit_version "
        f"FROM delta.`{escape_sql_identifier(str(path))}` ORDER BY id"
    ).collect()


def test_delta_spark_reads_row_tracking_written_by_sail(spark, delta_jvm_spark: SparkSession, tmp_path):
    name = "row_tracking_interop"
    path = tmp_path / "row_tracking"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ({_ROW_TRACKING})"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b'), (3, 'c')").collect()
        spark.sql(f"INSERT INTO {name} VALUES (4, 'd')").collect()
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert [(row.id, row.value, row.row_commit_version) for row in rows] == [
            (1, "a", 1),
            (2, "b", 1),
            (3, "c", 1),
            (4, "d", 2),
        ]
        assert sorted(row.row_id for row in rows) == [0, 1, 2, 3]

        spark.sql(f"UPDATE {name} SET value = 'x' WHERE id = 1").collect()
        spark.sql(f"DELETE FROM {name} WHERE id = 2").collect()
        spark.sql(
            f"MERGE INTO {name} AS t USING (SELECT 4 AS id, 'y' AS value UNION ALL SELECT 5, 'e') AS s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        state = _row_tracking_state(path)
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert [(row.id, row.value) for row in rows] == [(1, "x"), (3, "c"), (4, "y"), (5, "e")]
        assert len({row.row_id for row in rows}) == len(rows)
        assert max(row.row_id for row in rows) <= state["high_water_mark"]

        # Delta Spark continues the table, and keeps the row IDs of the rows that it updates.
        before = {row.id: row.row_id for row in rows}
        table = f"delta.`{escape_sql_identifier(str(path))}`"
        delta_jvm_spark.sql(f"UPDATE {table} SET value = 'z' WHERE id = 3")
        delta_jvm_spark.sql(f"INSERT INTO {table} VALUES (6, 'f')")
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert [(row.id, row.value) for row in rows] == [(1, "x"), (3, "z"), (4, "y"), (5, "e"), (6, "f")]
        assert {row.id: row.row_id for row in rows if row.id in before} == before
        assert len({row.row_id for row in rows}) == len(rows)
        assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [
            Row(id=1, value="x"),
            Row(id=3, value="z"),
            Row(id=4, value="y"),
            Row(id=5, value="e"),
            Row(id=6, value="f"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["copy-on-write", "deletion-vectors"])
def test_sail_writes_row_tracking_table_of_delta_spark(
    spark, delta_jvm_spark: SparkSession, tmp_path, deletion_vectors
):
    path = tmp_path / "row_tracking"
    table = f"delta.`{escape_sql_identifier(str(path))}`"
    properties = f"{_ROW_TRACKING}, 'delta.enableDeletionVectors' = '{str(deletion_vectors).lower()}'"
    delta_jvm_spark.sql(f"CREATE TABLE {table} (id BIGINT, value STRING) USING DELTA TBLPROPERTIES ({properties})")
    delta_jvm_spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    # The update of Delta Spark writes the materialized row tracking columns to the data file.
    delta_jvm_spark.sql(f"UPDATE {table} SET value = 'x' WHERE id = 1")
    delta_jvm_spark.sql(f"INSERT INTO {table} VALUES (4, 'd')")
    before = _jvm_row_tracking(delta_jvm_spark, path)
    high_water_mark = _row_tracking_state(path)["high_water_mark"]

    name = "row_tracking_interop"
    spark.sql(f"CREATE TABLE {name} USING delta LOCATION '{escape_sql_string_literal(str(path))}'").collect()
    try:
        assert spark.table(name).orderBy("id").collect() == [
            Row(id=1, value="x"),
            Row(id=2, value="b"),
            Row(id=3, value="c"),
            Row(id=4, value="d"),
        ]
        spark.sql(f"INSERT INTO {name} VALUES (5, 'e'), (6, 'f')").collect()
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert rows[:4] == before
        assert sorted(row.row_id for row in rows[4:]) == [high_water_mark + 1, high_water_mark + 2]

        spark.sql(f"UPDATE {name} SET value = 'y' WHERE id = 2").collect()
        spark.sql(f"DELETE FROM {name} WHERE id = 3").collect()
        spark.sql(
            f"MERGE INTO {name} AS t USING (SELECT 6 AS id, 'z' AS value UNION ALL SELECT 7, 'g') AS s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        state = _row_tracking_state(path)
        expected = [(1, "x"), (2, "y"), (4, "d"), (5, "e"), (6, "z"), (7, "g")]
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert [(row.id, row.value) for row in rows] == expected
        assert len({row.row_id for row in rows}) == len(rows)
        assert max(row.row_id for row in rows) <= state["high_water_mark"]
        assert [(row.id, row.value) for row in spark.table(name).orderBy("id").collect()] == expected

        delta_jvm_spark.sql(f"INSERT INTO {table} VALUES (8, 'h')")
        rows = _jvm_row_tracking(delta_jvm_spark, path)
        assert rows[-1].row_id == state["high_water_mark"] + 1
        assert len({row.row_id for row in rows}) == len(rows)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()
