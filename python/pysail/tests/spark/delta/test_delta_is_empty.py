from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import IntegerType, StructField, StructType

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _write_row_count_table(table_path, file_rows, *, record_counts=True):
    table_path.mkdir()
    schema = StructType([StructField("id", IntegerType(), nullable=True)])
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "row-count-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.json(),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0,
            }
        },
    ]
    for index, rows in enumerate(file_rows):
        data_file = table_path / f"part-{index}.parquet"
        pq.write_table(pa.table({"id": pa.array(rows, type=pa.int32())}), data_file)
        add = {
            "path": data_file.name,
            "partitionValues": {},
            "size": data_file.stat().st_size,
            "modificationTime": 0,
            "dataChange": True,
        }
        include_count = record_counts if isinstance(record_counts, bool) else record_counts[index]
        if include_count:
            add["stats"] = json.dumps({"numRecords": len(rows)})
        actions.append({"add": add})
    log_dir = table_path / "_delta_log"
    log_dir.mkdir()
    (log_dir / "00000000000000000000.json").write_text(
        "\n".join(json.dumps(action) for action in actions), encoding="utf-8"
    )


@pytest.mark.parametrize("record_counts", [True, False], ids=["exact-stats", "missing-stats"])
@pytest.mark.parametrize(
    "file_rows",
    [[], [[]], [[], [1, 2, 3]]],
    ids=["empty-snapshot", "empty-file", "later-nonempty-file"],
)
def test_is_empty_and_empty_projection_limit(spark, tmp_path, file_rows, record_counts):
    table_path = tmp_path / "row_counts"
    _write_row_count_table(table_path, file_rows, record_counts=record_counts)
    expected_count = sum(map(len, file_rows))
    df = spark.read.format("delta").load(str(table_path))

    assert df.isEmpty() == (expected_count == 0)
    limited = df.select().limit(1)
    assert limited.schema == StructType([])
    assert limited.collect() == ([()] if expected_count else [])
    assert df.select().limit(2).collect() == [()] * min(expected_count, 2)


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("partial", [False, True], ids=["complete-counts", "partial-counts"])
def test_empty_projection_limit_uses_sufficient_known_rows(spark, tmp_path, snapshot, partial):
    table_path = tmp_path / "bounded_row_counts"
    _write_row_count_table(table_path, [[], [1, 2, 3]], record_counts=[not partial, True])
    for data_file in table_path.glob("*.parquet"):
        data_file.unlink()
    frame = spark.read.format("delta").load(str(table_path))
    # Spark Connect 3.5's isEmpty() retains columns unless explicitly projected away.
    assert frame.select().isEmpty() is False
    plans = {}
    for count in [0, 1, 2, 3]:
        limited = frame.select().limit(count)
        assert limited.collect() == [()] * count
        plans[f"limit_{count}"] = normalize_plan_text(limited._explain_string())  # noqa: SLF001
    offset = frame.select().offset(1).limit(2)
    assert offset.collect() == [(), ()]
    plans["offset"] = normalize_plan_text(offset._explain_string())  # noqa: SLF001
    if not partial:
        past_end = frame.select().limit(10)
        assert past_end.collect() == [(), (), ()]
        plans["limit_past_end"] = normalize_plan_text(past_end._explain_string())  # noqa: SLF001
        offset_past_end = frame.select().offset(4).limit(2)
        assert offset_past_end.collect() == []
        plans["offset_past_end"] = normalize_plan_text(offset_past_end._explain_string())  # noqa: SLF001
    else:
        with pytest.raises(Exception, match=r"(?i)(not found|no such file)"):
            frame.select().limit(4).collect()
    assert plans == snapshot


@pytest.mark.yamlsnapshot(group="plan")
def test_empty_projection_reads_unknown_files_when_counts_are_insufficient(spark, tmp_path, snapshot):
    table_path = tmp_path / "insufficient_row_counts"
    _write_row_count_table(table_path, [[], [1, 2, 3]], record_counts=[True, False])
    frame = spark.read.format("delta").load(str(table_path))
    assert frame.isEmpty() is False
    limited = frame.select().limit(2)
    assert limited.collect() == [(), ()]
    assert normalize_plan_text(limited._explain_string()) == snapshot  # noqa: SLF001


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("count", [1, 2, 3])
def test_limit_reads_only_enough_known_files(spark, tmp_path, snapshot, count):
    table_path = tmp_path / "limited_files"
    _write_row_count_table(table_path, [[99], [], [1, 2], [3]], record_counts=[False, True, True, True])
    (table_path / "part-0.parquet").unlink()
    (table_path / "part-1.parquet").unlink()
    limited = spark.read.format("delta").load(str(table_path)).limit(count)
    rows = limited.collect()
    assert len(rows) == count
    assert all(row.id in [1, 2, 3] for row in rows)
    assert normalize_plan_text(limited._explain_string()) == snapshot  # noqa: SLF001


@pytest.mark.yamlsnapshot(group="plan")
def test_limit_preserves_row_filters_and_ordering(spark, tmp_path, snapshot):
    table_path = tmp_path / "filtered_limit"
    _write_row_count_table(table_path, [[1, 2], [3, 4]])
    frame = spark.read.format("delta").load(str(table_path))
    filtered = frame.where("id > 2").limit(2)
    assert sorted(row.id for row in filtered.collect()) == [3, 4]
    ordered = frame.orderBy("id", ascending=False).limit(2)
    assert ordered.collect() == [(4,), (3,)]
    assert {
        "filtered": normalize_plan_text(filtered._explain_string()),  # noqa: SLF001
        "ordered": normalize_plan_text(ordered._explain_string()),  # noqa: SLF001
    } == snapshot


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("record_counts", [True, False], ids=["exact-stats", "missing-stats"])
def test_empty_projection_limit_plan(spark, tmp_path, snapshot, record_counts):
    table_path = tmp_path / "row_counts"
    _write_row_count_table(table_path, [[1, 2, 3]], record_counts=record_counts)
    limited = spark.read.format("delta").load(str(table_path)).select().limit(1)

    assert limited.collect() == [()]
    assert normalize_plan_text(limited._explain_string()) == snapshot  # noqa: SLF001


@pytest.mark.parametrize("metadata_as_data", [False, True], ids=["eager", "replay"])
def test_is_empty_preserves_filters_and_replay(spark, tmp_path, metadata_as_data):
    table_path = tmp_path / "filtered"
    _write_row_count_table(table_path, [[], [1, 2, 3]])
    df = spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower()).load(str(table_path))

    assert df.isEmpty() is False
    assert df.where("id > 3").isEmpty() is True
    assert df.where("id = 2").isEmpty() is False
    assert df.where("id > 3").select().limit(1).collect() == []
    assert df.where("id = 2").select().limit(1).collect() == [()]
    assert df.orderBy("id").select().limit(1).collect() == [()]
    assert df.orderBy("id").limit(1).collect() == [(1,)]


def test_is_empty_uses_current_snapshot_after_deletion_vectors(spark, tmp_path):
    table_path = tmp_path / "deletion_vectors"
    table_location = escape_sql_string_literal(str(table_path))
    table_name = "delta_is_empty_dv"
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT) USING DELTA
            LOCATION '{table_location}'
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1), (2), (3)")  # noqa: S608
        spark.sql(f"DELETE FROM {table_name} WHERE id = 1")  # noqa: S608
        actions = [
            json.loads(line)
            for line in (table_path / "_delta_log" / "00000000000000000002.json").read_text().splitlines()
        ]
        add = next(action["add"] for action in actions if action.get("add", {}).get("deletionVector"))
        expected_physical_rows = 3
        assert json.loads(add["stats"])["numRecords"] == expected_physical_rows
        assert add["deletionVector"]["cardinality"] == 1

        df = spark.table(table_name)
        assert df.isEmpty() is False
        assert df.select().limit(1).collect() == [()]
        assert df.where("id = 1").isEmpty() is True

        spark.sql(f"DELETE FROM {table_name} WHERE id > 1")  # noqa: S608
        assert spark.table(table_name).isEmpty() is True
        assert spark.table(table_name).select().limit(1).collect() == []
        previous = spark.read.format("delta").option("versionAsOf", "2").load(str(table_path))
        assert previous.isEmpty() is False
        assert previous.select().limit(1).collect() == [()]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
