# ruff: noqa: S608

from pathlib import Path

import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyiceberg.manifest import DataFileContent, FileFormat, ManifestContent
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.steps.iceberg import (
    _current_deletion_vectors,
    _current_row_lineage,
    _current_snapshot,
    _find_latest_metadata,
    _latest_metadata_path,
)
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.utils import _current_manifest_entries, _local_file_path

MERGE_CONFIGS = [
    pytest.param((2, "copy-on-write"), id="v2-cow"),
    pytest.param((2, "merge-on-read"), id="v2-mor"),
    pytest.param((3, "copy-on-write"), id="v3-cow"),
    pytest.param((3, "merge-on-read"), id="v3-mor"),
]
MERGE_APIS = [
    "sql",
    pytest.param(
        "dataframe",
        marks=pytest.mark.skipif(
            not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+"
        ),
    ),
]
MERGE_SCHEMA = StructType([StructField("id", IntegerType()), StructField("dep", StringType())])
SOURCE_VIEW = "iceberg_merge_api_source"


@pytest.fixture(params=MERGE_CONFIGS)
def merge_table(spark, tmp_path, request):
    version, mode = request.param
    name = "iceberg_merge_api"
    path = tmp_path / name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, dep STRING) USING iceberg
            LOCATION '{escape_sql_string_literal(str(path))}'
            TBLPROPERTIES ('format-version' = '{version}', 'write.merge.mode' = '{mode}')""")
        yield name, path, version, mode
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {SOURCE_VIEW}")
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _table_files(path: Path) -> dict[Path, bytes]:
    return {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()}


def _assert_rows_and_schema(frame, expected):
    assert frame.schema == MERGE_SCHEMA
    assert [tuple(row) for row in frame.orderBy("id").collect()] == expected


def _merge_all_clauses(spark, name, source, api):
    matched_delete_id = 2
    target_update_id = 3
    target_delete_id = 4
    if api == "sql":
        spark.sql(f"""MERGE INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.id
            WHEN MATCHED AND t.id = 1 THEN UPDATE SET *
            WHEN MATCHED AND t.id = 2 THEN DELETE
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET dep = 'invalid'
            WHEN NOT MATCHED BY SOURCE AND t.id = 4 THEN DELETE""").collect()
    else:
        (
            source.alias("s")
            .mergeInto(name, F.col(f"{name}.id") == F.col("s.id"))
            .whenMatched(F.col(f"{name}.id") == 1)
            .updateAll()
            .whenMatched(F.col(f"{name}.id") == matched_delete_id)
            .delete()
            .whenNotMatched()
            .insertAll()
            .whenNotMatchedBySource(F.col(f"{name}.id") == target_update_id)
            .update({"dep": F.lit("invalid")})
            .whenNotMatchedBySource(F.col(f"{name}.id") == target_delete_id)
            .delete()
            .merge()
        )


@pytest.mark.parametrize("api", MERGE_APIS)
@pytest.mark.parametrize("initial_state", ["populated", "empty-source", "empty-target"])
def test_merge_all_clauses_aligns_reordered_columns(spark, merge_table, api, initial_state):
    name, path, version, mode = merge_table
    initial_rows = [(1, "old-one"), (2, "old-two"), (3, "old-three"), (4, "old-four"), (6, "keep")]
    if initial_state != "empty-target":
        spark.sql(f"""INSERT INTO {name} SELECT /*+ COALESCE(1) */ * FROM VALUES
            (1, 'old-one'), (2, 'old-two'), (3, 'old-three'), (4, 'old-four'), (6, 'keep')""")
    source_rows = [] if initial_state == "empty-source" else [(1, "new-one"), (2, "new-two"), (5, "new-five")]
    source = spark.createDataFrame(source_rows, MERGE_SCHEMA).select("dep", "id")
    source.createOrReplaceTempView(SOURCE_VIEW)
    before = _find_latest_metadata(path)
    before_entries = _current_manifest_entries(path, ManifestContent.DATA) if initial_state != "empty-target" else []
    before_paths = {entry.data_file.file_path for entry in before_entries}
    before_lineage = _current_row_lineage(path) if version == 3 and before_entries else {}  # noqa: PLR2004
    changed_ids = {3, 4} if initial_state == "empty-source" else {1, 2, 3, 4}
    expected_deletes = {
        (entry.data_file.file_path, position)
        for entry in before_entries
        for position, row in enumerate(pq.ParquetFile(_local_file_path(entry.data_file.file_path)).read().to_pylist())
        if row["id"] in changed_ids
    }

    _merge_all_clauses(spark, name, source, api)

    expected = {
        "populated": [(1, "new-one"), (3, "invalid"), (5, "new-five"), (6, "keep")],
        "empty-source": [(1, "old-one"), (2, "old-two"), (3, "invalid"), (6, "keep")],
        "empty-target": source_rows,
    }[initial_state]
    _assert_rows_and_schema(spark.table(name), expected)
    _assert_rows_and_schema(spark.read.format("iceberg").load(str(path)), expected)
    after = _find_latest_metadata(path)
    assert after["schemas"] == before["schemas"]
    assert after["current-schema-id"] == before["current-schema-id"]
    assert after["snapshots"][:-1] == before.get("snapshots", [])
    snapshot = _current_snapshot(after)
    assert snapshot["summary"]["operation"] == ("append" if initial_state == "empty-target" else "overwrite")
    if before_entries:
        assert snapshot["parent-snapshot-id"] == before["current-snapshot-id"]
        historical = spark.read.format("iceberg").option("snapshotId", before["current-snapshot-id"]).load(str(path))
        _assert_rows_and_schema(historical, initial_rows)

    after_entries = _current_manifest_entries(path, ManifestContent.DATA)
    after_paths = {entry.data_file.file_path for entry in after_entries}
    delete_entries = _current_manifest_entries(path, ManifestContent.DELETES)
    assert after_paths - before_paths
    if mode == "copy-on-write":
        assert before_paths - after_paths == {file for file, _ in expected_deletes}
        assert delete_entries == []
        assert sum(entry.data_file.record_count for entry in after_entries) == len(expected)
    else:
        assert before_paths <= after_paths
        if version == 2:  # noqa: PLR2004
            assert all(entry.data_file.content == DataFileContent.POSITION_DELETES for entry in delete_entries)
            assert all(entry.data_file.file_format == FileFormat.PARQUET for entry in delete_entries)
            actual_deletes = {
                (row["file_path"], row["pos"])
                for entry in delete_entries
                for row in pq.ParquetFile(_local_file_path(entry.data_file.file_path)).read().to_pylist()
            }
        else:
            assert all(entry.data_file.file_format == FileFormat.PUFFIN for entry in delete_entries)
            actual_deletes = {
                (file, position)
                for file, positions in _current_deletion_vectors(path).items()
                for position in positions
            }
        assert actual_deletes == expected_deletes

    if version == 3:  # noqa: PLR2004
        lineage = _current_row_lineage(path)
        assert set(lineage) == {row[0] for row in expected}
        previous_ids = {row_id for row_id, _ in before_lineage.values()}
        updated_ids = {3} if initial_state == "empty-source" else {1, 3}
        for key, (row_id, sequence) in lineage.items():
            if key not in before_lineage:
                assert row_id not in previous_ids
                assert sequence == snapshot["sequence-number"]
            elif key in updated_ids:
                assert row_id == before_lineage[key][0]
                assert sequence == snapshot["sequence-number"]
            else:
                assert (row_id, sequence) == before_lineage[key]


def _merge_stars(spark, name, source, api, *, schema_evolution=False):
    if api == "sql":
        evolution = "WITH SCHEMA EVOLUTION " if schema_evolution else ""
        spark.sql(f"""MERGE {evolution}INTO {name} AS t USING {SOURCE_VIEW} AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""").collect()
    else:
        merge = source.alias("s").mergeInto(name, F.col(f"{name}.id") == F.col("s.id"))
        if schema_evolution:
            merge = merge.withSchemaEvolution()
        merge.whenMatched().updateAll().whenNotMatched().insertAll().merge()


@pytest.mark.parametrize("api", MERGE_APIS)
def test_merge_cardinality_failure_preserves_table(spark, merge_table, api, record_property):
    name, path, _, _ = merge_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original'), (2, 'keep')")
    source = spark.createDataFrame([(1, "first"), (1, "second"), (3, "insert")], MERGE_SCHEMA)
    source.createOrReplaceTempView(SOURCE_VIEW)
    before_metadata = _find_latest_metadata(path)
    before_head = _latest_metadata_path(path)
    before_files = _table_files(path)

    with pytest.raises(Exception, match=r"MERGE_CARDINALITY_VIOLATION|Multiple source rows"):
        _merge_stars(spark, name, source, api)

    _assert_rows_and_schema(spark.table(name), [(1, "original"), (2, "keep")])
    assert _find_latest_metadata(path) == before_metadata
    assert _latest_metadata_path(path) == before_head
    after_files = _table_files(path)
    assert {file: after_files.get(file) for file in before_files} == before_files
    # A pre-commit failure may leave completed sibling-task output, but must not
    # publish metadata, alter existing artifacts, or reference the new files.
    new_files = after_files.keys() - before_files.keys()
    record_property("unreferenced_task_files", len(new_files))
    active_files = {
        _local_file_path(entry.data_file.file_path).resolve()
        for entry in _current_manifest_entries(path, ManifestContent.DATA)
    }
    for file in new_files:
        assert file.parts[0] == "data"
        assert file.suffix == ".parquet"
        assert (path / file).resolve() not in active_files
        rows = pq.ParquetFile(path / file).read(columns=["id", "dep"]).to_pylist()
        assert {(row["id"], row["dep"]) for row in rows} <= {(2, "keep"), (3, "insert")}


@pytest.mark.parametrize("api", MERGE_APIS)
def test_merge_schema_evolution_rejection_preserves_table(spark, merge_table, api):
    name, path, _, _ = merge_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame(
        [(1, "updated", "extra"), (2, "insert", "extra")], "id INT, dep STRING, extra STRING"
    )
    source.createOrReplaceTempView(SOURCE_VIEW)
    before_metadata = _find_latest_metadata(path)
    before_head = _latest_metadata_path(path)
    before_files = _table_files(path)

    with pytest.raises(Exception, match=r"Iceberg MERGE WITH SCHEMA EVOLUTION.*not supported"):
        _merge_stars(spark, name, source, api, schema_evolution=True)

    _assert_rows_and_schema(spark.table(name), [(1, "original")])
    assert _find_latest_metadata(path) == before_metadata
    assert _latest_metadata_path(path) == before_head
    assert _table_files(path) == before_files


@pytest.mark.skipif(not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+")
@pytest.mark.parametrize("action", ["matched", "target-only", "insert"])
def test_dataframe_merge_empty_assignments(spark, merge_table, action):
    name, path, _, _ = merge_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source_id = 1 if action == "matched" else 2
    source = spark.createDataFrame([(source_id, "unused")], MERGE_SCHEMA).alias("s")
    merge = source.mergeInto(name, F.col(f"{name}.id") == F.col("s.id"))
    if action == "matched":
        merge.whenMatched().update({}).merge()
    elif action == "target-only":
        merge.whenNotMatchedBySource().update({}).merge()
    else:
        merge.whenNotMatched().insert({}).merge()
    expected = [(None, None), (1, "original")] if action == "insert" else [(1, "original")]
    _assert_rows_and_schema(spark.table(name), expected)
    _assert_rows_and_schema(spark.read.format("iceberg").load(str(path)), expected)


@pytest.mark.skipif(not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+")
@pytest.mark.parametrize("merge_table", [MERGE_CONFIGS[2], MERGE_CONFIGS[3]], indirect=True)
@pytest.mark.parametrize("assignment", ["omitted", "default", "null"])
def test_dataframe_merge_distinguishes_defaults_from_null(spark, merge_table, assignment):
    name, path, _, _ = merge_table
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN dep SET DEFAULT 'fallback'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame([(1,), (2,)], "id INT").alias("s")
    update = (
        {}
        if assignment == "omitted"
        else {"dep": F.expr("DEFAULT") if assignment == "default" else F.lit(None).cast("string")}
    )
    insert = {"id": F.col("s.id"), **update}
    (
        source.mergeInto(name, F.col(f"{name}.id") == F.col("s.id"))
        .whenMatched()
        .update(update)
        .whenNotMatched()
        .insert(insert)
        .merge()
    )
    expected = {
        "omitted": [(1, "original"), (2, "fallback")],
        "default": [(1, "fallback"), (2, "fallback")],
        "null": [(1, None), (2, None)],
    }[assignment]
    _assert_rows_and_schema(spark.table(name), expected)
    _assert_rows_and_schema(spark.read.format("iceberg").load(str(path)), expected)


@pytest.mark.skipif(not hasattr(SparkDataFrame, "mergeInto"), reason="DataFrame.mergeInto requires Spark 4.0+")
@pytest.mark.parametrize("merge_table", [MERGE_CONFIGS[2], MERGE_CONFIGS[3]], indirect=True)
@pytest.mark.parametrize("action", ["update", "insert"])
def test_dataframe_merge_star_requires_defaulted_source_column(spark, merge_table, action):
    name, path, _, _ = merge_table
    spark.sql(f"ALTER TABLE {name} ALTER COLUMN dep SET DEFAULT 'fallback'")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'original')")
    source = spark.createDataFrame([(1,), (2,)], "id INT").alias("s")
    merge = source.mergeInto(name, F.col(f"{name}.id") == F.col("s.id"))
    merge = merge.whenMatched().updateAll() if action == "update" else merge.whenNotMatched().insertAll()
    before = _table_files(path)
    with pytest.raises(Exception, match=r"(?i)(not found|cannot resolve|unresolved)"):
        merge.merge()
    _assert_rows_and_schema(spark.table(name), [(1, "original")])
    assert _table_files(path) == before
