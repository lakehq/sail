from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from pathlib import Path


def _read_first_commit_metadata(table_path: Path) -> dict:
    log_file = table_path / "_delta_log" / "00000000000000000000.json"
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "metaData" in obj:
                return obj["metaData"]
    msg = f"metaData action not found in first delta log: {log_file}"
    raise AssertionError(msg)


def _checkpoint_file(table_path: Path, version: int) -> Path:
    matches = sorted((table_path / "_delta_log").glob(f"{version:020d}.checkpoint*.parquet"))
    assert len(matches) == 1, f"expected one checkpoint for version {version}, found {matches}"
    return matches[0]


def _write_partitioned_history_through_json_commit(spark, table_path: Path) -> None:
    initial = spark.createDataFrame(
        [
            Row(id=1, category="A", value=10),
            Row(id=2, category="B", value=20),
        ]
    )
    (
        initial.write.format("delta")
        .mode("overwrite")
        .option("delta.checkpointInterval", "2")
        .option("delta.checkpoint.writeStatsAsStruct", "true")
        .partitionBy("category")
        .save(str(table_path))
    )

    for row in [
        Row(id=3, category="A", value=30),
        Row(id=4, category="B", value=40),
        Row(id=5, category="C", value=50),
    ]:
        spark.createDataFrame([row]).write.format("delta").mode("append").save(str(table_path))

    _checkpoint_file(table_path, 2)
    assert (table_path / "_delta_log" / "00000000000000000003.json").is_file()


def _assert_struct_tree_nullable(field: pa.Field, path: str) -> None:
    assert field.nullable, f"checkpoint field {path} must be nullable"
    if pa.types.is_struct(field.type):
        for child in field.type:
            _assert_struct_tree_nullable(child, f"{path}.{child.name}")


def test_delta_write_to_table_properties_materialize_metadata(spark, tmp_path: Path):
    """Test that .writeTo().tableProperty() materializes Delta table properties on first commit.

    This exercises the DataFrame API code path (.writeTo + .tableProperty), which is distinct
    from the SQL DDL path (TBLPROPERTIES) covered by checkpoint_properties.feature.
    """
    base = tmp_path / "delta_write_to_checkpoint"
    table_name = "delta_write_to_checkpoint"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.createDataFrame([Row(id=1), Row(id=2)]).writeTo(table_name).using("delta").option(
            "path",
            str(base),
        ).tableProperty("delta.checkpointInterval", "3").create()

        actual = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert [row.id for row in actual] == [1, 2]

        metadata = _read_first_commit_metadata(base)
        assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "3"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_delta_write_option_routes_table_property_to_metadata(spark, tmp_path: Path):
    """Test that Delta table properties can be passed through DataFrame write options.

    This covers the path-based DataFrame API (`.write.option(...).save(...)`), where Delta table
    properties arrive mixed in with transient write options and must be routed into the first
    `metaData.configuration` action for a new table.
    """
    base = tmp_path / "delta_write_option_checkpoint"

    df = spark.createDataFrame([Row(id=1), Row(id=2)])
    (df.write.format("delta").mode("overwrite").option("delta.checkpointInterval", "3").save(str(base)))

    actual = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [row.id for row in actual] == [1, 2]

    metadata = _read_first_commit_metadata(base)
    assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "3"


def test_partitioned_overwrite_after_checkpoint_and_json_commit(spark, tmp_path: Path):
    table_path = tmp_path / "delta_checkpoint_partitioned_overwrite"

    def overwrite(version: int) -> None:
        row = Row(id=version, category=f"category-{version % 2}", value=version * 10)
        writer = spark.createDataFrame([row]).write.format("delta").mode("overwrite").partitionBy("category")
        if version == 0:
            writer = writer.option("delta.checkpointInterval", "2").option(
                "delta.checkpoint.writeStatsAsStruct", "true"
            )
        writer.save(str(table_path))

    for version in range(4):
        overwrite(version)

    _checkpoint_file(table_path, 2)
    assert (table_path / "_delta_log" / "00000000000000000003.json").is_file()

    overwrite(4)

    rows = spark.read.format("delta").load(str(table_path)).select("id", "category", "value").collect()
    assert rows == [Row(id=4, category="category-0", value=40)]


@pytest.mark.parametrize("mutation", ["replace_where", "delete"])
def test_partitioned_mutation_after_checkpoint_and_json_commit(spark, tmp_path: Path, mutation: str):
    table_path = tmp_path / f"delta_checkpoint_{mutation}"
    table_name = f"delta_checkpoint_{mutation}"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    try:
        _write_partitioned_history_through_json_commit(spark, table_path)

        if mutation == "replace_where":
            replacement = spark.createDataFrame([Row(id=10, category="A", value=100)])
            (
                replacement.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", "category = 'A'")
                .save(str(table_path))
            )
            expected_ids = [2, 4, 5, 10]
        else:
            location = escape_sql_string_literal(str(table_path))
            spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{location}'").collect()
            spark.sql(f"DELETE FROM {table_name} WHERE category = 'A'")  # noqa: S608
            expected_ids = [2, 4, 5]

        actual_ids = [row.id for row in spark.read.format("delta").load(str(table_path)).orderBy("id").collect()]
        assert actual_ids == expected_ids
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


def test_checkpoint_parsed_fields_are_recursively_nullable(spark, tmp_path: Path):
    table_path = tmp_path / "delta_checkpoint_nullable_parsed_fields"
    table_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("value", LongType(), nullable=False),
        ]
    )

    for version in range(3):
        frame = spark.createDataFrame(
            [(version, f"category-{version % 2}", version * 10)],
            schema=table_schema,
        )
        writer = frame.write.format("delta").mode("overwrite" if version == 0 else "append")
        if version == 0:
            writer = (
                writer.option("delta.checkpointInterval", "2")
                .option("delta.checkpoint.writeStatsAsStruct", "true")
                .partitionBy("category")
            )
        writer.save(str(table_path))

    checkpoint = pq.read_table(_checkpoint_file(table_path, 2))
    add_field = checkpoint.schema.field("add")
    stats_parsed = add_field.type.field("stats_parsed")
    partition_values_parsed = add_field.type.field("partitionValues_parsed")

    _assert_struct_tree_nullable(stats_parsed, "add.stats_parsed")
    _assert_struct_tree_nullable(partition_values_parsed, "add.partitionValues_parsed")

    add_array = checkpoint.column("add").combine_chunks()
    partition_values_array = add_array.field(add_array.type.get_field_index("partitionValues_parsed"))
    assert add_array.null_count > 0
    assert partition_values_array.null_count >= add_array.null_count
