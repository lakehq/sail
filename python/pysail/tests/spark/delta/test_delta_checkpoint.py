from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc
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


def _write_checkpoint_replacement(checkpoint: Path, table: pa.Table) -> None:
    replacement = checkpoint.with_name(f"{checkpoint.stem}.replacement.parquet")
    pq.write_table(table, replacement)
    replacement.replace(checkpoint)


def _drop_null_checkpoint_action(checkpoint: Path, action_name: str) -> None:
    table = pq.read_table(checkpoint)
    action = table.column(action_name)
    assert action.null_count == table.num_rows
    _write_checkpoint_replacement(checkpoint, table.drop([action_name]))


def _drop_null_checkpoint_add_field(checkpoint: Path, field_name: str) -> None:
    table = pq.read_table(checkpoint)
    add_index = table.schema.get_field_index("add")
    add = table.column(add_index).combine_chunks()
    field_index = add.type.get_field_index(field_name)
    assert field_index >= 0
    assert add.field(field_index).null_count == len(add)

    fields = [field for field in add.type if field.name != field_name]
    columns = [add.field(index) for index, field in enumerate(add.type) if field.name != field_name]
    replacement_add = pa.StructArray.from_arrays(columns, fields=fields, mask=pc.is_null(add))
    table_columns = [table.column(index) for index in range(table.num_columns)]
    table_columns[add_index] = pa.chunked_array([replacement_add])
    table_fields = list(table.schema)
    table_fields[add_index] = pa.field("add", replacement_add.type, nullable=True)
    _write_checkpoint_replacement(
        checkpoint,
        pa.Table.from_arrays(
            table_columns,
            schema=pa.schema(table_fields, metadata=table.schema.metadata),
        ),
    )


def _require_legacy_partition_values_parsed(checkpoint: Path) -> None:
    table = pq.read_table(checkpoint)
    add_index = table.schema.get_field_index("add")
    add = table.column(add_index).combine_chunks()
    parsed_index = add.type.get_field_index("partitionValues_parsed")
    assert parsed_index >= 0

    fields = list(add.type)
    parsed_field = fields[parsed_index]
    fields[parsed_index] = pa.field(
        parsed_field.name,
        parsed_field.type,
        nullable=False,
        metadata=parsed_field.metadata,
    )
    columns = [add.field(index) for index in range(len(add.type))]
    parsed = columns[parsed_index]
    columns[parsed_index] = pa.array(
        [{} if value is None else value for value in parsed.to_pylist()],
        type=parsed.type,
    )
    replacement_add = pa.StructArray.from_arrays(columns, fields=fields, mask=pc.is_null(add))

    table_columns = [table.column(index) for index in range(table.num_columns)]
    table_columns[add_index] = pa.chunked_array([replacement_add])
    table_fields = list(table.schema)
    table_fields[add_index] = pa.field("add", replacement_add.type, nullable=True)
    _write_checkpoint_replacement(
        checkpoint,
        pa.Table.from_arrays(
            table_columns,
            schema=pa.schema(table_fields, metadata=table.schema.metadata),
        ),
    )
    assert not pq.read_schema(checkpoint).field("add").type.field("partitionValues_parsed").nullable


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


def test_legacy_nonnullable_parsed_checkpoint_replays_json_tail(spark, tmp_path: Path):
    table_path = tmp_path / "delta_checkpoint_legacy_nonnullable_parsed"
    _write_partitioned_history_through_json_commit(spark, table_path)

    _require_legacy_partition_values_parsed(_checkpoint_file(table_path, 2))

    replayed_ids = [
        row.id
        for row in (
            spark.read.format("delta")
            .option("metadataAsDataRead", "true")
            .load(str(table_path))
            .orderBy("id")
            .collect()
        )
    ]
    assert replayed_ids == [1, 2, 3, 4, 5]

    replacement = spark.createDataFrame([Row(id=10, category="A", value=100)])
    (replacement.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(table_path)))
    actual_ids = [row.id for row in spark.read.format("delta").load(str(table_path)).orderBy("id").collect()]
    assert actual_ids == [2, 4, 5, 10]


@pytest.mark.parametrize("omitted_action", ["add", "remove"])
def test_sparse_checkpoint_action_schema_replays_json_tail(
    spark,
    tmp_path: Path,
    omitted_action: str,
):
    table_path = tmp_path / f"delta_checkpoint_missing_{omitted_action}"
    table_name = f"delta_checkpoint_missing_{omitted_action}"
    location = escape_sql_string_literal(str(table_path))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, category STRING)
            USING DELTA
            PARTITIONED BY (category)
            LOCATION '{location}'
            TBLPROPERTIES ('delta.checkpointInterval' = '2')
            """
        ).collect()
        if omitted_action == "add":
            spark.sql(f"INSERT INTO {table_name} VALUES (1, 'A'), (2, 'B')")  # noqa: S608
            spark.sql(f"DELETE FROM {table_name} WHERE TRUE")  # noqa: S608
            spark.sql(f"INSERT INTO {table_name} VALUES (3, 'C')")  # noqa: S608
            expected_ids = [3]
        else:
            spark.sql(f"INSERT INTO {table_name} VALUES (1, 'A')")  # noqa: S608
            spark.sql(f"INSERT INTO {table_name} VALUES (2, 'B'), (3, 'C')")  # noqa: S608
            spark.sql(f"DELETE FROM {table_name} WHERE id = 1")  # noqa: S608
            expected_ids = [2, 3]
        spark.sql(f"DROP TABLE {table_name}").collect()

        _drop_null_checkpoint_action(_checkpoint_file(table_path, 2), omitted_action)

        driver_ids = [row.id for row in spark.read.format("delta").load(str(table_path)).orderBy("id").collect()]
        metadata_ids = [
            row.id
            for row in (
                spark.read.format("delta")
                .option("metadataAsDataRead", "true")
                .load(str(table_path))
                .orderBy("id")
                .collect()
            )
        ]
        assert driver_ids == expected_ids
        assert metadata_ids == expected_ids
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


@pytest.mark.parametrize("write_stats_as_struct", ["false", "true"])
def test_checkpoint_without_json_stats_supports_filter_and_delete(
    spark,
    tmp_path: Path,
    write_stats_as_struct: str,
):
    suffix = write_stats_as_struct
    table_path = tmp_path / f"delta_checkpoint_without_json_stats_{suffix}"
    table_name = f"delta_checkpoint_without_json_stats_{suffix}"
    location = escape_sql_string_literal(str(table_path))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, category STRING, value BIGINT)
            USING DELTA
            PARTITIONED BY (category)
            LOCATION '{location}'
            TBLPROPERTIES (
              'delta.checkpointInterval' = '2',
              'delta.checkpoint.writeStatsAsJson' = 'false',
              'delta.checkpoint.writeStatsAsStruct' = '{suffix}'
            )
            """
        ).collect()
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'A', 10)")  # noqa: S608
        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'B', 20)")  # noqa: S608
        checkpoint = _checkpoint_file(table_path, 2)
        add_fields = {field.name for field in pq.read_schema(checkpoint).field("add").type}
        assert "stats" not in add_fields
        assert ("stats_parsed" in add_fields) is (write_stats_as_struct == "true")
        spark.sql(f"INSERT INTO {table_name} VALUES (3, 'C', 30)")  # noqa: S608
        spark.sql(f"DROP TABLE {table_name}").collect()

        filtered_ids = [
            row.id
            for row in (
                spark.read.format("delta")
                .option("metadataAsDataRead", "true")
                .load(str(table_path))
                .where("value >= 20")
                .orderBy("id")
                .collect()
            )
        ]
        assert filtered_ids == [2, 3]

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{location}'").collect()
        spark.sql(f"DELETE FROM {table_name} WHERE value = 20")  # noqa: S608
        remaining_ids = [row.id for row in spark.table(table_name).orderBy("id").collect()]
        assert remaining_ids == [1, 3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


def test_deletion_vector_transition_after_checkpoint_preserves_json_field(
    spark,
    tmp_path: Path,
):
    table_path = tmp_path / "delta_checkpoint_deletion_vector_transition"
    table_name = "delta_checkpoint_deletion_vector_transition"
    location = escape_sql_string_literal(str(table_path))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, value STRING)
            USING DELTA
            LOCATION '{location}'
            TBLPROPERTIES ('delta.checkpointInterval' = '2')
            """
        ).collect()
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'A'), (2, 'B'), (3, 'C')")  # noqa: S608
        spark.sql(f"INSERT INTO {table_name} VALUES (4, 'D')")  # noqa: S608
        checkpoint = _checkpoint_file(table_path, 2)
        _drop_null_checkpoint_add_field(checkpoint, "deletionVector")

        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES "
            "('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '10')"
        ).collect()
        spark.sql(f"DELETE FROM {table_name} WHERE id = 2")  # noqa: S608

        delete_commit = table_path / "_delta_log" / "00000000000000000004.json"
        actions = [json.loads(line) for line in delete_commit.read_text(encoding="utf-8").splitlines()]
        assert any(action.get("add", {}).get("deletionVector") for action in actions)

        driver_ids = [row.id for row in spark.table(table_name).orderBy("id").collect()]
        metadata_ids = [
            row.id
            for row in (
                spark.read.format("delta")
                .option("metadataAsDataRead", "true")
                .load(str(table_path))
                .orderBy("id")
                .collect()
            )
        ]
        assert driver_ids == [1, 3, 4]
        assert metadata_ids == [1, 3, 4]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
