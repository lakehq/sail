# ruff: noqa: S608

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import BooleanType, IntegerType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_identifier

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration


def _latest_metadata(table_path: Path) -> dict:
    for log_file in sorted((table_path / "_delta_log").glob("*.json"), reverse=True):
        with log_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                action = json.loads(line)
                if "metaData" in action:
                    return action["metaData"]
    message = f"metadata action not found in {table_path / '_delta_log'}"
    raise AssertionError(message)


def _physical_name(metadata: dict, logical_name: str) -> str:
    schema = json.loads(metadata["schemaString"])
    field = next(field for field in schema["fields"] if field["name"] == logical_name)
    return field["metadata"]["delta.columnMapping.physicalName"]


def _latest_add_stats(table_path: Path) -> dict:
    for log_file in sorted((table_path / "_delta_log").glob("*.json"), reverse=True):
        with log_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                action = json.loads(line)
                if "add" in action:
                    return json.loads(action["add"]["stats"])
    message = f"add action not found in {table_path / '_delta_log'}"
    raise AssertionError(message)


def _nested_schema(value_field: str) -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), nullable=True),
            StructField(
                "details",
                StructType(
                    [
                        StructField(value_field, IntegerType(), nullable=True),
                        StructField("active", BooleanType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )


def test_sail_pruning_uses_only_physical_names_for_mapped_stats(
    delta_jvm_spark: SparkSession,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = tmp_path / "delta_spark_mapped_stats_name_collision"
    table_identifier = f"delta.`{escape_sql_identifier(str(table_path))}`"
    delta_jvm_spark.sql(
        f"""
        CREATE TABLE {table_identifier} (
            source_value BIGINT,
            target_value BIGINT
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.dataSkippingStatsColumns' = 'source_value'
        )
        """
    )
    metadata = _latest_metadata(table_path)
    source_physical_name = _physical_name(metadata, "source_value")
    target_physical_name = _physical_name(metadata, "target_value")
    assert source_physical_name != target_physical_name

    escaped_renamed_target = escape_sql_identifier(source_physical_name)
    delta_jvm_spark.sql(f"ALTER TABLE {table_identifier} RENAME COLUMN target_value TO `{escaped_renamed_target}`")
    delta_jvm_spark.sql(f"INSERT INTO {table_identifier} VALUES (0, 100)")

    renamed_metadata = _latest_metadata(table_path)
    assert _physical_name(renamed_metadata, source_physical_name) == target_physical_name
    stats = _latest_add_stats(table_path)
    for stats_field in ("minValues", "maxValues", "nullCount"):
        assert source_physical_name in stats[stats_field]
        assert target_physical_name not in stats[stats_field]

    renamed_target = F.col(f"`{source_physical_name}`")
    rows = (
        spark.read.format("delta")
        .load(str(table_path))
        .where(renamed_target > F.lit(50))
        .select(renamed_target.alias("target_value"))
        .collect()
    )
    assert [row.target_value for row in rows] == [100]


def test_delta_spark_nested_rename_is_readable_and_appendable_by_sail(
    delta_jvm_spark: SparkSession,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = tmp_path / "delta_spark_nested_mapping"
    table_identifier = f"delta.`{escape_sql_identifier(str(table_path))}`"
    delta_jvm_spark.sql(
        f"""
        CREATE TABLE {table_identifier} (
            id INT,
            details STRUCT<amount: INT, active: BOOLEAN>
        )
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        """
    )
    delta_jvm_spark.sql(
        f"""
        INSERT INTO {table_identifier}
        VALUES
            (1, named_struct('amount', 10, 'active', true)),
            (2, named_struct('amount', 20, 'active', false))
        """
    )
    data_files_before_rename = sorted(table_path.rglob("*.parquet"))

    delta_jvm_spark.sql(f"ALTER TABLE {table_identifier} RENAME COLUMN details.amount TO total")
    assert sorted(table_path.rglob("*.parquet")) == data_files_before_rename

    delta_jvm_spark.sql(
        f"""
        INSERT INTO {table_identifier}
        VALUES (3, named_struct('total', 30, 'active', true))
        """
    )

    frame = spark.read.format("delta").load(str(table_path))
    rows = frame.selectExpr("id", "details.total AS total", "details.active AS active").orderBy("id").collect()
    assert [(row.id, row.total, row.active) for row in rows] == [
        (1, 10, True),
        (2, 20, False),
        (3, 30, True),
    ]
    assert [row.id for row in frame.where("details.total >= 20").select("id").orderBy("id").collect()] == [2, 3]

    append = spark.createDataFrame(
        [Row(id=4, details=Row(total=40, active=False))],
        schema=_nested_schema("total"),
    )
    append.write.format("delta").mode("append").save(str(table_path))

    delta_jvm_spark.catalog.refreshByPath(str(table_path))
    rows = delta_jvm_spark.sql(
        f"""
        SELECT id, details.total AS total, details.active AS active
        FROM {table_identifier}
        ORDER BY id
        """
    ).collect()
    assert [(row.id, row.total, row.active) for row in rows] == [
        (1, 10, True),
        (2, 20, False),
        (3, 30, True),
        (4, 40, False),
    ]


def test_sail_nested_mapping_is_readable_and_appendable_by_delta_spark(
    delta_jvm_spark: SparkSession,
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = tmp_path / "sail_nested_mapping"
    table_identifier = f"delta.`{escape_sql_identifier(str(table_path))}`"
    source = spark.createDataFrame(
        [
            Row(id=1, details=Row(amount=10, active=True)),
            Row(id=2, details=Row(amount=20, active=False)),
        ],
        schema=_nested_schema("amount"),
    )
    (source.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(table_path)))

    rows = delta_jvm_spark.sql(
        f"""
        SELECT id, details.amount AS amount, details.active AS active
        FROM {table_identifier}
        ORDER BY id
        """
    ).collect()
    assert [(row.id, row.amount, row.active) for row in rows] == [
        (1, 10, True),
        (2, 20, False),
    ]
    filtered = delta_jvm_spark.sql(
        f"""
        SELECT id
        FROM {table_identifier}
        WHERE details.amount >= 20
        ORDER BY id
        """
    ).collect()
    assert [row.id for row in filtered] == [2]

    delta_jvm_spark.sql(
        f"""
        INSERT INTO {table_identifier}
        VALUES (3, named_struct('amount', 30, 'active', true))
        """
    )

    frame = spark.read.format("delta").load(str(table_path))
    rows = frame.selectExpr("id", "details.amount AS amount", "details.active AS active").orderBy("id").collect()
    assert [(row.id, row.amount, row.active) for row in rows] == [
        (1, 10, True),
        (2, 20, False),
        (3, 30, True),
    ]
