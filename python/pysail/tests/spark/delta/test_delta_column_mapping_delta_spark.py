# ruff: noqa: S608

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row
from pyspark.sql.types import BooleanType, IntegerType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_identifier

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration


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
