from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    from pathlib import Path


def _latest_add_stats(table_path: Path) -> dict:
    for log_file in sorted((table_path / "_delta_log").glob("*.json"), reverse=True):
        for line in log_file.read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if stats := action.get("add", {}).get("stats"):
                return json.loads(stats)
    message = f"add stats not found in {table_path / '_delta_log'}"
    raise AssertionError(message)


def test_count_non_null_containers_does_not_count_null_elements(spark, tmp_path: Path):
    table_path = tmp_path / "delta_container_null_count"
    schema = StructType(
        [
            StructField(
                "items",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "attributes",
                MapType(StringType(), IntegerType(), valueContainsNull=True),
                nullable=True,
            ),
        ]
    )
    source = spark.createDataFrame(
        [([None], {"a": None}), ([1], {"b": 1})],
        schema=schema,
    )
    source.write.format("delta").mode("overwrite").save(str(table_path))

    result = (
        spark.read.format("delta")
        .load(str(table_path))
        .selectExpr(
            "COUNT(items) AS item_count",
            "COUNT(attributes) AS attribute_count",
            "SUM(SIZE(items)) AS item_size",
            "SUM(SIZE(attributes)) AS attribute_size",
        )
        .collect()
    )
    assert result == [Row(item_count=2, attribute_count=2, item_size=2, attribute_size=2)]
    assert _latest_add_stats(table_path)["nullCount"] == {"items": 0, "attributes": 0}


def test_decimal_writer_stats_preserve_values_beyond_float_precision(spark, tmp_path: Path):
    table_path = tmp_path / "delta_decimal_writer_stats"
    schema = StructType([StructField("amount", DecimalType(18, 0), nullable=False)])
    source = spark.createDataFrame(
        [(Decimal(9007199254740993),), (Decimal(9007199254740995),)],
        schema=schema,
    )
    source.write.format("delta").mode("overwrite").save(str(table_path))

    stats = _latest_add_stats(table_path)
    assert stats["minValues"] == {"amount": 9007199254740993}
    assert stats["maxValues"] == {"amount": 9007199254740995}
