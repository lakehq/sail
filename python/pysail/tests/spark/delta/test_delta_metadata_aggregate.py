from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.tests.spark.delta.test_delta_external_log_stats import _write_external_delta_table

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


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", [False, True], ids=["driver", "metadata"])
@pytest.mark.parametrize(
    ("groups", "cast", "expected"),
    [
        pytest.param([[2, 3, 10, None]], "CAST(v AS STRING)", (4, 3, "10", "3"), id="varying-numbers"),
        pytest.param([[2, 2], [3, 3], [10, 10]], "CAST(v AS STRING)", (6, 6, "10", "3"), id="constant-files"),
        pytest.param([["2", "3", "10", None]], "CAST(v AS INT)", (4, 3, 2, 10), id="numeric-strings"),
        pytest.param([[0, 127, 128, None]], "TRY_CAST(v AS TINYINT)", (4, 2, 0, 127), id="mixed-overflow"),
        pytest.param([[128, 128]], "TRY_CAST(v AS TINYINT)", (2, 0, None, None), id="constant-overflow"),
        pytest.param([["10", "2", "bad", None]], "TRY_CAST(v AS INT)", (4, 2, 2, 10), id="invalid-string"),
    ],
)
def test_cast_aggregates_preserve_order_and_null_counts(
    spark, tmp_path, metadata_as_data, groups, cast, expected, snapshot
):
    table_path = tmp_path / "delta_cast_aggregates"
    strings = isinstance(groups[0][0], str)
    schema = StructType([StructField("v", StringType() if strings else LongType())])
    files = []
    for values in groups:
        present = [value for value in values if value is not None]
        stats = {
            "numRecords": len(values),
            "minValues": {"v": min(present)},
            "maxValues": {"v": max(present)},
            "nullCount": {"v": len(values) - len(present)},
            "tightBounds": True,
        }
        table = pa.table({"v": pa.array(values, type=pa.string() if strings else pa.int64())})
        files.append((table, stats))
    _write_external_delta_table(table_path, schema, files)
    frame = spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower()).load(str(table_path))

    expressions = [f"COUNT({cast}) AS present", f"MIN({cast}) AS minimum", f"MAX({cast}) AS maximum"]
    plans = {}
    for name, projection, result in [
        ("extrema", expressions[1:], expected[2:]),
        ("count", expressions[:1], (expected[1],)),
        ("combined", ["COUNT(*) AS rows", *expressions], expected),
    ]:
        query = frame.selectExpr(*projection)
        assert [tuple(row) for row in query.collect()] == [result]
        plans[name] = normalize_plan_text(query._explain_string())  # noqa: SLF001
    assert plans == snapshot


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", [False, True], ids=["driver", "metadata"])
def test_partition_cast_extrema_preserve_lexical_order(spark, tmp_path, metadata_as_data, snapshot):
    table_path = tmp_path / "delta_partition_cast"
    source = spark.createDataFrame([(2, 0), (2, 1), (3, 2), (3, 3), (10, 4), (10, 5), (None, 6)], "v LONG, payload INT")
    source.coalesce(1).write.format("delta").partitionBy("v").save(str(table_path))
    frame = spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower()).load(str(table_path))

    expressions = ["MIN(CAST(v AS STRING)) AS minimum", "MAX(CAST(v AS STRING)) AS maximum"]
    extrema = frame.selectExpr(*expressions)
    combined = frame.selectExpr("COUNT(*) AS rows", "COUNT(v) AS present", *expressions)
    assert extrema.collect() == [Row(minimum="10", maximum="3")]
    assert combined.collect() == [Row(rows=7, present=6, minimum="10", maximum="3")]
    assert {
        "extrema": normalize_plan_text(extrema._explain_string()),  # noqa: SLF001
        "combined": normalize_plan_text(combined._explain_string()),  # noqa: SLF001
    } == snapshot
