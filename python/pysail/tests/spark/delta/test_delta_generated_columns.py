import json
import time
import uuid
from pathlib import Path

import pytest


def _create_empty_generated_column_table(table_path: Path) -> None:
    now_ms = int(time.time() * 1000)
    log_dir = table_path / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)

    schema = {
        "type": "struct",
        "fields": [
            {"name": "some_id", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "some_category", "type": "string", "nullable": True, "metadata": {}},
            {
                "name": "gen",
                "type": "integer",
                "nullable": True,
                "metadata": {"delta.generationExpression": "some_id * 2"},
            },
        ],
    }
    actions = [
        {
            "commitInfo": {
                "timestamp": now_ms,
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "isBlindAppend": True,
            }
        },
        {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 7,
                "writerFeatures": ["generatedColumns"],
            }
        },
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema, separators=(",", ":")),
                "partitionColumns": ["some_category"],
                "configuration": {},
                "createdTime": now_ms,
            }
        },
    ]

    with (log_dir / "00000000000000000000.json").open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action, separators=(",", ":")))
            f.write("\n")


def test_dataframewriter_path_append_computes_omitted_generated_column(spark, tmp_path):
    delta_path = tmp_path / "delta_generated_columns"
    _create_empty_generated_column_table(delta_path)

    spark.createDataFrame(
        [(1, "A"), (2, "B")],
        "some_id INT, some_category STRING",
    ).write.format("delta").mode("append").save(str(delta_path))
    spark.createDataFrame(
        [(3, 6, "C")],
        "some_id INT, gen INT, some_category STRING",
    ).write.format("delta").mode("append").save(str(delta_path))

    rows = spark.read.format("delta").load(str(delta_path)).orderBy("some_id").collect()

    assert [row.asDict() for row in rows] == [
        {"some_id": 1, "gen": 2, "some_category": "A"},
        {"some_id": 2, "gen": 4, "some_category": "B"},
        {"some_id": 3, "gen": 6, "some_category": "C"},
    ]


def test_dataframewriter_path_append_rejects_generated_column_mismatch(spark, tmp_path):
    delta_path = tmp_path / "delta_generated_columns_mismatch"
    _create_empty_generated_column_table(delta_path)

    df = spark.createDataFrame(
        [(1, 999, "A")],
        "some_id INT, gen INT, some_category STRING",
    )

    with pytest.raises(Exception, match="DELTA_GENERATED_COLUMNS_VALUE_MISMATCH"):
        df.write.format("delta").mode("append").save(str(delta_path))


def test_dataframewriter_path_overwrite_computes_omitted_generated_column(spark, tmp_path):
    delta_path = tmp_path / "delta_generated_columns_overwrite"
    _create_empty_generated_column_table(delta_path)

    spark.createDataFrame(
        [(1, "A"), (2, "B")],
        "some_id INT, some_category STRING",
    ).write.format("delta").mode("overwrite").save(str(delta_path))

    rows = spark.read.format("delta").load(str(delta_path)).orderBy("some_id").collect()

    assert [row.asDict() for row in rows] == [
        {"some_id": 1, "gen": 2, "some_category": "A"},
        {"some_id": 2, "gen": 4, "some_category": "B"},
    ]


def test_dataframewriter_path_overwrite_rejects_generated_column_mismatch(spark, tmp_path):
    delta_path = tmp_path / "delta_generated_columns_overwrite_mismatch"
    _create_empty_generated_column_table(delta_path)

    df = spark.createDataFrame(
        [(1, 999, "A")],
        "some_id INT, gen INT, some_category STRING",
    )

    with pytest.raises(Exception, match="DELTA_GENERATED_COLUMNS_VALUE_MISMATCH"):
        df.write.format("delta").mode("overwrite").save(str(delta_path))


def test_dataframewriter_path_overwrite_schema_does_not_rewrite_old_generated_column(spark, tmp_path):
    delta_path = tmp_path / "delta_generated_columns_overwrite_schema"
    _create_empty_generated_column_table(delta_path)

    spark.createDataFrame(
        [(1, "A")],
        "some_id INT, some_category STRING",
    ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(delta_path))

    df = spark.read.format("delta").load(str(delta_path))

    assert df.schema.fieldNames() == ["some_id", "some_category"]
    assert [row.asDict() for row in df.collect()] == [
        {"some_id": 1, "some_category": "A"},
    ]
