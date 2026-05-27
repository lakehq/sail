import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.window import Window


def _write_partition_first_delta_table(table_path):
    data_dir = table_path / "load_date=2023-01-03"
    data_dir.mkdir(parents=True)
    data_file = data_dir / "part-00000-external-stats-c000.snappy.parquet"
    table = pa.table(
        {
            "id": pa.array(["2", "3"], type=pa.string()),
            "payload_column_1": pa.array([22, 3], type=pa.int32()),
            "some_col": pa.array(["foo", "foo"], type=pa.string()),
        }
    )
    pq.write_table(table, data_file, compression="snappy")

    delta_log_dir = table_path / "_delta_log"
    delta_log_dir.mkdir()
    schema = {
        "type": "struct",
        "fields": [
            {"name": "load_date", "type": "date", "nullable": True, "metadata": {}},
            {"name": "id", "type": "string", "nullable": True, "metadata": {}},
            {
                "name": "payload_column_1",
                "type": "integer",
                "nullable": True,
                "metadata": {},
            },
            {"name": "some_col", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    add_stats = {
        "numRecords": 2,
        "minValues": {"some_col": "foo", "payload_column_1": 3, "id": "2"},
        "maxValues": {"id": "3", "some_col": "foo", "payload_column_1": 22},
        "nullCount": {"some_col": 0, "payload_column_1": 0, "id": 0},
    }
    actions = [
        {
            "commitInfo": {
                "timestamp": 0,
                "operation": "WRITE",
                "operationParameters": {
                    "mode": "Overwrite",
                    "partitionBy": json.dumps(["load_date"]),
                },
                "engineInfo": "delta-rs:py-regression",
                "clientVersion": "delta-rs.py-regression",
            }
        },
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "external-delta-stats-regression",
                "name": None,
                "description": None,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema, separators=(",", ":")),
                "partitionColumns": ["load_date"],
                "createdTime": 0,
                "configuration": {},
            }
        },
        {
            "add": {
                "path": data_file.relative_to(table_path).as_posix(),
                "partitionValues": {"load_date": "2023-01-03"},
                "size": data_file.stat().st_size,
                "modificationTime": 0,
                "dataChange": True,
                "stats": json.dumps(add_stats, separators=(",", ":")),
            }
        },
    ]
    log_file = delta_log_dir / "00000000000000000000.json"
    log_file.write_text(
        "".join(f"{json.dumps(action, separators=(',', ':'))}\n" for action in actions),
        encoding="utf-8",
    )


def test_window_dedup_reads_external_delta_stats_with_partition_first_schema(spark, tmp_path):
    table_path = tmp_path / "delta_partition_first_stats"
    _write_partition_first_delta_table(table_path)

    df = spark.read.format("delta").load(str(table_path))
    assert df.columns == ["load_date", "id", "payload_column_1", "some_col"]

    deduplicated = (
        df.withColumn(
            "_row_number_",
            F.row_number().over(Window.partitionBy("id", "payload_column_1").orderBy(F.lit(1))),
        )
        .filter(F.col("_row_number_") == 1)
        .drop("_row_number_")
    )

    rows = [(row.id, row.payload_column_1, row.some_col, row.load_date) for row in deduplicated.orderBy("id").collect()]
    assert rows == [
        ("2", 22, "foo", date(2023, 1, 3)),
        ("3", 3, "foo", date(2023, 1, 3)),
    ]
