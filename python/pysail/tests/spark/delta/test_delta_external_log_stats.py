import json
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import IntegerType, StructField, StructType
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


def _write_external_delta_table(table_path, schema, files):
    table_path.mkdir()
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": table_path.name,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.json(),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0,
            }
        },
    ]
    for index, (table, stats) in enumerate(files):
        data_file = table_path / f"part-{index}.parquet"
        pq.write_table(table, data_file)
        actions.append(
            {
                "add": {
                    "path": data_file.name,
                    "partitionValues": {},
                    "size": data_file.stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps(stats),
                }
            }
        )
    log_dir = table_path / "_delta_log"
    log_dir.mkdir()
    (log_dir / "00000000000000000000.json").write_text(
        "".join(f"{json.dumps(action)}\n" for action in actions), encoding="utf-8"
    )


@pytest.mark.parametrize("dotted_stats", ["missing", "null_count_only", "present"])
def test_dotted_column_does_not_borrow_nested_statistics(spark, tmp_path, dotted_stats):
    table_path = tmp_path / "dotted_column_stats"
    schema = StructType(
        [
            StructField("a.b", IntegerType()),
            StructField("a", StructType([StructField("b", IntegerType())])),
        ]
    )
    files = []
    for value in [1, 2]:
        values = [value, value] if dotted_stats == "present" else [1, 2]
        table = pa.table(
            {
                "a.b": pa.array(values, type=pa.int32()),
                "a": pa.array([{"b": 7}, {"b": 7}], type=pa.struct([("b", pa.int32())])),
            }
        )
        stats = {
            "numRecords": 2,
            "minValues": {"a": {"b": 7}},
            "maxValues": {"a": {"b": 7}},
            "nullCount": {"a": {"b": 0}},
        }
        if dotted_stats != "missing":
            stats["nullCount"]["a.b"] = 0
        if dotted_stats == "present":
            stats["minValues"]["a.b"] = value
            stats["maxValues"]["a.b"] = value
        files.append((table, stats))
    _write_external_delta_table(table_path, schema, files)

    for source in ["parquet", "delta"]:
        relation = f"{source}.`{table_path}`"
        assert spark.sql(
            f"SELECT `a.b`, COUNT(*) AS n FROM {relation} GROUP BY `a.b` ORDER BY `a.b`"  # noqa: S608
        ).collect() == [Row(1, 2), Row(2, 2)]
        assert spark.sql(f"SELECT `a.b` FROM {relation} WHERE `a.b` = 1").collect() == [Row(1), Row(1)]  # noqa: S608
        assert spark.sql(f"SELECT MIN(a.b), MAX(a.b) FROM {relation}").collect() == [Row(7, 7)]  # noqa: S608


@pytest.mark.parametrize("tight_bounds", [True, False])
@pytest.mark.parametrize("all_null", [False, True])
def test_extrema_ignore_bounds_from_all_null_files(spark, tmp_path, tight_bounds, all_null):
    table_path = tmp_path / "all_null_file_stats"
    schema = StructType([StructField("id", IntegerType())])
    files = [
        (
            pa.table({"id": pa.array([None, None], type=pa.int32())}),
            {
                "numRecords": 2,
                "minValues": {"id": -999},
                "maxValues": {"id": 999},
                "nullCount": {"id": 2},
                "tightBounds": tight_bounds,
            },
        )
    ]
    if not all_null:
        files.append(
            (
                pa.table({"id": pa.array([5, 6], type=pa.int32())}),
                {
                    "numRecords": 2,
                    "minValues": {"id": 5},
                    "maxValues": {"id": 6},
                    "nullCount": {"id": 0},
                },
            )
        )
    _write_external_delta_table(table_path, schema, files)

    expected = [Row(None, None, 0, 2)] if all_null else [Row(5, 6, 2, 4)]
    for source in ["parquet", "delta"]:
        assert (
            spark.sql(
                f"SELECT MIN(id), MAX(id), COUNT(id), COUNT(*) FROM {source}.`{table_path}`"  # noqa: S608
            ).collect()
            == expected
        )
