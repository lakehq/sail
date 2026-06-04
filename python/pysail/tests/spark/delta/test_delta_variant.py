from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

if TYPE_CHECKING:
    from pathlib import Path


pytestmark = [
    pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta VariantShredding physical scan path"),
    pytest.mark.skipif(pyspark_version() < (4,), reason="Variant SQL functions require PySpark 4+"),
]


def _write_shredded_variant_delta_table(table_location: Path) -> None:
    table_location.mkdir(parents=True, exist_ok=True)
    log_dir = table_location / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)

    data_file = table_location / "part-00000-shredded-variant.parquet"
    payload_type = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("typed_value", pa.int64(), nullable=True),
        ]
    )
    payload = pa.array(
        [
            {"metadata": b"\x01\x00\x00", "typed_value": 42},
            {"metadata": b"\x01\x00\x00", "typed_value": 99},
        ],
        type=payload_type,
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            payload,
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("payload", payload_type, nullable=True),
            ]
        ),
    )
    pq.write_table(table, data_file)

    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "payload", "type": "variant", "nullable": True, "metadata": {}},
        ],
    }
    created_time = 1_700_000_000_000
    actions = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["variantType", "variantShredding-preview"],
                "writerFeatures": ["variantType", "variantShredding-preview"],
            }
        },
        {
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000001",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema, separators=(",", ":")),
                "partitionColumns": [],
                "configuration": {"delta.enableVariantShredding": "true"},
                "createdTime": created_time,
            }
        },
        {
            "add": {
                "path": data_file.name,
                "partitionValues": {},
                "size": data_file.stat().st_size,
                "modificationTime": created_time,
                "dataChange": True,
                "stats": json.dumps(
                    {
                        "numRecords": 2,
                        "minValues": {"id": 1},
                        "maxValues": {"id": 2},
                        "nullCount": {"id": 0, "payload": 0},
                    },
                    separators=(",", ":"),
                ),
            }
        },
    ]
    with (log_dir / "00000000000000000000.json").open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action, separators=(",", ":")))
            f.write("\n")


def test_delta_dataframe_read_shredded_variant_physical_data(spark, tmp_path: Path):
    table_path = tmp_path / "delta_variant_shredded_read"
    _write_shredded_variant_delta_table(table_path)

    rows = (
        spark.read.format("delta")
        .load(str(table_path))
        .select(
            "id",
            F.expr("variant_get(payload, '$', 'int')").alias("payload_value"),
            F.expr("to_json(payload)").alias("payload_json"),
        )
        .orderBy("id")
        .collect()
    )

    assert [(row.id, row.payload_value, row.payload_json) for row in rows] == [
        (1, 42, "42"),
        (2, 99, "99"),
    ]


def _data_parquet_files(table_location: Path) -> list[Path]:
    return sorted(path for path in table_location.rglob("*.parquet") if "_delta_log" not in path.parts)


def _first_add_stats(table_location: Path) -> dict:
    log_file = table_location / "_delta_log" / "00000000000000000000.json"
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            action = json.loads(line)
            if "add" in action:
                return json.loads(action["add"]["stats"])
    msg = f"add action not found in {log_file}"
    raise AssertionError(msg)


def test_delta_writer_shreds_variant_physical_data(spark, tmp_path: Path):
    table_path = tmp_path / "delta_variant_shredded_write"
    spark.sql("DROP TABLE IF EXISTS delta_variant_shredded_write_table")
    spark.sql(
        f"""
        CREATE TABLE delta_variant_shredded_write_table (
          id INT,
          payload VARIANT
        )
        USING DELTA
        LOCATION '{table_path.as_posix()}'
        TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO delta_variant_shredded_write_table
        SELECT * FROM VALUES
          (1, parse_json('{"a":1,"b":"delta"}')),
          (2, parse_json('{"a":2,"b":"lake"}'))
        """
    )

    data_files = _data_parquet_files(table_path)
    assert len(data_files) == 1

    payload_field = pq.read_schema(data_files[0]).field("payload")
    payload_type = payload_field.type
    assert pa.types.is_struct(payload_type)
    payload_fields = {payload_type.field(i).name for i in range(payload_type.num_fields)}
    assert "metadata" in payload_fields
    assert "typed_value" in payload_fields
    typed_value = payload_type.field("typed_value").type
    assert pa.types.is_struct(typed_value)
    typed_value_fields = {typed_value.field(i).name for i in range(typed_value.num_fields)}
    assert "a" in typed_value_fields
    assert "b" in typed_value_fields

    rows = (
        spark.read.format("delta")
        .load(str(table_path))
        .select(
            "id",
            F.expr("variant_get(payload, '$.a', 'int')").alias("a"),
            F.expr("variant_get(payload, '$.b', 'string')").alias("b"),
        )
        .orderBy("id")
        .collect()
    )
    assert [(row.id, row.a, row.b) for row in rows] == [(1, 1, "delta"), (2, 2, "lake")]

    stats = _first_add_stats(table_path)
    assert isinstance(stats.get("minValues", {}).get("payload"), str)
    assert isinstance(stats.get("maxValues", {}).get("payload"), str)
    assert stats.get("nullCount", {}).get("payload") == 0
    assert "typed_value" not in json.dumps(stats, separators=(",", ":"))
