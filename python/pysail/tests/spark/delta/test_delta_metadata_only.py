from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.utils import is_jvm_spark

if TYPE_CHECKING:
    from pathlib import Path


def _write_metadata_only_log(table_path: Path) -> None:
    table_path.mkdir(parents=True, exist_ok=True)
    log_dir = table_path / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)

    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
    )

    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "name": None,
                "description": None,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1700000000000,
            }
        },
    ]

    commit_file = log_dir / "00000000000000000000.json"
    with commit_file.open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action))
            f.write("\n")


@pytest.mark.parametrize(
    "read_mode",
    ["driver_path", "metadata_path"],
    ids=["driver_path", "metadata_path"],
)
def test_read_metadata_only_table_without_add_actions(spark, tmp_path: Path, read_mode: str):
    if is_jvm_spark():
        pytest.skip("Sail-only: metadata-only Delta log replay behavior is Sail-specific")

    table_path = tmp_path / "delta_metadata_only"
    _write_metadata_only_log(table_path)

    reader = spark.read.format("delta")
    if read_mode == "metadata_path":
        reader = reader.option("metadataAsDataRead", "true")

    rows = reader.load(str(table_path)).collect()
    assert rows == []
