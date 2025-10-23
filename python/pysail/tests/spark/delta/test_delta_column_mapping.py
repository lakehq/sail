from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import Row


class TestDeltaColumnMapping:
    def test_create_table_with_column_mapping_name(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_name"
        df = spark.createDataFrame(
            [
                Row(id=1, name="a"),
                Row(id=2, name="b"),
            ]
        )

        # Write new table with column mapping name mode
        df.write.format("delta").mode("overwrite").option(
            "column_mapping_mode", "name"
        ).save(str(base))

        # Basic read should succeed
        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]

        # Inspect first commit log to validate protocol and metadata
        log_file = base / "_delta_log" / "00000000000000000000.json"
        assert log_file.exists(), f"missing delta log file: {log_file}"
        protocol = None
        metadata = None
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if "protocol" in obj:
                    protocol = obj["protocol"]
                if "metaData" in obj:
                    metadata = obj["metaData"]

        assert protocol is not None, "protocol action not found in first commit"
        assert metadata is not None, "metadata action not found in first commit"

        assert protocol.get("minReaderVersion", 0) >= 2
        assert protocol.get("minWriterVersion", 0) >= 5
        config = metadata.get("configuration", {})
        assert config.get("delta.columnMapping.mode") == "name"


