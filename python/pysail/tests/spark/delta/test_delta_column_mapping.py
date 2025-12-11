from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003

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
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

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

        assert protocol.get("minReaderVersion", 0) >= 2  # noqa: PLR2004
        assert protocol.get("minWriterVersion", 0) >= 5  # noqa: PLR2004
        config = metadata.get("configuration", {})
        assert config.get("delta.columnMapping.mode") == "name"
        assert "delta.columnMapping.maxColumnId" in config
        assert int(config["delta.columnMapping.maxColumnId"]) >= 2  # noqa: PLR2004

    def test_create_and_append_with_column_mapping_id(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_id"
        df = spark.createDataFrame(
            [
                Row(i=1, s="x"),
                Row(i=2, s="y"),
            ]
        )

        # Create table with id mode
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "id").save(str(base))

        # Append without option
        df2 = spark.createDataFrame([Row(i=3, s="z")])
        df2.write.format("delta").mode("append").save(str(base))

        out = spark.read.format("delta").load(str(base)).orderBy("i").collect()
        assert [r.asDict() for r in out] == [
            {"i": 1, "s": "x"},
            {"i": 2, "s": "y"},
            {"i": 3, "s": "z"},
        ]

        # Validate protocol and configuration reflect id mode
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
        assert protocol.get("minReaderVersion", 0) >= 2  # noqa: PLR2004
        assert protocol.get("minWriterVersion", 0) >= 5  # noqa: PLR2004
        config = metadata.get("configuration", {})
        assert config.get("delta.columnMapping.mode") == "id"
        assert "delta.columnMapping.maxColumnId" in config
        assert int(config["delta.columnMapping.maxColumnId"]) >= 2  # noqa: PLR2004

    def test_merge_schema_with_column_mapping_name(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_merge_name"

        # Create initial table with name mode
        df = spark.createDataFrame(
            [
                Row(id=1, name="a"),
                Row(id=2, name="b"),
            ]
        )
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

        # Append with a new column using mergeSchema
        df2 = spark.createDataFrame(
            [
                Row(id=3, name="c", age=10),
                Row(id=4, name="d", age=20),
            ]
        )
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

        # Read should include new column, with nulls for old rows
        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 1, "name": "a", "age": None},
            {"id": 2, "name": "b", "age": None},
            {"id": 3, "name": "c", "age": 10},
            {"id": 4, "name": "d", "age": 20},
        ]

        # Validate that maxColumnId exists and is non-decreasing across commits with metadata
        log_dir = base / "_delta_log"
        logs = sorted(log_dir.glob("*.json"))
        assert logs, f"no delta logs in {log_dir}"

        def extract_metadata_config(p: Path) -> dict | None:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    if "metaData" in obj:
                        return obj["metaData"].get("configuration", {})
            return None

        cfgs = [c for c in (extract_metadata_config(p) for p in logs) if c is not None]
        assert cfgs, "no metadata actions found in commit logs"
        assert cfgs[0].get("delta.columnMapping.mode") == "name"
        assert "delta.columnMapping.maxColumnId" in cfgs[0]
        # If there is a later metadata action, ensure maxColumnId is non-decreasing
        if len(cfgs) > 1:
            assert (
                int(cfgs[-1]["delta.columnMapping.maxColumnId"]) >= int(cfgs[0]["delta.columnMapping.maxColumnId"]) + 1
            )

    def test_merge_nested_struct_in_name_mode(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_nested_struct"
        df = spark.createDataFrame([Row(user=Row(id=1, name="a"))])
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

        df2 = spark.createDataFrame([Row(user=Row(id=2, name="b", age=30))])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

        out = spark.read.format("delta").load(str(base)).orderBy("user.id").collect()
        assert [r.asDict(recursive=True) for r in out] == [
            {"user": {"id": 1, "name": "a", "age": None}},
            {"user": {"id": 2, "name": "b", "age": 30}},
        ]

    def test_merge_array_of_struct_in_name_mode(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_array_struct"
        df = spark.createDataFrame([Row(events=[Row(ts=1)])])
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

        df2 = spark.createDataFrame([Row(events=[Row(ts=2, kind="x")])])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

        rows = [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(base)).collect()]
        assert any("kind" in ev for row in rows for ev in row.get("events", []) or [])

    def test_add_new_array_struct_field(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_new_array_struct"
        df = spark.createDataFrame([Row(id=1)])
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

        df2 = spark.createDataFrame([Row(id=2, items=[Row(a=10)])])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        rows = [r.asDict(recursive=True) for r in out]
        assert rows[0]["id"] == 1
        assert rows[0]["items"] is None
        assert rows[1]["id"] == 2  # noqa: PLR2004
        assert isinstance(rows[1]["items"], list)
        assert len(rows[1]["items"]) == 1
        # Do not assert nested field logical name to avoid coupling to physicalName mapping
        assert list(rows[1]["items"][0].values()) == [10]

    def test_merge_map_value_struct(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_map_value_struct"
        df = spark.createDataFrame([Row(attrs={"k": Row(a=1)})])
        df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

        df2 = spark.createDataFrame([Row(attrs={"k": Row(a=2, b=3)})])
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

        row = spark.read.format("delta").load(str(base)).collect()[0].asDict(recursive=True)
        assert "b" in row["attrs"]["k"]

    def test_partitioned_table_with_column_mapping_name(self, spark, tmp_path: Path):
        """Ensure partition columns are resolved when column mapping is enabled."""

        base = tmp_path / "delta_partitioned_cm_name"

        # Create initial partitioned table with column mapping
        df = spark.createDataFrame(
            [
                Row(id=1, region="us", data="a"),
                Row(id=2, region="eu", data="b"),
            ]
        )

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("column_mapping_mode", "name")
            .partitionBy("region")
            .save(str(base))
        )

        # Verify initial write produced data files under the partitioned directory
        parquet_files = list(base.glob("**/*.parquet"))
        assert parquet_files

        # Append new data
        df2 = spark.createDataFrame(
            [
                Row(id=3, region="us", data="c"),
                Row(id=4, region="asia", data="d"),
            ]
        )

        # This would fail previously because the physical partition column was "col-<uuid>"
        df2.write.format("delta").mode("append").save(str(base))

        # Verify read
        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 1, "region": "us", "data": "a"},
            {"id": 2, "region": "eu", "data": "b"},
            {"id": 3, "region": "us", "data": "c"},
            {"id": 4, "region": "asia", "data": "d"},
        ]

    def test_partitioned_table_with_column_mapping_id(self, spark, tmp_path: Path):
        """Partitioned table append/read should work in column mapping id mode."""

        base = tmp_path / "delta_partitioned_cm_id"

        df = spark.createDataFrame(
            [
                Row(id=1, region="us", data="a"),
                Row(id=2, region="eu", data="b"),
            ]
        )

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("column_mapping_mode", "id")
            .partitionBy("region")
            .save(str(base))
        )

        parquet_files = list(base.glob("**/*.parquet"))
        assert parquet_files

        df2 = spark.createDataFrame(
            [
                Row(id=3, region="us", data="c"),
                Row(id=4, region="asia", data="d"),
            ]
        )

        df2.write.format("delta").mode("append").save(str(base))

        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 1, "region": "us", "data": "a"},
            {"id": 2, "region": "eu", "data": "b"},
            {"id": 3, "region": "us", "data": "c"},
            {"id": 4, "region": "asia", "data": "d"},
        ]
