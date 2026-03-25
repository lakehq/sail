import json
import os
import time
from datetime import datetime, timezone

import pytest
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import is_jvm_spark


class TestDeltaAdvancedFeatures:
    """Delta Lake advanced features tests"""

    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake time travel")
    def test_delta_feature_time_travel_by_version(self, spark, tmp_path):
        """Test Delta Lake time travel functionality by version."""
        delta_path = tmp_path / "delta_table"
        delta_table_path = str(delta_path)

        # Version 0: Initial data
        v0_data = [Row(id=1, value="v0")]
        df0 = spark.createDataFrame(v0_data)
        df0.write.format("delta").mode("overwrite").save(delta_table_path)

        # Version 1: Add data
        v1_data = [Row(id=2, value="v1")]
        df1 = spark.createDataFrame(v1_data)
        df1.write.format("delta").mode("append").save(delta_table_path)

        # Version 2: Overwrite data
        v2_data = [Row(id=3, value="v2")]
        df2 = spark.createDataFrame(v2_data)
        df2.write.format("delta").mode("overwrite").save(delta_table_path)

        # Read latest version
        latest_df = spark.read.format("delta").load(delta_table_path)
        assert latest_df.collect() == [Row(id=3, value="v2")]

        # Read version 0
        v0_df = spark.read.format("delta").option("versionAsOf", "0").load(delta_table_path)
        assert v0_df.collect() == [Row(id=1, value="v0")]

        # Read version 1
        v1_df = spark.read.format("delta").option("versionAsOf", "1").load(delta_table_path).sort("id")
        expected_v1 = [Row(id=1, value="v0"), Row(id=2, value="v1")]
        assert v1_df.collect() == expected_v1

    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake time travel")
    def test_delta_feature_time_travel_by_timestamp(self, spark, tmp_path):
        """Test Delta Lake time travel functionality by timestamp."""
        delta_path = tmp_path / "delta_table"
        delta_table_path = str(delta_path)

        # Version 0: Initial data
        v0_data = [Row(id=1, value="v0")]
        df0 = spark.createDataFrame(v0_data)
        df0.write.format("delta").mode("overwrite").save(delta_table_path)
        # Capture the timestamp right after the commit
        ts0 = datetime.now(timezone.utc).isoformat()
        time.sleep(0.1)

        # Version 1: Add data
        v1_data = [Row(id=2, value="v1")]
        df1 = spark.createDataFrame(v1_data)
        df1.write.format("delta").mode("append").save(delta_table_path)
        ts1 = datetime.now(timezone.utc).isoformat()
        time.sleep(0.1)

        # Version 2: Overwrite data
        v2_data = [Row(id=3, value="v2")]
        df2 = spark.createDataFrame(v2_data)
        df2.write.format("delta").mode("overwrite").save(delta_table_path)
        ts2 = datetime.now(timezone.utc).isoformat()

        # Read latest version (should be v2)
        latest_df = spark.read.format("delta").load(delta_table_path)
        assert latest_df.collect() == [Row(id=3, value="v2")]

        # Read state as of timestamp ts0 (version 0)
        v0_df = spark.read.format("delta").option("timestampAsOf", ts0).load(delta_table_path)
        assert v0_df.collect() == [Row(id=1, value="v0")]

        # Read state as of timestamp ts1 (version 1)
        v1_df = spark.read.format("delta").option("timestampAsOf", ts1).load(delta_table_path).sort("id")
        expected_v1 = [Row(id=1, value="v0"), Row(id=2, value="v1")]
        assert v1_df.collect() == expected_v1

        # Read state as of timestamp ts2 (version 2)
        v2_df = spark.read.format("delta").option("timestampAsOf", ts2).load(delta_table_path)
        assert v2_df.collect() == [Row(id=3, value="v2")]

    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake in-commit timestamps")
    def test_delta_feature_time_travel_uses_in_commit_timestamp_not_json_mtime(self, spark, tmp_path):
        delta_path = tmp_path / "delta_ict_table"
        delta_table_path = str(delta_path)
        table_name = "delta_ict_time_travel_test"

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(
            f"""
            CREATE TABLE {table_name}
            USING DELTA
            LOCATION '{delta_table_path}'
            TBLPROPERTIES (
              'delta.enableInCommitTimestamps' = 'true'
            )
            AS SELECT 1 AS id, 'v1' AS value
            """
        )
        spark.createDataFrame([Row(id=2, value="v2")]).write.format("delta").mode("append").save(delta_table_path)

        log_dir = delta_path / "_delta_log"

        def read_in_commit_timestamp(version: int) -> int:
            log_path = log_dir / f"{version:020}.json"
            with log_path.open("r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    if "commitInfo" in obj:
                        return int(obj["commitInfo"]["inCommitTimestamp"])
            msg = f"inCommitTimestamp missing from {log_path}"
            raise AssertionError(msg)

        ts_v0 = read_in_commit_timestamp(0)
        ts_v0_secs = ts_v0 / 1000
        os.utime(
            log_dir / "00000000000000000000.json",
            (ts_v0_secs + 86_400, ts_v0_secs + 86_400),
        )
        os.utime(log_dir / "00000000000000000001.json", (1, 1))

        ts_v0_iso = datetime.fromtimestamp(ts_v0 / 1000, tz=timezone.utc).isoformat()
        df = (
            spark.read.format("delta")
            .option("timestampAsOf", ts_v0_iso)
            .load(delta_table_path)
            .sort("id")
        )
        assert df.collect() == [Row(id=1, value="v1")]

        sql_df = spark.sql(
            f"""
            SELECT *
            FROM {table_name}
            TIMESTAMP AS OF '{ts_v0_iso}'
            ORDER BY id
            """
        )
        assert sql_df.collect() == [Row(id=1, value="v1")]

    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake in-commit timestamps")
    def test_delta_feature_log_cleanup_uses_in_commit_timestamp_not_json_mtime(self, spark, tmp_path):
        delta_path = tmp_path / "delta_ict_cleanup_table"
        delta_table_path = str(delta_path)
        table_name = "delta_ict_cleanup_test"

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(
            f"""
            CREATE TABLE {table_name}
            USING DELTA
            LOCATION '{delta_table_path}'
            TBLPROPERTIES (
              'delta.checkpointInterval' = '2',
              'delta.logRetentionDuration' = 'interval 0 seconds',
              'delta.enableExpiredLogCleanup' = 'true',
              'delta.enableInCommitTimestamps' = 'true'
            )
            AS SELECT 1 AS id
            """
        )
        spark.createDataFrame([Row(id=2)]).write.format("delta").mode("append").save(delta_table_path)
        spark.createDataFrame([Row(id=3)]).write.format("delta").mode("append").save(delta_table_path)

        log_dir = delta_path / "_delta_log"

        def rewrite_commit_timestamp(version: int, timestamp_ms: int) -> None:
            log_path = log_dir / f"{version:020}.json"
            rewritten = []
            with log_path.open("r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    if "commitInfo" in obj:
                        obj["commitInfo"]["inCommitTimestamp"] = timestamp_ms
                    rewritten.append(json.dumps(obj, separators=(",", ":")))
            with log_path.open("w", encoding="utf-8") as f:
                f.write("\n".join(rewritten))

            crc_path = log_dir / f"{version:020}.crc"
            with crc_path.open("r", encoding="utf-8") as f:
                crc_obj = json.load(f)
            crc_obj["inCommitTimestampOpt"] = timestamp_ms
            with crc_path.open("w", encoding="utf-8") as f:
                json.dump(crc_obj, f, separators=(",", ":"))

        for version, timestamp_ms in enumerate((100, 200, 300)):
            rewrite_commit_timestamp(version, timestamp_ms)
            future_seconds = time.time() + 86_400
            os.utime(log_dir / f"{version:020}.json", (future_seconds, future_seconds))

        spark.createDataFrame([Row(id=4)]).write.format("delta").mode("append").save(delta_table_path)
        spark.createDataFrame([Row(id=5)]).write.format("delta").mode("append").save(delta_table_path)

        assert spark.read.format("delta").load(delta_table_path).sort("id").collect() == [
            Row(id=1),
            Row(id=2),
            Row(id=3),
            Row(id=4),
            Row(id=5),
        ]
        assert sorted(path.name for path in log_dir.iterdir()) == [
            "00000000000000000002.checkpoint.parquet",
            "00000000000000000002.crc",
            "00000000000000000002.json",
            "00000000000000000003.crc",
            "00000000000000000003.json",
            "00000000000000000004.checkpoint.parquet",
            "00000000000000000004.crc",
            "00000000000000000004.json",
            "_last_checkpoint",
        ]
