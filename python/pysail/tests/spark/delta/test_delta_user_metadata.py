from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql.types import Row

if TYPE_CHECKING:
    from pathlib import Path


def _read_latest_commit_info(table_path: Path) -> dict:
    log_dir = table_path / "_delta_log"
    logs = sorted(log_dir.glob("*.json"))
    assert logs, f"no delta logs found in {log_dir}"
    latest = logs[-1]
    with latest.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "commitInfo" in obj:
                return obj["commitInfo"]
    msg = f"commitInfo action not found in {latest}"
    raise AssertionError(msg)


class TestDeltaUserMetadata:
    """`userMetadata` writer option populates `commitInfo.userMetadata` per-commit."""

    def test_append_with_user_metadata_records_value(self, spark, tmp_path):
        delta_path = tmp_path / "delta_user_metadata_append"
        df = spark.createDataFrame([Row(id=1), Row(id=2)])

        df.write.format("delta").mode("append").option("userMetadata", "job=daily-load run=42").save(str(delta_path))

        commit_info = _read_latest_commit_info(delta_path)
        assert commit_info.get("userMetadata") == "job=daily-load run=42"
        # First write to a non-existing path is a CREATE TABLE commit.
        assert commit_info.get("operation") == "CREATE TABLE"

    def test_overwrite_with_user_metadata_records_value(self, spark, tmp_path):
        delta_path = tmp_path / "delta_user_metadata_overwrite"
        spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").save(str(delta_path))

        spark.createDataFrame([Row(id=2), Row(id=3)]).write.format("delta").mode("overwrite").option(
            "userMetadata", "audit=overwrite-v2"
        ).save(str(delta_path))

        commit_info = _read_latest_commit_info(delta_path)
        assert commit_info.get("userMetadata") == "audit=overwrite-v2"
        assert commit_info.get("operation") == "WRITE"

    def test_user_metadata_is_per_commit(self, spark, tmp_path):
        """A subsequent write without `userMetadata` must not inherit the previous value."""
        delta_path = tmp_path / "delta_user_metadata_per_commit"

        spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option(
            "userMetadata", "first-commit"
        ).save(str(delta_path))
        first_commit = _read_latest_commit_info(delta_path)
        assert first_commit.get("userMetadata") == "first-commit"

        spark.createDataFrame([Row(id=2)]).write.format("delta").mode("append").save(str(delta_path))
        second_commit = _read_latest_commit_info(delta_path)
        assert "userMetadata" not in second_commit

    def test_empty_user_metadata_is_omitted(self, spark, tmp_path):
        """An empty option value parses to `None`, so `userMetadata` is not written."""
        delta_path = tmp_path / "delta_user_metadata_empty"

        spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option("userMetadata", "").save(
            str(delta_path)
        )

        commit_info = _read_latest_commit_info(delta_path)
        assert "userMetadata" not in commit_info

    @pytest.mark.parametrize(
        "alias_key",
        ["userMetadata", "user_metadata"],
    )
    def test_user_metadata_alias_keys(self, spark, tmp_path, alias_key):
        """Both the camelCase and snake_case option keys are accepted."""
        delta_path = tmp_path / f"delta_user_metadata_alias_{alias_key}"

        spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option(
            alias_key, f"label-via-{alias_key}"
        ).save(str(delta_path))

        commit_info = _read_latest_commit_info(delta_path)
        assert commit_info.get("userMetadata") == f"label-via-{alias_key}"
