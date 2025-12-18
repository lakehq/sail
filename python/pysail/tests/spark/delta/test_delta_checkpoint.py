from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.utils import escape_sql_string_literal, is_jvm_spark

if TYPE_CHECKING:
    from pathlib import Path


def test_delta_checkpoint_created_and_metrics_exposed(spark, tmp_path: Path):
    if is_jvm_spark():
        pytest.skip("Sail-only: checkpoint creation and DataFusion metrics are Sail-specific")

    # NOTE: `TBLPROPERTIES(delta.checkpointInterval)` isn't plumbed into Delta metadata yet.
    # Default interval (100) means only v0 checkpoints are guaranteed in short tests.

    base = tmp_path / "delta_checkpoint"
    table_name = "delta_checkpoint_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    delta_path = escape_sql_string_literal(str(base))
    spark.sql(f"CREATE TABLE {table_name} (id INT) USING DELTA LOCATION '{delta_path}'")

    spark.sql(f"INSERT INTO {table_name} VALUES (1), (2)")  # noqa: S608

    log_dir = base / "_delta_log"
    assert log_dir.exists(), f"missing delta log dir: {log_dir}"

    # We should always create a checkpoint at v0 (0 % interval == 0).
    assert list(log_dir.glob(f"{0:020}.checkpoint*.parquet")), (
        f"expected v0 checkpoint parquet in {log_dir}, found none"
    )

    last_checkpoint_file = log_dir / "_last_checkpoint"
    assert last_checkpoint_file.exists(), f"missing _last_checkpoint: {last_checkpoint_file}"
    last_checkpoint = json.loads(last_checkpoint_file.read_text(encoding="utf-8"))
    assert last_checkpoint.get("version") == 0

    # Prove checkpoints are actually usable (not just created):
    # delete the v0 JSON commit and ensure we can still read via checkpoint + remaining logs.
    v0_json = log_dir / f"{0:020}.json"
    assert v0_json.exists(), f"missing v0 delta log file: {v0_json}"
    v0_json.unlink()

    # Latest snapshot read should still work (uses checkpoint v0 + v1 json).
    assert spark.read.format("delta").load(str(base)).count() == 2  # noqa: PLR2004

    # Time travel to version 0 should still work without the v0 JSON file.
    assert (
        spark.read.format("delta").option("versionAsOf", "0").load(str(base)).count() == 2  # noqa: PLR2004
    )

    # Verify plan includes our custom commit metrics labels.
    plan = spark.sql(f"EXPLAIN ANALYZE INSERT INTO {table_name} VALUES (3)").collect()[0][0]  # noqa: S608
    assert "DeltaCommitExec" in plan
    assert "num_commit_retries" in plan
    assert "checkpoint_created" in plan
    assert "log_files_cleaned" in plan
