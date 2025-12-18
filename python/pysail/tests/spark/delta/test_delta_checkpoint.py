from __future__ import annotations

import json
from pathlib import Path

import pytest

from pysail.tests.spark.utils import escape_sql_string_literal, is_jvm_spark


def test_delta_checkpoint_created_and_metrics_exposed(spark, tmp_path: Path):
    if is_jvm_spark():
        pytest.skip("Sail-only: checkpoint creation and DataFusion metrics are Sail-specific")

    # NOTE: `TBLPROPERTIES(delta.checkpointInterval)` isn't plumbed into Delta metadata yet.
    # Default interval (100) means only v0 checkpoints are guaranteed in short tests.

    base = tmp_path / "delta_checkpoint"
    table_name = "delta_checkpoint_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    delta_path = escape_sql_string_literal(str(base))
    spark.sql(
        "CREATE TABLE {table_name} (id INT) USING DELTA "
        "LOCATION '{delta_path}'".format(table_name=table_name, delta_path=delta_path)
    )

    spark.sql(f"INSERT INTO {table_name} VALUES (1), (2)")

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

    # Verify plan includes our custom commit metrics labels.
    plan = spark.sql(f"EXPLAIN ANALYZE INSERT INTO {table_name} VALUES (3)").collect()[0][0]
    assert "DeltaCommitExec" in plan
    assert "num_commit_retries" in plan
    assert "checkpoint_created" in plan
    assert "log_files_cleaned" in plan


