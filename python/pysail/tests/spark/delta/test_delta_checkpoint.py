from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from pathlib import Path


def _first_commit_metadata(table_path: Path) -> dict:
    log_file = table_path / "_delta_log" / "00000000000000000000.json"
    assert log_file.exists(), f"missing delta log file: {log_file}"
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "metaData" in obj:
                return obj["metaData"]
    msg = f"metadata action not found in first delta log: {log_file}"
    raise AssertionError(msg)


def test_delta_checkpoint_created_and_metrics_exposed(spark, tmp_path: Path):
    if is_jvm_spark():
        pytest.skip("Sail-only: checkpoint creation and DataFusion metrics are Sail-specific")

    base = tmp_path / "delta_checkpoint"
    table_name = "delta_checkpoint_test"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    delta_path = escape_sql_string_literal(str(base))
    spark.sql(f"""
        CREATE TABLE {table_name} (id INT)
        USING DELTA
        LOCATION '{delta_path}'
        TBLPROPERTIES ('delta.checkpointInterval'='1')
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES (1), (2)")  # noqa: S608
    spark.sql(f"INSERT INTO {table_name} VALUES (3)")  # noqa: S608

    log_dir = base / "_delta_log"
    assert log_dir.exists(), f"missing delta log dir: {log_dir}"

    metadata = _first_commit_metadata(base)
    assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "1"

    assert not list(log_dir.glob(f"{0:020}.checkpoint*.parquet")), f"did not expect v0 checkpoint parquet in {log_dir}"
    assert list(log_dir.glob(f"{1:020}.checkpoint*.parquet")), (
        f"expected v1 checkpoint parquet in {log_dir}, found none"
    )

    last_checkpoint_file = log_dir / "_last_checkpoint"
    assert last_checkpoint_file.exists(), f"missing _last_checkpoint: {last_checkpoint_file}"
    last_checkpoint = json.loads(last_checkpoint_file.read_text(encoding="utf-8"))
    assert last_checkpoint.get("version") == 1

    # Prove checkpoints are actually usable (not just created):
    # delete the v1 JSON commit and ensure we can still read via checkpoint + remaining logs.
    v1_json = log_dir / f"{1:020}.json"
    assert v1_json.exists(), f"missing v1 delta log file: {v1_json}"
    v1_json.unlink()

    # Latest snapshot read should still work (uses checkpoint v1).
    assert spark.read.format("delta").load(str(base)).count() == 3  # noqa: PLR2004

    # Time travel to version 1 should still work without the v1 JSON file.
    assert (
        spark.read.format("delta").option("versionAsOf", "1").load(str(base)).count() == 3  # noqa: PLR2004
    )


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
def test_delta_write_to_table_properties_materialize_metadata(spark, tmp_path: Path):
    base = tmp_path / "delta_write_to_checkpoint"
    table_name = "delta_write_to_checkpoint"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.createDataFrame([Row(id=1), Row(id=2)]).writeTo(table_name).using("delta").option(
            "path",
            str(base),
        ).tableProperty("delta.checkpointInterval", "3").create()

        actual = spark.sql(f"SELECT * FROM {table_name} ORDER BY id").collect()  # noqa: S608
        assert [row.id for row in actual] == [1, 2]

        metadata = _first_commit_metadata(base)
        assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "3"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
