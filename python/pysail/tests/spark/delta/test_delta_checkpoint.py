from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from pathlib import Path


def _read_first_commit_metadata(table_path: Path) -> dict:
    log_file = table_path / "_delta_log" / "00000000000000000000.json"
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "metaData" in obj:
                return obj["metaData"]
    msg = f"metaData action not found in first delta log: {log_file}"
    raise AssertionError(msg)


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
def test_delta_write_to_table_properties_materialize_metadata(spark, tmp_path: Path):
    """Test that .writeTo().tableProperty() materializes Delta table properties on first commit.

    This exercises the DataFrame API code path (.writeTo + .tableProperty), which is distinct
    from the SQL DDL path (TBLPROPERTIES) covered by checkpoint_properties.feature.
    """
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

        metadata = _read_first_commit_metadata(base)
        assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "3"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.skipif(is_jvm_spark(), reason="Spark does not handle v1 and v2 tables properly")
def test_delta_write_option_routes_table_property_to_metadata(spark, tmp_path: Path):
    """Test that Delta table properties can be passed through DataFrame write options.

    This covers the path-based DataFrame API (`.write.option(...).save(...)`), where Delta table
    properties arrive mixed in with transient write options and must be routed into the first
    `metaData.configuration` action for a new table.
    """
    base = tmp_path / "delta_write_option_checkpoint"

    df = spark.createDataFrame([Row(id=1), Row(id=2)])
    (df.write.format("delta").mode("overwrite").option("delta.checkpointInterval", "3").save(str(base)))

    actual = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [row.id for row in actual] == [1, 2]

    metadata = _read_first_commit_metadata(base)
    assert metadata.get("configuration", {}).get("delta.checkpointInterval") == "3"
