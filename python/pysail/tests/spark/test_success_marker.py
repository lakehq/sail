"""Tests verifying that Sail writes a `_SUCCESS` marker file under the output
directory after a successful file write, matching Spark's default behavior
controlled by `mapreduce.fileoutputcommitter.marksuccessfuljobs`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _success_marker(directory: str | Path) -> Path:
    return Path(directory) / "_SUCCESS"


@pytest.mark.parametrize("fmt", ["parquet", "json", "csv", "text"])
def test_write_creates_success_marker(spark, tmp_path, fmt):
    location = str(tmp_path / fmt)
    if fmt == "text":
        df = spark.createDataFrame([("alice",), ("bob",)], schema=["value"])
    else:
        df = spark.createDataFrame([(1, "alice"), (2, "bob")], schema=["id", "name"])
    df.write.format(fmt).save(location)

    marker = _success_marker(location)
    assert marker.is_file(), f"missing _SUCCESS marker at {marker}"
    # The marker file is empty.
    assert marker.stat().st_size == 0


def test_write_partitioned_creates_single_root_marker(spark, tmp_path):
    location = str(tmp_path / "partitioned")
    df = spark.createDataFrame(
        [(1, "a", 10), (2, "a", 20), (3, "b", 30)],
        schema=["id", "p", "v"],
    )
    df.write.partitionBy("p").parquet(location)

    root = Path(location)
    # Root marker exists.
    assert _success_marker(root).is_file()
    # Marker is not written into partition subdirectories.
    partition_markers = [p for p in root.rglob("_SUCCESS")]
    assert partition_markers == [_success_marker(root)]


def test_write_empty_dataframe_creates_marker(spark, tmp_path):
    location = str(tmp_path / "empty")
    df = spark.createDataFrame([], schema="id LONG, name STRING")
    df.write.parquet(location)
    assert _success_marker(location).is_file()


def test_insert_overwrite_directory_creates_marker(spark, tmp_path):
    location = str(tmp_path / "iod")
    spark.sql(f"""
        INSERT OVERWRITE DIRECTORY '{escape_sql_string_literal(location)}' USING PARQUET
        SELECT CAST(1 AS LONG) AS id, 'alice' AS name
    """)
    assert _success_marker(location).is_file()


def test_marker_present_after_dataframe_writer_save(spark, tmp_path):
    """Verify the marker is present when using the generic DataFrameWriter.save API."""
    location = str(tmp_path / "save")
    df = spark.createDataFrame([(1, "alice")], schema=["id", "name"])
    df.write.save(location, format="parquet")
    assert _success_marker(location).is_file()
