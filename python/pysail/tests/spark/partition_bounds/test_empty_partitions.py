"""A partition directory that holds no readable file must not win min or max.

These cases cannot be written as scenarios because the files Sail writes have
generated names, so they have to be located and removed from the test itself.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


def _create(spark, name: str, location, columns: str, partition_by: str, values: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(
        f"CREATE TABLE {name} ({columns}) USING parquet "
        f"PARTITIONED BY ({partition_by}) LOCATION '{location}'"
    )
    spark.sql(f"INSERT INTO {name} VALUES {values}")


def test_leading_level_skips_a_partition_left_without_files(spark, tmp_path):
    location = tmp_path / "flat"
    _create(
        spark,
        "pb_empty_flat",
        location,
        "id INT, dt STRING",
        "dt",
        "(1, '2025-10-01'), (2, '2025-10-04')",
    )
    try:
        removed = [p.unlink() for p in (location / "dt=2025-10-04").glob("*.parquet")]
        assert removed, "expected the latest partition to hold a file to remove"
        assert (location / "dt=2025-10-04").is_dir(), "the directory must survive the removal"

        # The directory of the latest partition is still listed, but it no longer holds
        # a file a scan would read, so the answer is the partition below it.
        assert spark.sql("SELECT max(dt) FROM pb_empty_flat").collect()[0][0] == "2025-10-01"
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_empty_flat")


def test_middle_level_descends_past_intermediate_directories(spark, tmp_path):
    location = tmp_path / "nested"
    _create(
        spark,
        "pb_empty_nested",
        location,
        "id INT, year STRING, month STRING, day STRING",
        "year, month, day",
        "(1, '2025', '03', '07'), (2, '2025', '10', '04')",
    )
    try:
        removed = [p.unlink() for p in (location / "year=2025" / "month=10").rglob("*.parquet")]
        assert removed, "expected the latest month to hold a file to remove"
        assert (location / "year=2025" / "month=10" / "day=04").is_dir()

        # `month=10` holds a further directory rather than files, so deciding whether it
        # holds data at all means descending through `day=04`. It does not, so `month=03` wins.
        result = spark.sql("SELECT max(month) FROM pb_empty_nested WHERE year = '2025'").collect()
        assert result[0][0] == "03"
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_empty_nested")
