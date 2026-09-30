"""Partition directories that a scan would not read must not win min or max.

These cases cannot be written as scenarios because they need the files on disk to be
moved or removed, and the names Sail generates for them are not known in advance.

Every case runs against both bounds. `min` and `max` walk the candidates from opposite
ends, so a partition holding no data is skipped by different code paths depending on
which end it sits at, and `min` is the direction that a retention policy empties.
"""

from __future__ import annotations

import shutil

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")

# Matches the number of individual probes in `partition_bounds.rs`, so that the tests
# below cross the point where the search switches to a single listing.
MAX_INDIVIDUAL_PROBES = 8


def _create(spark, name: str, location, columns: str, partition_by: str, values: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(
        f"CREATE TABLE {name} ({columns}) USING parquet "
        f"PARTITIONED BY ({partition_by}) LOCATION '{location}'"
    )
    spark.sql(f"INSERT INTO {name} VALUES {values}")


def _empty(directory) -> None:
    removed = [p.unlink() for p in directory.rglob("*.parquet")]
    assert removed, f"expected {directory} to hold a file to remove"
    assert directory.is_dir(), "the directory must survive the removal"


@pytest.mark.parametrize(
    ("bound", "emptied", "expected"),
    [
        ("max", "2025-10-04", "2025-10-01"),
        ("min", "2025-10-01", "2025-10-04"),
    ],
)
def test_leading_level_skips_a_partition_left_without_files(
    spark, tmp_path, bound, emptied, expected
):
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
        _empty(location / f"dt={emptied}")

        # The directory is still listed, but it no longer holds a file a scan would
        # read, so the answer is the partition on the other side.
        assert spark.sql(f"SELECT {bound}(dt) FROM pb_empty_flat").collect()[0][0] == expected
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_empty_flat")


@pytest.mark.parametrize(
    ("bound", "emptied", "expected"),
    [
        ("max", "10", "03"),
        ("min", "03", "10"),
    ],
)
def test_middle_level_descends_past_intermediate_directories(
    spark, tmp_path, bound, emptied, expected
):
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
        _empty(location / "year=2025" / f"month={emptied}")

        # A month directory holds further directories rather than files, so deciding
        # whether it holds data at all means descending through the day below it.
        result = spark.sql(
            f"SELECT {bound}(month) FROM pb_empty_nested WHERE year = '2025'"
        ).collect()
        assert result[0][0] == expected
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_empty_nested")


@pytest.mark.parametrize("bound", ["max", "min"])
def test_more_empty_partitions_than_probes_falls_back_to_one_listing(spark, tmp_path, bound):
    location = tmp_path / "many"
    # One partition more than the probe budget is emptied at the end the bound starts
    # from, so the search runs out of individual probes and has to settle the rest
    # with a single listing.
    emptied = MAX_INDIVIDUAL_PROBES + 1
    days = [f"2025-10-{day:02d}" for day in range(1, emptied + 3)]
    values = ", ".join(f"({i}, '{day}')" for i, day in enumerate(days))
    _create(spark, "pb_many", location, "id INT, dt STRING", "dt", values)
    try:
        if bound == "max":
            drained, expected = days[-emptied:], days[-emptied - 1]
        else:
            drained, expected = days[:emptied], days[emptied]
        for day in drained:
            _empty(location / f"dt={day}")

        assert spark.sql(f"SELECT {bound}(dt) FROM pb_many").collect()[0][0] == expected
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_many")


def test_table_without_any_partition_has_null_bounds(spark, tmp_path):
    location = tmp_path / "void"
    location.mkdir()
    spark.sql("DROP TABLE IF EXISTS pb_void")
    spark.sql(
        f"CREATE TABLE pb_void (id INT, dt STRING) USING parquet "
        f"PARTITIONED BY (dt) LOCATION '{location}'"
    )
    try:
        # No partition directory exists at all, so there is no bound to report.
        assert spark.sql("SELECT max(dt), min(dt) FROM pb_void").collect()[0] == (None, None)

        # The bound is NULL, so the filter it feeds matches nothing. This walks the
        # inlining with a NULL literal, which then reaches the scan as a partition
        # filter, and must come back empty rather than error or match everything.
        rows = spark.sql(
            "SELECT count(*) AS n FROM pb_void WHERE dt = (SELECT max(dt) FROM pb_void)"
        ).collect()
        assert rows[0][0] == 0
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_void")


def test_null_bound_filter_over_a_table_that_has_rows(spark, tmp_path):
    location = tmp_path / "null_bound"
    _create(
        spark,
        "pb_null_bound",
        location,
        "id INT, dt STRING",
        "dt",
        "(1, '2025-10-01'), (2, '2025-10-04')",
    )
    try:
        for day in ("2025-10-01", "2025-10-04"):
            _empty(location / f"dt={day}")

        # Every partition is empty, so the bound is NULL while the directories are
        # still there. Comparing against NULL matches nothing.
        rows = spark.sql(
            "SELECT count(*) AS n FROM pb_null_bound "
            "WHERE dt = (SELECT max(dt) FROM pb_null_bound)"
        ).collect()
        assert rows[0][0] == 0
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_null_bound")


def test_every_partition_emptied_has_null_bounds(spark, tmp_path):
    location = tmp_path / "drained"
    _create(
        spark,
        "pb_drained",
        location,
        "id INT, dt STRING",
        "dt",
        "(1, '2025-10-01'), (2, '2025-10-04')",
    )
    try:
        for day in ("2025-10-01", "2025-10-04"):
            _empty(location / f"dt={day}")

        # Directories remain but none holds data, so both bounds are NULL rather than
        # the largest or smallest directory name.
        assert spark.sql("SELECT max(dt), min(dt) FROM pb_drained").collect()[0] == (None, None)
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_drained")


def test_null_partition_is_excluded_from_bounds(spark, tmp_path):
    location = tmp_path / "hive_null"
    _create(
        spark,
        "pb_hive_null",
        location,
        "id INT, dt STRING",
        "dt",
        "(1, '2025-10-01'), (2, '2025-10-04')",
    )
    try:
        # Writers outside Sail encode a NULL partition value with this marker, and it
        # sorts above every date. `min`/`max` ignore NULL, so it must not win.
        shutil.copytree(location / "dt=2025-10-04", location / "dt=__HIVE_DEFAULT_PARTITION__")

        assert spark.sql("SELECT max(dt) FROM pb_hive_null").collect()[0][0] == "2025-10-04"
        assert spark.sql("SELECT min(dt) FROM pb_hive_null").collect()[0][0] == "2025-10-01"
    finally:
        spark.sql("DROP TABLE IF EXISTS pb_hive_null")
