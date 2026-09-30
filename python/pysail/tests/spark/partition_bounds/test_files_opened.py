"""How many files the scan actually opens, measured rather than inferred.

The plan snapshots show how many files the scan was given. This asserts how many it
opened, which is the number that matters on object storage and the one that stayed a
deduction until now.
"""

from __future__ import annotations

import re

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")

DAYS = ["2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04"]


@pytest.fixture
def table(spark, tmp_path):
    location = tmp_path / "opened"
    spark.sql("DROP TABLE IF EXISTS pb_opened")
    spark.sql(f"CREATE TABLE pb_opened (id INT, dt STRING) USING parquet PARTITIONED BY (dt) LOCATION '{location}'")
    values = ", ".join(f"({i}, '{day}')" for i, day in enumerate(DAYS, start=1))
    spark.sql(f"INSERT INTO pb_opened VALUES {values}")  # noqa: S608
    yield "pb_opened"
    spark.sql("DROP TABLE IF EXISTS pb_opened")


def files_opened(spark, query: str) -> int:
    """The number of files the scan reported opening, or 0 when there is no scan."""
    rows = spark.sql(f"EXPLAIN ANALYZE {query}").collect()
    text = "\n".join(str(row[i]) for row in rows for i in range(len(row)))
    scans = [line for line in text.split("\n") if "DataSourceExec" in line]
    total = 0
    for scan in scans:
        match = re.search(r"files_ranges_pruned_statistics=(\d+(?:\.\d+)?)\s*(K?)", scan)
        assert match, f"no file metric in scan: {scan[:200]}"
        count = float(match.group(1)) * (1000 if match.group(2) == "K" else 1)
        total += round(count)
    return total


def test_bounds_query_opens_no_file(spark, table):
    # The aggregate is answered from directory names, so there is no scan at all.
    assert files_opened(spark, f"SELECT max(dt) FROM {table}") == 0  # noqa: S608


def test_subquery_filter_opens_only_the_matching_partition(spark, table):
    # The subquery is inlined while planning, so the scan is handed one partition and
    # opens one file rather than all four.
    query = f"SELECT count(*) FROM {table} WHERE dt = (SELECT max(dt) FROM {table})"  # noqa: S608
    assert files_opened(spark, query) == 1


def test_window_function_still_opens_every_file(spark, table):
    # The control: `rank()` needs every row, so nothing is saved and the count must
    # stay at the number of partitions.
    query = f"SELECT dt FROM (SELECT dt, rank() OVER (ORDER BY dt DESC) rk FROM {table}) WHERE rk = 1"  # noqa: S608
    assert files_opened(spark, query) == len(DAYS)
