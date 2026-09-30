"""The mirror of the same measurement with the setting off.

Together with the enabled package these two files are the actual evidence for the
claim that the rule cuts the number of files a query opens: the same queries, the
same data, the same measurement, one number each.
"""

from __future__ import annotations

import pytest

from pysail.tests.spark.partition_bounds.test_files_opened import DAYS, files_opened
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture
def table(spark, tmp_path):
    location = tmp_path / "opened_off"
    spark.sql("DROP TABLE IF EXISTS pb_opened_off")
    spark.sql(
        f"CREATE TABLE pb_opened_off (id INT, dt STRING) USING parquet "
        f"PARTITIONED BY (dt) LOCATION '{location}'"
    )
    values = ", ".join(f"({i}, '{day}')" for i, day in enumerate(DAYS, start=1))
    spark.sql(f"INSERT INTO pb_opened_off VALUES {values}")
    yield "pb_opened_off"
    spark.sql("DROP TABLE IF EXISTS pb_opened_off")


def test_bounds_query_opens_every_file(spark, table):
    # Without the rule the aggregate reads the partition column out of every file.
    assert files_opened(spark, f"SELECT max(dt) FROM {table}") == len(DAYS)


def test_subquery_filter_opens_every_file_twice_over(spark, table):
    # The query holds two scans: one for the subquery and one for the outer filter.
    # Neither can be narrowed, since the subquery is only resolved during execution,
    # so each opens every partition and all but one row is discarded above the scan.
    #
    # With the setting on this whole query opens a single file, which is the contrast
    # `partition_bounds/test_files_opened.py` asserts.
    query = f"SELECT count(*) FROM {table} WHERE dt = (SELECT max(dt) FROM {table})"
    assert files_opened(spark, query) == 2 * len(DAYS)