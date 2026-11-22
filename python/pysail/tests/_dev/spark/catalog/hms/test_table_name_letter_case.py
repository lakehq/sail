# SPDX-License-Identifier: Apache-2.0

"""The Hive Metastore ignores the letter case of table names.

One statement can name a table in two ways that differ in letter case. Sail asks the
metastore for each name, and the metastore returns the same table for both. The
statement then uses the first answer for both names, so that it reads and writes
one version of the table.
"""

from __future__ import annotations

import pytest

_BUCKET = "hms-warehouse"


@pytest.mark.parametrize("table_format", ["iceberg", "delta"])
def test_table_names_that_differ_in_letter_case_share_one_table_load(spark, table_format):
    database = f"letter_case_{table_format}"
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    spark.sql(f"CREATE DATABASE {database} LOCATION 's3://{_BUCKET}/{database}'")
    try:
        spark.sql(f"CREATE TABLE {database}.t (id INT) USING {table_format}")
        spark.sql(f"INSERT INTO {database}.t VALUES (1), (2)").collect()

        # The plan of the write prints the name of the table load that the write uses.
        # The statement reads `t` first, so the write to `T` uses the load of `t`.
        plan = spark.sql(f"EXPLAIN EXTENDED INSERT INTO {database}.T SELECT id + 10 FROM {database}.t").collect()[0][0]
        assert f'catalog_table: ["sail", "{database}", "t"]' in plan
        assert f'catalog_table: ["sail", "{database}", "T"]' not in plan

        spark.sql(f"INSERT INTO {database}.T SELECT id + 10 FROM {database}.t").collect()
        rows = spark.sql(
            f"SELECT a.id FROM {database}.t a JOIN {database}.T b ON a.id = b.id ORDER BY a.id"  # noqa: S608
        ).collect()
        assert [row.id for row in rows] == [1, 2, 11, 12]
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
