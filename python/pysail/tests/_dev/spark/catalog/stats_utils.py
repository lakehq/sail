# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S608

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def check_iceberg_stats(spark, table, read_metadata, location=None, *, retains_history=True):
    clause = f"LOCATION '{escape_sql_string_literal(str(location))}'" if location else ""
    spark.sql(f"CREATE TABLE {table} (id INT, p INT) USING iceberg PARTITIONED BY (p) {clause}")
    procedures = [
        ("compute_table_stats", "statistics", ".stats"),
        ("compute_partition_stats", "partition-statistics", ".parquet"),
    ]
    try:
        for proc, _, _ in procedures:
            assert spark.sql(f"CALL system.{proc}('{table}')").collect() == []
        spark.sql(f"INSERT INTO {table} VALUES (1, 10), (2, 20)")
        old = read_metadata()["current-snapshot-id"]
        spark.sql(f"INSERT INTO {table} VALUES (3, 10)")
        before = read_metadata()
        latest = before["current-snapshot-id"]
        assert latest != old
        assert (old in {s["snapshot-id"] for s in before["snapshots"]}) == retains_history
        for proc, key, ext in procedures:
            expected = {}
            for snapshot, args in [(latest, ""), (old, f", {old}")]:
                if snapshot == old and not retains_history:
                    with pytest.raises(Exception, match="Snapshot not found"):
                        spark.sql(f"CALL system.{proc}('{table}'{args})").collect()
                    continue
                path = spark.sql(f"CALL system.{proc}('{table}'{args})").first()[0]
                assert path.endswith(ext)
                expected[snapshot] = path
            saved = read_metadata()
            assert {f["snapshot-id"]: f["statistics-path"] for f in saved[key]} == expected
            assert all(f["file-size-in-bytes"] > 0 for f in saved[key])
            assert {s["snapshot-id"] for s in saved["snapshots"]} == {s["snapshot-id"] for s in before["snapshots"]}
            if key == "partition-statistics":
                assert spark.sql(f"CALL system.{proc}('{table}')").first()[0] == expected[latest]
        assert spark.sql(f"SELECT sum(id) FROM {table}").first()[0] == 6
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
