import pytest
from pyspark.errors import IllegalArgumentException


def test_truncate_hive_null_partition_scope_and_reinsert(spark):
    table = "truncate_hive_null_scope"
    spark.sql(f"CREATE TABLE {table} (id INT, p STRING, q INT) USING PARQUET PARTITIONED BY (p, q)")
    try:
        spark.sql(
            "INSERT INTO truncate_hive_null_scope VALUES "
            "(1, NULL, NULL), (2, '', 0), (3, 'null', 0), (4, 'keep', 0), (5, 'keep', 1)"
        )
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
            (1, None, None),
            (2, None, 0),
            (3, "null", 0),
            (4, "keep", 0),
            (5, "keep", 1),
        ]
        # Sail currently supports whole-table TRUNCATE only. Unsupported partition
        # specifications must not mutate either NULL or non-NULL partitions.
        for partition in ["p = NULL", "p = 'keep', q = 0"]:
            with pytest.raises(IllegalArgumentException, match="found PARTITION"):
                spark.sql(f"TRUNCATE TABLE {table} PARTITION ({partition})")
            assert [row.id for row in spark.table(table).orderBy("id").collect()] == [1, 2, 3, 4, 5]
        spark.sql(f"TRUNCATE TABLE {table}")
        assert spark.table(table).collect() == []
        spark.sql("INSERT INTO truncate_hive_null_scope VALUES (6, NULL, 0)")
        assert [tuple(row) for row in spark.table(table).collect()] == [(6, None, 0)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
