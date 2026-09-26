import pyarrow as pa
import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize("batch_size", [5000, 15000])
def test_local_projected_in_preserves_expression_state_across_batches(spark, batch_size):
    # SQL BDD cannot control the Arrow IPC batch boundaries of a local relation.
    batches = [
        pa.record_batch([pa.array(range(start, min(start + batch_size, 100000)), type=pa.int64())], names=["id"])
        for start in range(0, 100000, batch_size)
    ]
    spark.createDataFrame(pa.Table.from_batches(batches)).createOrReplaceTempView("local_in_candidates")
    try:
        actual = spark.sql(
            """
            SELECT SUM(CAST(present AS BIGINT)) AS matches,
              SUM(CAST(monotonic_present AS BIGINT)) AS monotonic_matches,
              SUM(CAST(partition_present AS BIGINT)) AS partition_matches
            FROM (
              SELECT id IN (
                SELECT CAST(rand(0) * 100000 AS BIGINT) FROM local_in_candidates
              ) AS present,
              id IN (
                SELECT monotonically_increasing_id() FROM local_in_candidates
              ) AS monotonic_present,
              id IN (
                SELECT spark_partition_id() FROM local_in_candidates
              ) AS partition_present
              FROM range(100000)
            )
            """
        ).collect()
        # Spark initializes local state once at partition zero for all input rows.
        assert actual == [Row(matches=63228, monotonic_matches=100000, partition_matches=1)]
    finally:
        spark.catalog.dropTempView("local_in_candidates")


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table input requires PySpark 4.0+")
@pytest.mark.parametrize("bad_index", [0, 49999])
def test_local_projected_in_evaluates_unused_errors_across_large_inputs(spark, bad_index):
    values = ["1"] * 50000
    values[bad_index] = "invalid"
    spark.createDataFrame(pa.table({"x": range(50000), "value": values})).createOrReplaceTempView("local_in_errors")
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        with pytest.raises(Exception, match=r"(?i)(cast_invalid_input|cannot cast string)"):
            spark.sql(
                """
                SELECT id IN (
                  SELECT x FROM (
                    SELECT x, CAST(value AS INT) AS unused FROM local_in_errors
                  )
                ) AS present FROM range(1)
                """
            ).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        spark.catalog.dropTempView("local_in_errors")
