import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture
def partitioned_table(spark, tmp_path):
    path = str(tmp_path / "data")
    spark.range(1).selectExpr("id AS key", "id AS value").write.partitionBy("key").mode("overwrite").save(path)
    spark.sql(
        "CREATE TABLE recover_partitions_tbl (key LONG, value LONG) "
        f"USING parquet OPTIONS (path '{escape_sql_string_literal(path)}') PARTITIONED BY (key)"
    )
    yield "recover_partitions_tbl"
    spark.sql("DROP TABLE IF EXISTS recover_partitions_tbl")


# Ported from Spark 3 (3.5.9): pyspark/sql/catalog.py, Catalog.recoverPartitions doctest.
# Sail discovers the partitions from the files when the table is read, so the data is visible
# before the partitions are recovered, which is a known difference.
def test_recover_partitions(spark, partitioned_table):
    assert spark.catalog.recoverPartitions(partitioned_table) is None
    assert spark.table(partitioned_table).collect() == [Row(value=0, key=0)]
    assert spark.sql(f"ALTER TABLE {partitioned_table} RECOVER PARTITIONS").collect() == []


def test_recover_partitions_requires_partitioned_table(spark, tmp_path):
    path = str(tmp_path / "data")
    spark.sql(
        "CREATE TABLE recover_partitions_flat (key LONG, value LONG) "
        f"USING parquet OPTIONS (path '{escape_sql_string_literal(path)}')"
    )
    try:
        with pytest.raises(AnalysisException, match="not a partitioned table"):
            spark.catalog.recoverPartitions("recover_partitions_flat")
    finally:
        spark.sql("DROP TABLE recover_partitions_flat")


def test_recover_partitions_requires_table(spark):
    spark.sql("SELECT 1 AS a").createOrReplaceTempView("recover_partitions_view")
    try:
        with pytest.raises(AnalysisException, match=r"is a (temp )?view"):
            spark.catalog.recoverPartitions("recover_partitions_view")
        with pytest.raises(AnalysisException, match=r"(cannot be|not) found"):
            spark.catalog.recoverPartitions("recover_partitions_missing")
    finally:
        spark.catalog.dropTempView("recover_partitions_view")
