import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.functions import expr

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(
    is_jvm_spark() and pyspark_version() < (4, 2),
    reason="vector_sum requires Spark 4.2+ on JVM Spark",
)


def test_vector_sum_merges_partitions(spark):
    vectors = spark.sql(
        "SELECT CAST(id % 2 AS INT) AS k, array(CAST(id AS FLOAT), 1.0F) AS v FROM range(100)"
    ).repartition(4)
    result = vectors.selectExpr("vector_sum(v) AS total").first()
    assert result.total == [4950.0, 100.0]
    grouped = vectors.groupBy("k").agg(expr("vector_sum(v)").alias("total")).collect()
    assert {row.k: row.total for row in grouped} == {0: [2450.0, 50.0], 1: [2500.0, 50.0]}


def test_vector_sum_distinct_merges_partitions(spark):
    vectors = spark.sql(
        "SELECT CAST(id % 2 AS INT) AS k, array(CAST(id % 3 AS FLOAT)) AS v FROM range(120)"
    ).repartition(4)
    aggregates = [
        expr("vector_sum(DISTINCT v)").alias("distinct_total"),
        expr("vector_sum(v)").alias("total"),
    ]
    result = vectors.agg(*aggregates).first()
    assert result.distinct_total == [3.0]
    assert result.total == [120.0]
    grouped = vectors.groupBy("k").agg(*aggregates).collect()
    assert {row.k: (row.distinct_total, row.total) for row in grouped} == {
        0: ([3.0], [60.0]),
        1: ([3.0], [60.0]),
    }


def test_vector_sum_distinct_empty_and_null_vectors(spark):
    rows = spark.sql(
        """
        SELECT k, vector_sum(DISTINCT v) AS total
        FROM VALUES
          (1, CAST(NULL AS ARRAY<FLOAT>)),
          (1, array(CAST(NULL AS FLOAT))),
          (2, CAST(NULL AS ARRAY<FLOAT>)),
          (2, CAST(array() AS ARRAY<FLOAT>)),
          (2, CAST(array() AS ARRAY<FLOAT>))
        AS t(k, v)
        GROUP BY k ORDER BY k
        """
    ).collect()
    assert [row.total for row in rows] == [None, []]


def test_vector_sum_with_parquet_large_list(spark, tmp_path):
    path = tmp_path / "large_list_vectors.parquet"
    pq.write_table(
        pa.table({"v": pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.large_list(pa.float32()))}),
        path,
    )
    result = spark.read.parquet(str(path)).selectExpr("vector_sum(v) AS total").first()
    assert result.total == [4.0, 6.0]


def test_vector_sum_skips_nulls_and_preserves_empty_vectors(spark):
    result = spark.sql(
        """
        SELECT vector_sum(v) AS total
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (array(9.0F, CAST(NULL AS FLOAT), 10.0F)),
          (array(1.0F, 2.0F)),
          (array(3.0F, 4.0F))
        AS t(v)
        """
    ).first()
    assert result.total == [4.0, 6.0]
    result = spark.sql(
        """
        SELECT vector_sum(v) AS total
        FROM VALUES (CAST(array() AS ARRAY<FLOAT>)) AS t(v)
        """
    ).first()
    assert result.total == []


def test_vector_sum_empty_group_returns_null(spark):
    result = spark.sql("SELECT vector_sum(v) AS total FROM VALUES (array(1.0F)) AS t(v) WHERE false").first()
    assert result.total is None


def test_vector_sum_uses_float_arithmetic(spark):
    result = spark.sql(
        """
        SELECT vector_sum(v) AS total
        FROM VALUES (array(3.0e38F)), (array(3.0e38F)) AS t(v)
        """
    ).first()
    assert math.isinf(result.total[0])


def test_vector_sum_rejects_dimension_mismatch(spark):
    with pytest.raises(Exception, match=r"VECTOR_DIMENSION_MISMATCH|matching dimensions"):
        spark.sql("SELECT vector_sum(v) FROM VALUES (array(1.0F)), (array(1.0F, 2.0F)) AS t(v)").collect()


def test_vector_sum_rejects_non_float_vectors(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_sum(array(1.0D, 2.0D))").collect()
