import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.functions import expr


def test_vector_sum_merges_partitions(spark):
    vectors = spark.sql(
        "SELECT CAST(id % 2 AS INT) AS k, array(CAST(id AS FLOAT), 1.0F) AS v FROM range(100)"
    ).repartition(4)
    result = vectors.selectExpr("vector_sum(v) AS total").first()
    assert result.total == [4950.0, 100.0]
    grouped = vectors.groupBy("k").agg(expr("vector_sum(v)").alias("total")).collect()
    assert {row.k: row.total for row in grouped} == {0: [2450.0, 50.0], 1: [2500.0, 50.0]}


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
    with pytest.raises(Exception, match="matching dimensions"):
        spark.sql("SELECT vector_sum(v) FROM VALUES (array(1.0F)), (array(1.0F, 2.0F)) AS t(v)").collect()


def test_vector_sum_rejects_non_float_vectors(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_sum(array(1.0D, 2.0D))").collect()
