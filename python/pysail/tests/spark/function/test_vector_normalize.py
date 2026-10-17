import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_vector_normalize(spark):
    actual = spark.sql("SELECT vector_normalize(array(3.0F, 4.0F))").first()[0]
    assert actual == pytest.approx([0.6, 0.8])


def test_vector_normalize_supports_all_degrees(spark):
    row = spark.sql(
        """
        SELECT
          vector_normalize(array(3.0F, 4.0F), 1.0F) AS l1,
          vector_normalize(array(3.0F, 4.0F), 2.0F) AS l2,
          vector_normalize(array(3.0F, 4.0F), float('inf')) AS inf
        """
    ).first()
    assert row.l1 == pytest.approx([0.42857143, 0.5714286])
    assert row.l2 == pytest.approx([0.6, 0.8])
    assert row.inf == pytest.approx([0.75, 1.0])


def test_vector_normalize_with_parquet_large_list(spark, tmp_path):
    path = tmp_path / "large_list_vector.parquet"
    pq.write_table(
        pa.table({"v": pa.array([[3.0, 4.0]], type=pa.large_list(pa.float32()))}),
        path,
    )

    actual = spark.read.parquet(str(path)).selectExpr("vector_normalize(v) AS normalized").first()

    assert actual.normalized == pytest.approx([0.6, 0.8])


def test_vector_normalize_null_empty_and_zero_vectors(spark):
    assert spark.sql("SELECT vector_normalize(CAST(NULL AS ARRAY<FLOAT>))").first()[0] is None
    assert spark.sql("SELECT vector_normalize(array(1.0F, CAST(NULL AS FLOAT)))").first()[0] is None
    assert spark.sql("SELECT vector_normalize(array(1.0F, 2.0F), CAST(NULL AS FLOAT))").first()[0] is None
    assert spark.sql("SELECT vector_normalize(CAST(array() AS ARRAY<FLOAT>))").first()[0] == []
    assert spark.sql("SELECT vector_normalize(array(0.0F, 0.0F))").first()[0] is None


def test_vector_normalize_extreme_values(spark):
    assert spark.sql("SELECT vector_normalize(array(3.0e19F, 4.0e19F))").first()[0] == [0.0, 0.0]
    assert spark.sql("SELECT vector_normalize(array(1.0e-39F, 0.0F))").first()[0] is None
    actual = spark.sql("SELECT vector_normalize(array(float('nan'), 1.0F), float('inf'))").first()[0]
    assert all(math.isnan(value) for value in actual)


def test_vector_normalize_rejects_invalid_degree(spark):
    with pytest.raises(Exception, match=r"(?i)(INVALID_VECTOR_NORM_DEGREE|degree must be)"):
        spark.sql("SELECT vector_normalize(array(1.0F, 2.0F), 3.0F)").collect()


def test_vector_normalize_rejects_non_float_vectors(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_normalize(array(1.0D, 2.0D))").collect()
