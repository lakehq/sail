import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_vector_norm_supported_degrees(spark):
    row = spark.sql(
        """
        SELECT
          vector_norm(array(3.0F, 4.0F)) AS default_norm,
          vector_norm(array(-3.0F, 4.0F), 1.0F) AS l1_norm,
          vector_norm(array(3.0F, 4.0F), 2.0F) AS l2_norm,
          vector_norm(array(-3.0F, 4.0F), float('inf')) AS infinity_norm
        """
    ).first()

    assert row.default_norm == pytest.approx(5.0)
    assert row.l1_norm == pytest.approx(7.0)
    assert row.l2_norm == pytest.approx(5.0)
    assert row.infinity_norm == pytest.approx(4.0)


def test_vector_norm_with_parquet_large_list(spark, tmp_path):
    path = tmp_path / "large_list_vector.parquet"
    pq.write_table(
        pa.table({"v": pa.array([[3.0, 4.0]], type=pa.large_list(pa.float32()))}),
        path,
    )

    actual = spark.read.parquet(str(path)).selectExpr("vector_norm(v) AS norm").first()

    assert actual.norm == pytest.approx(5.0)


def test_vector_norm_null_and_empty_values(spark):
    row = spark.sql(
        """
        SELECT
          vector_norm(CAST(NULL AS ARRAY<FLOAT>), 2.0F) AS null_vector,
          vector_norm(array(1.0F, CAST(NULL AS FLOAT)), 2.0F) AS null_element,
          vector_norm(array(1.0F), CAST(NULL AS FLOAT)) AS null_degree,
          vector_norm(CAST(array() AS ARRAY<FLOAT>)) AS empty_vector
        """
    ).first()

    assert row.null_vector is None
    assert row.null_element is None
    assert row.null_degree is None
    assert row.empty_vector == 0.0


def test_vector_norm_extreme_values(spark):
    row = spark.sql(
        """
        SELECT
          vector_norm(array(3.0e38F, 3.0e38F), 1.0F) AS l1_overflow,
          vector_norm(array(3.0e19F, 4.0e19F), 2.0F) AS l2_overflow,
          vector_norm(array(1.0e-23F, 0.0F), 2.0F) AS l2_underflow,
          vector_norm(array(float('nan'), 1.0F), float('inf')) AS infinity_nan
        """
    ).first()

    assert math.isinf(row.l1_overflow)
    assert math.isinf(row.l2_overflow)
    assert row.l2_underflow == 0.0
    assert row.infinity_nan == 1.0


def test_vector_norm_rejects_invalid_degree(spark):
    with pytest.raises(Exception, match="degree must be"):
        spark.sql("SELECT vector_norm(array(1.0F), 3.0F)").collect()


def test_vector_norm_rejects_invalid_types(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_norm(array(1.0D))").collect()
    with pytest.raises(Exception, match="FLOAT"):
        spark.sql("SELECT vector_norm(array(1.0F), 2.0D)").collect()
