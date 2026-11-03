import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def test_vector_l2_distance(spark):
    actual = spark.sql("SELECT vector_l2_distance(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F))").first()[0]
    assert actual == pytest.approx(5.196152)


def test_vector_l2_distance_with_parquet_large_list(spark, tmp_path):
    path = tmp_path / "large_list_vector.parquet"
    pq.write_table(
        pa.table({"v": pa.array([[1.0, 2.0]], type=pa.large_list(pa.float32()))}),
        path,
    )

    actual = spark.read.parquet(str(path)).selectExpr("vector_l2_distance(v, array(4.0F, 6.0F)) AS distance").first()

    assert actual.distance == pytest.approx(5.0)


def test_vector_l2_distance_null_and_empty_vectors(spark):
    assert spark.sql("SELECT vector_l2_distance(CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F))").first()[0] is None
    assert (
        spark.sql("SELECT vector_l2_distance(array(1.0F, CAST(NULL AS FLOAT)), array(1.0F, 2.0F))").first()[0] is None
    )
    assert (
        spark.sql("SELECT vector_l2_distance(CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>))").first()[0]
        == 0.0
    )


def test_vector_l2_distance_extreme_values(spark):
    assert math.isinf(spark.sql("SELECT vector_l2_distance(array(3.0e19F, 4.0e19F), array(0.0F, 0.0F))").first()[0])
    assert spark.sql("SELECT vector_l2_distance(array(1.0e-23F, 0.0F), array(0.0F, 0.0F))").first()[0] == 0.0


def test_vector_l2_distance_rejects_dimension_mismatch(spark):
    with pytest.raises(Exception, match="matching dimensions"):
        spark.sql("SELECT vector_l2_distance(array(1.0F, 2.0F), array(1.0F))").collect()


def test_vector_l2_distance_rejects_non_float_vectors(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_l2_distance(array(1.0D), array(1.0D))").collect()
