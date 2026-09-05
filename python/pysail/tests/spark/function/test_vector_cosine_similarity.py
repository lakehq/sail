import math

import pytest


def test_vector_cosine_similarity(spark):
    assert spark.sql("SELECT vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F))").first()[
        0
    ] == pytest.approx(0.97463185)
    assert spark.sql("SELECT vector_cosine_similarity(array(1.0F, 0.0F), array(0.0F, 1.0F))").first()[
        0
    ] == pytest.approx(0.0)
    assert spark.sql("SELECT vector_cosine_similarity(array(1.0F, 0.0F), array(-1.0F, 0.0F))").first()[
        0
    ] == pytest.approx(-1.0)


def test_vector_cosine_similarity_null_cases(spark):
    assert spark.sql("SELECT vector_cosine_similarity(NULL, array(1.0F, 2.0F))").first()[0] is None
    assert spark.sql("SELECT vector_cosine_similarity(array(1.0F, 2.0F), NULL)").first()[0] is None
    assert (
        spark.sql(
            "SELECT vector_cosine_similarity(CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>))"
        ).first()[0]
        is None
    )
    assert spark.sql("SELECT vector_cosine_similarity(array(0.0F, 0.0F), array(1.0F, 2.0F))").first()[0] is None
    assert (
        spark.sql("SELECT vector_cosine_similarity(array(1.0F, CAST(NULL AS FLOAT)), array(1.0F, 2.0F))").first()[0]
        is None
    )


def test_vector_cosine_similarity_extreme_values(spark):
    assert spark.sql("SELECT vector_cosine_similarity(array(3.0e19F, 4.0e19F), array(3.0e19F, 4.0e19F))").first()[
        0
    ] == pytest.approx(1.0)
    assert spark.sql("SELECT vector_cosine_similarity(array(1.0e-23F, 0.0F), array(1.0e-23F, 0.0F))").first()[
        0
    ] == pytest.approx(1.0)
    assert math.isnan(
        spark.sql("SELECT vector_cosine_similarity(array(float('inf'), 1.0F), array(1.0F, 1.0F))").first()[0]
    )


def test_vector_cosine_similarity_rejects_dimension_mismatch(spark):
    with pytest.raises(Exception, match="matching dimensions"):
        spark.sql("SELECT vector_cosine_similarity(array(1.0F, 2.0F), array(1.0F))").collect()


def test_vector_cosine_similarity_rejects_non_float_vectors(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_cosine_similarity(array(1.0D), array(1.0D))").collect()
