import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as sf


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


@pytest.mark.parametrize("function", ["vector_norm", "vector_normalize"])
@pytest.mark.parametrize("degree", [1.0, 2.0, float("inf"), None, 0.0, 3.0, float("nan"), -float("inf")])
@pytest.mark.parametrize(
    "vector", [None, [1.0, None], [], [3.0, 4.0]], ids=["outer-null", "nested-null", "empty", "normal"]
)
def test_vector_norm_degree_and_null_precedence(spark, function, degree, vector):
    data = [(vector, degree)] * 4
    df = spark.createDataFrame(data, "v ARRAY<FLOAT>, d FLOAT").repartition(4)
    if vector is not None and degree is not None and degree not in (1.0, 2.0, float("inf")):
        with pytest.raises(Exception, match="degree must be"):
            df.selectExpr(f"{function}(v, d)").collect()
        return
    if vector is None or degree is None or None in vector:
        expected = None
    elif not vector:
        expected = 0.0 if function == "vector_norm" else []
    else:
        norm = {1.0: 7.0, 2.0: 5.0, float("inf"): 4.0}[degree]
        expected = norm if function == "vector_norm" else [value / norm for value in vector]
    actual = df.selectExpr(f"{function}(v, d)").collect()
    assert len(actual) == len(data)
    for row in actual:
        if expected is None:
            assert row[0] is None
        else:
            assert row[0] == pytest.approx(expected)


@pytest.mark.parametrize("entry", ["sql", "selectExpr"])
def test_vector_norm_output_names(spark, entry):
    expressions = [
        "vector_norm(array(3F, 4F))",
        "vector_norm(array(3F, 4F), 2F)",
        "vector_norm(array(3F, 4F), 1F)",
        "vector_norm(array(3F, 4F)) AS default_norm",
        "vector_norm(array(3F, 4F), 2F) AS explicit_norm",
    ]
    df = spark.sql("SELECT " + ", ".join(expressions)) if entry == "sql" else spark.range(1).selectExpr(*expressions)
    assert df.columns == [
        "vector_norm(array(3.0, 4.0), 2.0)",
        "vector_norm(array(3.0, 4.0), 2.0)",
        "vector_norm(array(3.0, 4.0), 1.0)",
        "default_norm",
        "explicit_norm",
    ]
    assert list(df.first()) == [5.0, 5.0, 7.0, 5.0, 5.0]


@pytest.mark.parametrize("use_arrow", [False, True])
def test_vector_norm_inline_udf_output_name(spark, use_arrow):
    @sf.udf("float", useArrow=use_arrow)
    def vector_norm(value):
        return value + 1.0

    source = spark.createDataFrame([(3.0,)] * 4, "v FLOAT").repartition(4)
    df = source.select(vector_norm("v"), vector_norm("v").alias("named_udf"))
    assert df.dtypes == [("vector_norm(v)", "float"), ("named_udf", "float")]
    assert all(field.nullable for field in df.schema)
    assert df.collect() == [(4.0, 4.0)] * 4
    assert df.select("vector_norm(v)").collect() == [(4.0,)] * 4


def test_vector_norm_registered_udf_output_name(spark, remote):
    source = spark.createDataFrame([([3.0, 4.0],)] * 4, "v ARRAY<FLOAT>").repartition(4)
    spark.udf.register("vector_norm", lambda value: value[0] + 1.0, "float")
    try:
        # Spark prioritizes built-ins; Sail permits session UDFs to shadow them.
        name, value = ("vector_norm(v, 2.0)", 5.0) if remote.startswith("local") else ("vector_norm(v)", 4.0)
        df = source.selectExpr("vector_norm(v)")
        assert df.dtypes == [(name, "float")]
        assert df.schema[0].nullable
        assert df.select(f"`{name}`").collect() == [(value,)] * 4
    finally:
        spark.sql("DROP TEMPORARY FUNCTION vector_norm")

    df = source.selectExpr("vector_norm(v)")
    assert df.columns == ["vector_norm(v, 2.0)"]
    assert df.collect() == [(5.0,)] * 4


def test_vector_norm_rejects_invalid_types(spark):
    with pytest.raises(Exception, match=r"ARRAY<FLOAT>"):
        spark.sql("SELECT vector_norm(array(1.0D))").collect()
    with pytest.raises(Exception, match="FLOAT"):
        spark.sql("SELECT vector_norm(array(1.0F), 2.0D)").collect()
