import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DoubleType, LongType, StructField, StructType


# Spark 4.2 random.sql and RDG.nullable: NULL seeds still produce non-null doubles.
@pytest.mark.parametrize("function", ["rand", "random"])
@pytest.mark.parametrize(
    "seed",
    [None, "0", "NULL", "CAST(NULL AS INT)", "CAST(NULL AS BIGINT)"],
    ids=["unseeded", "seeded", "null", "null-int", "null-bigint"],
)
def test_random_non_nullable_output(spark, function, seed):
    args = [] if seed is None else [F.expr(seed)]
    expression = F.call_function(function, *args).alias("r")
    expected = StructType([StructField("id", LongType(), False), StructField("r", DoubleType(), False)])

    result = spark.range(3, numPartitions=1).select("id", expression)
    rows = sorted(result.collect(), key=lambda row: row.id)
    assert [row.id for row in rows] == [0, 1, 2]
    assert all(row.r is not None and 0 <= row.r < 1 for row in rows)
    if seed == "0":
        assert [row.r for row in rows] == [0.7604953758285915, 0.5234194256885571, 0.0953472826424725]
    assert result.schema == expected

    empty = spark.range(0, numPartitions=1).select("id", expression)
    assert empty.collect() == []
    assert empty.schema == expected


# Spark AnalysisErrorSuite SPARK-33909: invalid types and non-foldable seeds reject.
@pytest.mark.parametrize("function", ["rand", "random"])
@pytest.mark.parametrize(
    ("args", "error"),
    [
        (("1.0",), "expects Integer Type for seed|DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        (("'1'",), "expects Integer Type for seed|DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        (("id",), "expects a scalar seed argument|SEED_EXPRESSION_IS_UNFOLDABLE"),
        (("0", "1"), "requires 0 to 1 arguments, got 2|WRONG_NUM_ARGS"),
        (("NULL", "1"), "requires 0 to 1 arguments, got 2|WRONG_NUM_ARGS"),
    ],
    ids=["decimal", "string", "column", "arity", "null-arity"],
)
def test_random_invalid_seed_rejected(spark, function, args, error):
    with pytest.raises(Exception, match=error):
        spark.range(3, numPartitions=1).select(F.call_function(function, *(F.expr(arg) for arg in args))).collect()
