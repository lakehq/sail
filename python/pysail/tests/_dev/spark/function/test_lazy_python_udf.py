import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
import pytest
from pyspark.sql.types import Row

FAILURE = "lazy Python UDF evaluation probe"


@pytest.fixture
def fail_string(spark):  # noqa: ARG001
    @F.udf("string")
    def fail(_value):
        raise RuntimeError(FAILURE)

    return fail


@pytest.fixture
def null_string(spark):  # noqa: ARG001
    @F.udf("string")
    def null(_value):
        return None

    return null


@pytest.mark.parametrize("dynamic_null", [False, True])
def test_sequence_extracts_python_udf_before_null_short_circuit(
    spark,
    fail_string,
    dynamic_null,
):
    if dynamic_null:
        start = F.when(F.col("id") == 0, F.lit(None)).otherwise(F.lit(1)).cast("long")
    else:
        start = F.lit(None).cast("long")

    with pytest.raises(Exception, match=FAILURE):
        spark.range(1).select(F.sequence(start, fail_string("id").cast("long"))).collect()


def test_convert_timezone_extracts_python_udf_for_dynamic_null(
    spark,
    fail_string,
):
    source = F.when(F.col("id") == 0, F.lit(None)).otherwise(F.lit("UTC"))
    with pytest.raises(Exception, match=FAILURE):
        spark.range(1).select(
            F.convert_timezone(
                source,
                fail_string("id"),
                F.lit("2024-01-01").cast("timestamp_ntz"),
            )
        ).collect()


def test_convert_timezone_python_null_short_circuits_invalid_zone(
    spark,
    null_string,
):
    timestamp = F.lit("2024-01-01").cast("timestamp_ntz")
    result = spark.range(1).select(
        F.convert_timezone(
            null_string("id"),
            F.lit("Not/AZone"),
            timestamp,
        ).alias("null_source"),
        F.convert_timezone(
            F.lit("Not/AZone"),
            null_string("id"),
            timestamp,
        ).alias("null_target"),
        F.convert_timezone(
            None,
            F.lit("Not/AZone"),
            null_string("id").cast("timestamp_ntz"),
        ).alias("null_timestamp"),
    )

    assert result.collect() == [Row(null_source=None, null_target=None, null_timestamp=None)]


def test_convert_timezone_raw_null_timestamp_type(spark):
    result = spark.range(1).select(F.expr("convert_timezone('UTC', NULL)").alias("value"))

    assert result.collect() == [Row(value=None)]
    assert isinstance(result.schema["value"].dataType, T.TimestampNTZType)


@pytest.mark.parametrize(
    "timestamp_sql",
    [
        "CAST(NULL AS INT)",
        "CAST(CAST(NULL AS ARRAY<INT>) AS TIMESTAMP_NTZ)",
    ],
)
def test_convert_timezone_direct_null_does_not_bypass_timestamp_type_check(
    spark,
    timestamp_sql,
):
    with pytest.raises(
        Exception,
        match=r"(DATATYPE_MISMATCH\.(CAST_WITHOUT_SUGGESTION|UNEXPECTED_INPUT_TYPE)|cannot cast|Unsupported CAST|invalid NTZ timestamp type)",
    ):
        spark.range(1).select(
            F.convert_timezone(
                F.lit(None),
                F.lit("UTC"),
                F.expr(timestamp_sql),
            )
        ).collect()
