import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import PandasUDFType, pandas_udf


@pytest.mark.parametrize(
    "function",
    ["to_date", "try_to_date", "to_timestamp", "try_to_timestamp", "to_timestamp_ltz", "to_timestamp_ntz"],
)
@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize(("source", "codegen"), [("range", True), ("range", False), ("parquet", True)])
def test_datetime_parsing_checks_null_before_pattern(spark, remote, tmp_path, function, ansi, source, codegen):
    # Sail intentionally checks NULL string input before the pattern, in both ANSI modes.
    # Spark folds to_timestamp(CAST(NULL AS STRING), 'QQQ') to NULL, but the all-NULL
    # CASE/range query below raises with spark.sql.codegen.wholeStage=true and
    # spark.sql.codegen.factoryMode=FALLBACK: generated execution initializes the
    # constant formatter before checking row NULLs. The exact same query returns NULL
    # with wholeStage=false and factoryMode=NO_CODEGEN, which checks the input first.
    # This parity exception keeps Sail's results independent of that execution choice.
    jvm_spark = remote.startswith("local")
    config = {"spark.sql.ansi.enabled": ansi}
    if jvm_spark:
        config.update(
            {
                "spark.sql.codegen.wholeStage": "true" if codegen else "false",
                "spark.sql.codegen.factoryMode": "FALLBACK" if codegen else "NO_CODEGEN",
            }
        )
    original = {key: spark.conf.get(key) for key in config}
    try:
        for key, value in config.items():
            spark.conf.set(key, value)

        if source == "parquet":
            location = (tmp_path / "datetime_null_pattern").as_uri()
            spark.sql(
                f"INSERT OVERWRITE DIRECTORY '{location}' USING parquet "  # noqa: S608
                "SELECT CAST(NULL AS STRING) AS value FROM range(2)"
            )
            query = (
                f"SELECT {function}(value, 'QQQ') AS result "  # noqa: S608
                f"FROM parquet.`{location}`"
            )
        else:
            query = (
                f"SELECT {function}("  # noqa: S608
                "CASE WHEN id < 0 THEN '2024-01-01' ELSE CAST(NULL AS STRING) END, "
                "'QQQ') AS result FROM range(2)"
            )

        invalid_pattern = r"(?i)(Unrecognized datetime pattern|invalid datetime pattern)"
        if jvm_spark and codegen:
            with pytest.raises(Exception, match=invalid_pattern):
                spark.sql(query).collect()
        else:
            assert spark.sql(query).collect() == [Row(result=None), Row(result=None)]

        assert spark.sql(
            f"SELECT {function}(CAST(NULL AS STRING), 'QQQ') AS result FROM range(2)"  # noqa: S608
        ).collect() == [Row(result=None), Row(result=None)]
        with pytest.raises(Exception, match=invalid_pattern):
            spark.sql(f"SELECT {function}('2024-01-01', 'QQQ') AS result").collect()
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)


# Spark can eliminate known-NULL conversions before extracting Python iterators.
# Sail permits ordinary Python execution; NULL-first pattern/zone validation does
# not promise to suppress the expressions that compute those arguments.
@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize(
    ("query", "expected", "spark_error", "sail_error"),
    [
        pytest.param(
            "SELECT convert_timezone(CAST(NULL AS STRING), fail_iter_string(id), "
            "TIMESTAMP_NTZ '2024-01-01') AS result FROM range(1)",
            None,
            None,
            "datetime-iterator-evaluated",
            id="literal_null_iterator",
        ),
        pytest.param(
            "SELECT convert_timezone(CAST(concat(CAST(NULL AS STRING), 'UTC') AS STRING), "
            "fail_iter_string(id), TIMESTAMP_NTZ '2024-01-01') AS result FROM range(1)",
            None,
            None,
            "datetime-iterator-evaluated",
            id="computed_source_null_iterator",
        ),
        pytest.param(
            "SELECT convert_timezone(fail_iter_string(id), "
            "CAST(concat(CAST(NULL AS STRING), 'UTC') AS STRING), "
            "TIMESTAMP_NTZ '2024-01-01') AS result FROM range(1)",
            None,
            None,
            "datetime-iterator-evaluated",
            id="computed_target_null_iterator",
        ),
        pytest.param(
            "SELECT convert_timezone(CAST(NULL AS STRING), "
            "fail_iter_string(CAST(raise_error(concat('iterator-input-evaluated-', id)) AS BIGINT)), "
            "TIMESTAMP_NTZ '2024-01-01') AS result FROM range(1)",
            None,
            None,
            "datetime-iterator-evaluated",
            id="null_iterator_input_error",
        ),
        # Both engines extract this iterator before checking the NULL timestamp.
        pytest.param(
            "SELECT to_timestamp(CAST(NULL AS STRING), fail_iter_string(id)) AS result FROM range(1)",
            None,
            "datetime-iterator-evaluated",
            "datetime-iterator-evaluated",
            id="null_timestamp_iterator_format",
        ),
        pytest.param(
            "SELECT to_timestamp(DATE '2024-01-01', CAST(convert_timezone("
            "CAST(concat(CAST(NULL AS STRING), 'UTC') AS STRING), fail_iter_string(id), "
            "TIMESTAMP_NTZ '2024-01-01') AS STRING)) AS result FROM range(1)",
            datetime.datetime(2024, 1, 1),  # noqa: DTZ001 - PySpark returns naive timestamps.
            None,
            "datetime-iterator-evaluated",
            id="nested_typed_format_iterator",
        ),
        pytest.param(
            "SELECT to_timestamp(DATE '2024-01-01', CAST(convert_timezone("
            "CAST(CAST(concat(CAST(NULL AS STRING), '1') AS INT) AS STRING), fail_iter_string(id), "
            "TIMESTAMP_NTZ '2024-01-01') AS STRING)) AS result FROM range(1)",
            datetime.datetime(2024, 1, 1),  # noqa: DTZ001 - PySpark returns naive timestamps.
            None,
            "datetime-iterator-evaluated",
            id="nested_typed_format_cast_iterator",
        ),
        pytest.param(
            "SELECT convert_timezone('UTC', fail_iter_string(id), TIMESTAMP_NTZ '2024-01-01') AS result FROM range(1)",
            None,
            "datetime-iterator-evaluated",
            "datetime-iterator-evaluated",
            id="live_iterator_error",
        ),
        pytest.param(
            "SELECT convert_timezone(CAST(NULL AS STRING), fail_iter_string(id), "
            "TIMESTAMP_NTZ '2024-01-01') AS result FROM range(2) GROUP BY id % 2",
            None,
            "MISSING_AGGREGATION",
            "(?i)No field named",
            id="null_iterator_grouping_error",
        ),
    ],
)
def test_datetime_null_validation_before_iterator_execution(
    spark, remote, ansi, query, expected, spark_error, sail_error
):
    @pandas_udf("string", PandasUDFType.SCALAR_ITER)
    def fail_iter_string(_batches):
        message = "datetime-iterator-evaluated"
        raise RuntimeError(message)

    spark.udf.register("fail_iter_string", fail_iter_string)
    original = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        error = spark_error if remote.startswith("local") else sail_error
        if error:
            with pytest.raises(Exception, match=error):
                spark.sql(query).collect()
        else:
            assert spark.sql(query).collect() == [Row(result=expected)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original)
