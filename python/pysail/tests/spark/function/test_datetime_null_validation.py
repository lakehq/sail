import pytest
from pyspark.sql import Row


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
