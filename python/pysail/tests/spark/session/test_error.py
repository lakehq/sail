import pytest
from pyspark.errors import AnalysisException


# Adapted from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectSessionTests.test_error_stack_trace; preserves the portable error behavior.
@pytest.mark.parametrize("enabled", [False, True])
def test_analysis_error_with_stacktrace_config(spark, enabled):
    key = "spark.sql.pyspark.jvmStacktrace.enabled"
    original = spark.conf.get(key)
    try:
        spark.conf.set(key, str(enabled).lower())
        with pytest.raises(AnalysisException, match="missing_column") as error:
            spark.sql("SELECT missing_column").collect()
        if not enabled:
            assert "JVM stacktrace" not in str(error.value)
        assert spark.sql("SELECT 1 AS value").first().value == 1
    finally:
        spark.conf.set(key, original)
