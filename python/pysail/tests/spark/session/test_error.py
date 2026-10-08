import pytest
from pyspark.errors import AnalysisException

from pysail.testing.spark.utils.common import pyspark_version


# Adapted from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectSessionTests.test_error_stack_trace; preserves the portable error behavior.
@pytest.mark.parametrize("enabled", [False, True])
def test_analysis_error_with_stacktrace_config(spark, enabled):
    keys = ["spark.sql.pyspark.jvmStacktrace.enabled"]
    if pyspark_version() >= (4,):
        keys.append("spark.sql.connect.serverStacktrace.enabled")
    original = {key: spark.conf.get(key) for key in keys}
    try:
        for key in keys:
            spark.conf.set(key, str(enabled).lower())
        with pytest.raises(AnalysisException, match="missing_column") as error:
            spark.sql("SELECT missing_column").collect()
        if not enabled:
            assert "JVM stacktrace" not in str(error.value)
        assert spark.sql("SELECT 1 AS value").first().value == 1
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)
