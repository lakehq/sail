import uuid

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture
def observer(remote):
    with spark_session_factory(remote) as sessions:
        yield sessions.create()


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_create_global_temp_view; both sessions use the same server.
def test_global_temp_view_is_shared_by_sessions_on_same_server(spark, observer):
    name = f"global_view_{uuid.uuid4().hex}"
    qualified = f"global_temp.{name}"
    try:
        spark.sql("SELECT 1 AS value").createGlobalTempView(name)
        assert observer.catalog.tableExists(qualified)
        assert observer.table(qualified).collect() == [Row(value=1)]
        with pytest.raises(AnalysisException):
            spark.sql("SELECT 2 AS value").createGlobalTempView(name)
        observer.sql("SELECT 2 AS value").createOrReplaceGlobalTempView(name)
        assert spark.table(qualified).collect() == [Row(value=2)]
        assert spark.catalog.dropGlobalTempView(name)
        assert not observer.catalog.tableExists(qualified)
    finally:
        spark.catalog.dropGlobalTempView(name)


# Regression extending Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_create_global_temp_view; local views must remain session-private.
def test_local_temp_view_is_private_to_session(spark, observer):
    name = f"local_view_{uuid.uuid4().hex}"
    try:
        spark.sql("SELECT 1 AS value").createTempView(name)
        assert spark.catalog.tableExists(name)
        assert not observer.catalog.tableExists(name)
    finally:
        spark.catalog.dropTempView(name)


# Regression extending Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_create_global_temp_view; verify the missing-view drop result.
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="dropGlobalTempView currently reports True even when the view does not exist",
    raises=AssertionError,
    strict=True,
)
def test_drop_missing_global_temp_view_returns_false(spark):
    assert not spark.catalog.dropGlobalTempView(f"missing_global_view_{uuid.uuid4().hex}")
