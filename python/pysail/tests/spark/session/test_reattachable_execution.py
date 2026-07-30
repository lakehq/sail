import pyspark.sql.connect.functions as F  # noqa: N812
import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="module")
def fail():
    @F.udf("long")
    def _fail(_value):
        msg = "expected failure"
        raise RuntimeError(msg)

    return _fail


def test_execution_error_without_reattachment(remote, fail):
    """Error handling for reattachable execution (the default) is exercised frequently in other tests.
    This test ensures that the error can still be propagated for (non-default) non-reattachable execution.
    """
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        spark._client.disable_reattachable_execute()  # noqa: SLF001
        with pytest.raises(SparkConnectGrpcException, match="expected failure"):
            spark.range(1).select(fail("id")).write.format("noop").mode("overwrite").save()
        with pytest.raises(SparkConnectGrpcException, match="expected failure"):
            spark.range(1).select(fail("id")).collect()
