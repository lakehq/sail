import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="session", autouse=True)
def skip_if_jvm_spark():
    if is_jvm_spark():
        pytest.skip("Delta Lake tests for JVM Spark")
