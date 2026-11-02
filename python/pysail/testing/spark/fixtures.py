import os
import platform
import time

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory


@pytest.fixture(scope="package")
def remote():
    """Creates a Spark Connect server if there is not one already running
    whose address is set in the `SPARK_REMOTE` environment variable.

    :yields: The remote address of the Spark Connect server to connect to.
    """
    with spark_connect_server() as server:
        yield server.remote


@pytest.fixture(scope="module")
def spark(remote):
    """Create and configure a Spark Session to be used in the tests.
    After the tests are finished, the Spark Session is stopped.

    :param remote: The remote address of the Spark Connect server to connect to.
    :yields: A Spark Session configured for the tests.
    """
    with spark_session_factory(remote) as sessions:
        yield sessions.create()


@pytest.fixture(scope="module", autouse=True)
def spark_doctest(doctest_namespace, spark):
    # The Spark session is scoped to each module, so that the registered
    # temporary views and UDFs do not interfere with each other.
    doctest_namespace["spark"] = spark


@pytest.fixture
def session_timezone(spark, request):
    tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", request.param)
    yield
    spark.conf.set("spark.sql.session.timeZone", tz)


@pytest.fixture
def local_timezone(request):
    if platform.system() == "Windows":
        pytest.skip("`time.tzset()` is not available on Windows")

    tz = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if tz is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = tz
    time.tzset()
