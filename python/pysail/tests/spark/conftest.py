import os
import time

import pyspark.sql.connect.session
import pytest
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from pysail.spark import SparkConnectServer


@pytest.fixture(scope="session")
def remote():
    if r := os.environ.get("SPARK_REMOTE"):
        yield r
    else:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()


@pytest.fixture(scope="module")
def sail(remote):
    if "SPARK_LOCAL_REMOTE" in os.environ:
        del os.environ["SPARK_LOCAL_REMOTE"]
    sail = SparkSession.builder.remote(remote).appName("Sail").getOrCreate()
    configure_spark_session(sail)
    patch_spark_connect_session(sail)
    yield sail
    sail.stop()


@pytest.fixture(scope="module")
def spark():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_REMOTE"] = "true"
    sc = SparkContext("local", "Spark")
    spark = SparkSession(sc)
    configure_spark_session(spark)
    yield spark
    spark.stop()
    if "SPARK_LOCAL_REMOTE" in os.environ:
        del os.environ["SPARK_LOCAL_REMOTE"]


def configure_spark_session(session: SparkSession):
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    # Enable Arrow to avoid data type errors when creating Spark DataFrame from Pandas.
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def patch_spark_connect_session(session: pyspark.sql.connect.session.SparkSession):
    """
    Patch the Spark Connect session to avoid deadlock when closing the session.
    """
    f = session._client.close  # noqa: SLF001

    def close():
        if session._client._closed:  # noqa: SLF001
            return
        return f()

    session._client.close = close  # noqa: SLF001


@pytest.fixture(scope="module", autouse=True)
def sail_doctest(doctest_namespace, sail):
    # The Spark session is scoped to each module, so that the registered
    # temporary views and UDFs do not interfere with each other.
    doctest_namespace["spark"] = sail


@pytest.fixture
def session_timezone(sail, request):
    tz = sail.conf.get("spark.sql.session.timeZone")
    sail.conf.set("spark.sql.session.timeZone", request.param)
    yield
    sail.conf.set("spark.sql.session.timeZone", tz)


@pytest.fixture
def local_timezone(request):
    tz = os.environ.get("TZ")
    os.environ["TZ"] = request.param
    time.tzset()
    yield
    if tz is None:
        os.environ.pop("TZ")
    else:
        os.environ["TZ"] = tz
    time.tzset()
