import os

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


@pytest.fixture(scope="session")
def sail(remote):
    if "SPARK_LOCAL_REMOTE" in os.environ:
        del os.environ["SPARK_LOCAL_REMOTE"]
    sail = SparkSession.builder.remote(remote).appName("Sail").getOrCreate()
    configure_spark_session(sail)
    yield sail
    sail.stop()


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session", autouse=True)
def sail_doctest(doctest_namespace, sail):
    # TODO: we may need to isolate the Spark session for each doctest
    #   so that the registered temporary views and UDFs does not interfere
    #   with each other.
    doctest_namespace["spark"] = sail
