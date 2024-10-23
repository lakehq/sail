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


@pytest.fixture(scope="session", autouse=True)
def sail(remote):
    SparkContext.getOrCreate().stop()
    if "SPARK_LOCAL_REMOTE" in os.environ:
        del os.environ["SPARK_LOCAL_REMOTE"]
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    sail = SparkSession.builder.remote(remote).appName("Sail").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    yield sail
    sail.stop()


@pytest.fixture(scope="session", autouse=False)
def spark():
    os.environ["SPARK_LOCAL_REMOTE"] = "true"
    SparkContext.getOrCreate().stop()
    sc = SparkContext("local", "Spark")
    spark = SparkSession(sc)
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.stop()
    if "SPARK_LOCAL_REMOTE" in os.environ:
        del os.environ["SPARK_LOCAL_REMOTE"]
