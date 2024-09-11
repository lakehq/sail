import os

import pytest
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
def spark(remote):
    spark = SparkSession.builder.remote(remote).getOrCreate()
    yield spark
    spark.stop()
