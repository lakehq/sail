import pytest
from pyspark.sql import SparkSession

from pysail.spark import SparkConnectServer


@pytest.fixture(scope="session", autouse=True)
def spark():
    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    host, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    yield spark
    spark.stop()
    server.stop()
