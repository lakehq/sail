import time

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql.types import LongType, Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

pytestmark = pytest.mark.skip(reason="slow tests that should be run manually")


@pytest.fixture(scope="module")
def bad_sessions():
    envs = {
        "SAIL_SPARK__SESSION_TIMEOUT_SECS": "2",
        "SAIL_SPARK__EXECUTION_HEARTBEAT_INTERVAL_SECS": "4",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions


@pytest.fixture(scope="module")
def good_sessions():
    envs = {
        "SAIL_SPARK__SESSION_TIMEOUT_SECS": "2",
        "SAIL_SPARK__EXECUTION_HEARTBEAT_INTERVAL_SECS": "1",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions


@pytest.fixture(scope="module")
def identity():
    @F.udf(LongType())
    def _identity(value):
        time.sleep(5)
        return value

    return _identity


@pytest.fixture(scope="module")
def data(identity):
    def _data(spark):
        return spark.range(1).select(identity("id").alias("id"))

    return _data


def test_query_fails_when_session_timeout_precedes_heartbeat(bad_sessions, data):
    spark = bad_sessions.create()
    with pytest.raises(SparkConnectGrpcException):
        data(spark).collect()


def test_command_fails_when_session_timeout_precedes_heartbeat(bad_sessions, data):
    spark = bad_sessions.create()
    with pytest.raises(SparkConnectGrpcException):
        data(spark).write.format("noop").mode("overwrite").save()


def test_query_succeeds_when_heartbeat_precedes_session_timeout(good_sessions, data):
    spark = good_sessions.create()
    assert data(spark).collect() == [Row(id=0)]
    # Send another request to make sure the session is still alive.
    assert spark.range(1).count() == 1


def test_command_succeeds_when_heartbeat_precedes_session_timeout(good_sessions, data):
    spark = good_sessions.create()
    data(spark).write.format("noop").mode("overwrite").save()
    # Send another request to make sure the session is still alive.
    assert spark.range(1).count() == 1
