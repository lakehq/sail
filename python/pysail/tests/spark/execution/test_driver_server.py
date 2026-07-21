import socket

import pytest

from pysail.spark import SparkConnectServer
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        _, driver_port = sock.getsockname()
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__DRIVER_LISTEN_PORT": str(driver_port),
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def test_multiple_sessions_share_driver_server(spark, remote):
    with spark_session_factory(remote) as registry:
        other = registry.create()

        assert spark.sql("SELECT 1 AS value").first().value == 1
        assert other.sql("SELECT 2 AS value").first().value == 2  # noqa: PLR2004


def test_driver_gateway_startup_failure(monkeypatch):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        _, driver_port = sock.getsockname()
        monkeypatch.setenv("SAIL_MODE", "local-cluster")
        monkeypatch.setenv("SAIL_CLUSTER__DRIVER_LISTEN_PORT", str(driver_port))

        server = SparkConnectServer("127.0.0.1", 0)
        with pytest.raises(RuntimeError, match="Address already in use"):
            server.start(background=False)
