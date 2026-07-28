import pytest

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="package")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server:
        yield server.remote
