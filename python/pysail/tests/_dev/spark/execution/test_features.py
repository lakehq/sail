import pytest
from pytest_bdd import scenarios

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module")
def remote():
    # These scenarios check query semantics, including exact runtime errors.
    # A retry after another partition publishes output can mask the original
    # error with an output-attempt conflict. Keep retry coverage in the separate
    # scheduler tests and fail on the first task attempt here.
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster", "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1"}) as server:
        yield server.remote


scenarios("features")
