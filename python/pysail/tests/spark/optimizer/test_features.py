from __future__ import annotations

import pytest
from pytest_bdd import scenarios

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="package")
def remote():
    with spark_connect_server(envs={"SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": "true"}) as server:
        yield server.remote


scenarios("features")
