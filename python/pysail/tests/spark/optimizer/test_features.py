from __future__ import annotations

import pytest
from pytest_bdd import scenarios

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="module")
def remote():
    """Override the global remote fixture to enable join reorder for this module."""
    # `optimizer.enable_join_reorder` (experimental) is configured via env at startup.
    with spark_connect_server(envs={"SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": "true"}) as server:
        yield server.remote


scenarios("features")
