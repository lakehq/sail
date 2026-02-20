from __future__ import annotations

import os

import pytest
from pytest_bdd import scenarios

from pysail.spark import SparkConnectServer
from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="module")
def remote():
    """Override the global remote fixture to enable join reorder for this module."""
    original_enable_join_reorder = os.environ.get("SAIL_OPTIMIZER__ENABLE_JOIN_REORDER")

    # `optimizer.enable_join_reorder` (experimental) is configured via env at startup.
    os.environ["SAIL_OPTIMIZER__ENABLE_JOIN_REORDER"] = "true"

    try:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        if original_enable_join_reorder is None:
            os.environ.pop("SAIL_OPTIMIZER__ENABLE_JOIN_REORDER", None)
        else:
            os.environ["SAIL_OPTIMIZER__ENABLE_JOIN_REORDER"] = original_enable_join_reorder


scenarios("features")
