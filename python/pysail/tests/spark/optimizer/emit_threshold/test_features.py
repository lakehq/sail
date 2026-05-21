from __future__ import annotations

import os

import pytest
from pytest_bdd import scenarios

from pysail.spark import SparkConnectServer
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="module")
def remote():
    """Run a dedicated Sail server with join reorder enabled and a tiny emit threshold.

    Setting `optimizer.join_reorder.emit_threshold` to 1 forces the DPhyp enumerator to trip
    its fallback path almost immediately on any multi-relation join, which exercises the
    greedy left-deep reconstruction code path. The tests in this module assert that the
    query result remains correct regardless of which path the optimizer takes.
    """
    overrides = {
        "SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": "true",
        "SAIL_OPTIMIZER__JOIN_REORDER__EMIT_THRESHOLD": "1",
    }
    originals = {key: os.environ.get(key) for key in overrides}
    for key, value in overrides.items():
        os.environ[key] = value

    try:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        for key, original in originals.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


scenarios("features")
