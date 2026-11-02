from __future__ import annotations

import pytest

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="package")
def remote():
    with spark_connect_server(envs={"SAIL_OPTIMIZER__ENABLE_JOIN_REORDER": "true"}) as server:
        yield server.remote
