from __future__ import annotations

import pytest

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="package")
def remote():
    # The mirror of the `partition_bounds` package: the same queries against a server
    # left at its defaults, where `execution.partition_bounds_from_listing` is off.
    # Statistics are off here too, so that the plans show what the engine does without
    # either shortcut and a folded literal cannot be mistaken for the rule firing.
    envs = {"SAIL_EXECUTION__COLLECT_STATISTICS": "false"}
    with spark_connect_server(envs=envs) as server:
        yield server.remote