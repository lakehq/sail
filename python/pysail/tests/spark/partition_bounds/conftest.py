from __future__ import annotations

import pytest

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="package")
def remote():
    # Statistics collection is turned off so that these tests show what the rule
    # itself does. With statistics on, DataFusion already folds `max` over any column
    # to a literal, but only after opening the footer of every file of the table,
    # which is the cost the rule exists to avoid.
    envs = {
        "SAIL_EXECUTION__PARTITION_BOUNDS_FROM_LISTING": "true",
        "SAIL_EXECUTION__COLLECT_STATISTICS": "false",
    }
    with spark_connect_server(envs=envs) as server:
        yield server.remote
