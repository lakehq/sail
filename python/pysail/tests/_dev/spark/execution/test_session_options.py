# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail sends DataFusion session options to cluster workers")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        }
    ) as server:
        yield server.remote


# Workers parse the options that the driver session sets, including values of enum and list types.
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("datafusion.explain.format", "tree"),
        ("datafusion.explain.analyze_categories", "rows,bytes"),
        ("datafusion.format.duration_format", "iso8601"),
        ("datafusion.spark.map_key_dedup_policy", "LAST_WIN"),
    ],
)
def test_cluster_job_runs_with_session_option(remote, key, value):
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        spark.sql(f"SET {key} = '{value}'").collect()
        rows = spark.range(0, 100, 1, 2).selectExpr("id % 3 AS k").groupBy("k").count().collect()
    assert sorted((row.k, row["count"]) for row in rows) == [(0, 34), (1, 33), (2, 33)]
