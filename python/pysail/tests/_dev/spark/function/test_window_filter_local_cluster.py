"""Aggregate functions with FILTER and WITHIN GROUP over a window on cluster workers.

The physical plan crosses the distributed codec, so these tests cover the filter of window
aggregates (which DataFusion does not serialize, so Sail passes it as an argument of a
wrapping aggregate function) and the WITHIN GROUP ordering of window functions.
The expected values were checked against JVM Spark 4.2.
"""

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode")

VALUES = (
    "VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), "
    "(6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)"
)


@pytest.fixture(scope="module")
def local_cluster_spark():
    with (
        spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        yield sessions.create()


def rows(df):
    return [tuple(row) for row in df.collect()]


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        pytest.param(
            "count(s) FILTER (WHERE id > 1) OVER (PARTITION BY id % 2)",
            [2, 2, 2, 2, 2, 2],
            id="partition",
        ),
        pytest.param(
            "sum(v) FILTER (WHERE id % 2 = 1) OVER (ORDER BY id)",
            [10, 10, 40, 40, 90, 90],
            id="growing",
        ),
        pytest.param(
            "sum(v) FILTER (WHERE id <> 3) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            [30, 30, 60, 90, 90, 50],
            id="sliding",
        ),
        pytest.param(
            "min(v) FILTER (WHERE id > 2) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
            [None, None, 30, 30, 30, 40],
            id="sliding-min",
        ),
        pytest.param(
            "sum(v) FILTER (WHERE v > 15) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
            [None, 20, 50, 70, 90, 50],
            id="range",
        ),
        pytest.param(
            "listagg(s, '-') FILTER (WHERE id > 2) OVER (ORDER BY id)",
            [None, None, "c", "c-d", "c-d-e", "c-d-e"],
            id="listagg",
        ),
        pytest.param(
            "max_by(s, v) FILTER (WHERE id <> 5) OVER ()",
            ["d", "d", "d", "d", "d", "d"],
            id="max-by",
        ),
        pytest.param(
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE id > 2) OVER (PARTITION BY id % 2)",
            [40.0, 40.0, 40.0, 40.0, 40.0, 40.0],
            id="percentile-cont-filter",
        ),
        pytest.param(
            "percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE id < 5) OVER (PARTITION BY id % 2)",
            [30.0, 40.0, 30.0, 40.0, 30.0, 40.0],
            id="percentile-disc-desc-filter",
        ),
        pytest.param(
            "percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) OVER ()",
            [40.0, 40.0, 40.0, 40.0, 40.0, 40.0],
            id="percentile-cont-desc",
        ),
        pytest.param(
            "listagg(s, ',') WITHIN GROUP (ORDER BY s DESC) OVER (PARTITION BY id % 2)",
            ["e,c,a", "d,b", "e,c,a", "d,b", "e,c,a", "d,b"],
            id="listagg-within-group",
        ),
        pytest.param(
            "mode() WITHIN GROUP (ORDER BY v DESC) OVER (PARTITION BY id % 2)",
            [50, 40, 50, 40, 50, 40],
            id="mode-within-group",
        ),
    ],
)
def test_window_filter_and_within_group(local_cluster_spark, expr, expected):
    actual = local_cluster_spark.sql(f"SELECT id, {expr} AS a FROM {VALUES} ORDER BY id")  # noqa: S608
    assert [a for _, a in rows(actual)] == expected
