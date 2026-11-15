"""`array_agg` and its Spark aliases on cluster workers.

The physical plan crosses the distributed codec, so these tests cover the paths that
workers run: ordered aggregation (including the pre-ordered input fast path),
the DISTINCT groups accumulator behind `collect_set`, and the retract path of sliding
window frames. The expected values were checked by hand and against JVM Spark 4.2
where Spark supports the syntax.
"""

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode")


@pytest.fixture(scope="module")
def local_cluster_spark():
    with (
        spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        yield sessions.create()


@pytest.fixture(scope="module")
def ordered(local_cluster_spark):
    spark = local_cluster_spark
    df = spark.createDataFrame(
        [(1, 3, "c"), (1, 1, "a"), (2, 2, "e"), (1, 2, "b"), (2, 1, "d"), (2, 3, "f"), (1, 4, None)],
        "g int, y int, x string",
    )
    df.repartition(3).createOrReplaceTempView("array_agg_ordered")
    yield spark
    spark.catalog.dropTempView("array_agg_ordered")


def rows(df):
    return [tuple(row) for row in df.collect()]


def test_ordered_listagg(ordered):
    actual = ordered.sql(
        """
        SELECT
          g,
          listagg(x, ',') WITHIN GROUP (ORDER BY y) AS asc_values,
          listagg(x, ',') WITHIN GROUP (ORDER BY y DESC) AS desc_values
        FROM array_agg_ordered
        GROUP BY g
        ORDER BY g
        """
    )
    assert rows(actual) == [(1, "a,b,c", "c,b,a"), (2, "d,e,f", "f,e,d")]


def test_ordered_collect_list_and_array_agg(ordered):
    # `WITHIN GROUP` on `collect_list` and `array_agg` is a Sail extension.
    actual = ordered.sql(
        """
        SELECT
          g,
          collect_list(x) WITHIN GROUP (ORDER BY y) AS asc_values,
          array_agg(x) WITHIN GROUP (ORDER BY y DESC) AS desc_values
        FROM array_agg_ordered
        GROUP BY g
        ORDER BY g
        """
    )
    assert rows(actual) == [(1, ["a", "b", "c"], ["c", "b", "a"]), (2, ["d", "e", "f"], ["f", "e", "d"])]


@pytest.mark.parametrize(
    ("direction", "expected"),
    [
        ("ASC", [(0, [0, 3, 6, 9]), (1, [1, 4, 7, 10]), (2, [2, 5, 8, 11])]),
        ("DESC", [(0, [9, 6, 3, 0]), (1, [10, 7, 4, 1]), (2, [11, 8, 5, 2])]),
    ],
)
def test_ordered_collect_list_over_pre_sorted_input(local_cluster_spark, direction, expected):
    spark = local_cluster_spark
    query = f"""
        SELECT id % 3 AS g, collect_list(id) WITHIN GROUP (ORDER BY id {direction}) AS ids
        FROM range(0, 12, 1, 3)
        GROUP BY id % 3
    """  # noqa: S608
    df = spark.sql(query)
    assert sorted(rows(df)) == expected

    # The partial aggregate reads input that is already sorted on the ORDER BY key, so
    # DataFusion's `OptimizeAggregateOrder` marks the aggregate as pre-ordered
    # (`with_beneficial_ordering(true)`) before the plan is sent to the workers.
    plan = [line.strip() for line in df._explain_string().splitlines()]  # noqa: SLF001
    partial = next(i for i, line in enumerate(plan) if line.startswith("AggregateExec: mode=Partial"))
    assert "array_agg(" in plan[partial]
    below = [line for line in plan[partial + 1 :] if not line.startswith("ProjectionExec:")]
    sort = "SortExec: expr=[id@0 ASC]" if direction == "ASC" else "SortExec: expr=[id@0 DESC NULLS LAST]"
    assert below[0].startswith(sort)
    assert "preserve_partitioning=[true]" in below[0]
    assert below[1].startswith("RangeExec:")


def test_grouped_collect_set(local_cluster_spark):
    spark = local_cluster_spark
    df = spark.createDataFrame(
        [(1, 1), (1, 1), (1, None), (1, 2), (2, None), (2, None), (3, 5), (3, 4), (3, 5), (3, 4)],
        "g int, x int",
    ).repartition(3)
    df.createOrReplaceTempView("collect_set_input")
    try:
        actual = spark.sql(
            """
            SELECT g, sort_array(collect_set(x)) AS xs, size(collect_set(x)) AS n
            FROM collect_set_input
            GROUP BY g
            ORDER BY g
            """
        )
        assert rows(actual) == [(1, [1, 2], 2), (2, [], 0), (3, [4, 5], 2)]
    finally:
        spark.catalog.dropTempView("collect_set_input")


@pytest.mark.parametrize(
    ("data", "schema", "preceding", "expected"),
    [
        (
            [(1, 1, "a"), (1, 2, "b"), (1, 3, "a"), (1, 4, "c")],
            "g int, o int, x string",
            1,
            [
                (1, ["a"], ["a"]),
                (2, ["a", "b"], ["a", "b"]),
                (3, ["b", "a"], ["a", "b"]),
                (4, ["a", "c"], ["a", "c"]),
            ],
        ),
        (
            [(1, 1, "a"), (1, 2, "b"), (1, 3, None), (1, 4, "b"), (1, 5, "c")],
            "g int, o int, x string",
            1,
            [
                (1, ["a"], ["a"]),
                (2, ["a", "b"], ["a", "b"]),
                (3, ["b"], ["b"]),
                (4, ["b"], ["b"]),
                (5, ["b", "c"], ["b", "c"]),
            ],
        ),
        (
            [(2, 1, 1), (2, 2, 1), (2, 3, 2), (2, 4, 1), (2, 5, 3)],
            "g int, o int, x int",
            2,
            [
                (1, [1], [1]),
                (2, [1, 1], [1]),
                (3, [1, 1, 2], [1, 2]),
                (4, [1, 2, 1], [1, 2]),
                (5, [2, 1, 3], [1, 2, 3]),
            ],
        ),
    ],
)
def test_sliding_window_collect_list_and_collect_set(local_cluster_spark, data, schema, preceding, expected):
    spark = local_cluster_spark
    spark.createDataFrame(data, schema).repartition(2).createOrReplaceTempView("sliding_input")
    try:
        actual = spark.sql(
            f"""
            SELECT
              o,
              collect_list(x) OVER w AS xs,
              sort_array(collect_set(x) OVER w) AS distinct_xs
            FROM sliding_input
            WINDOW w AS (PARTITION BY g ORDER BY o ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)
            ORDER BY o
            """  # noqa: S608
        )
        assert rows(actual) == expected
    finally:
        spark.catalog.dropTempView("sliding_input")
