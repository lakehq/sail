"""Seeded-expression regressions, with Spark-compatible cases verified on JVM Spark 4.2.0."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("seeded_argument", [False, True])
def test_with_columns_seeded_siblings_follow_generator(spark, reverse, seeded_argument):
    if seeded_argument:
        generator = F.explode(F.array(F.rand(42), F.rand(42) + 1))
        random = F.rand(43)
        expected = [(0.619189370225301, 0.8017532427858894), (1.619189370225301, 0.6565552949992319)]
    else:
        generator = F.explode(F.array(F.lit(1), F.lit(2)))
        random = F.rand(42)
        expected = [(1, 0.619189370225301), (2, 0.5096018842446481)]
    expressions = [("e", generator), ("r", random)]
    if reverse:
        expressions.reverse()
    df = spark.range(0, 1, 1, 1).withColumns(dict(expressions)).select("e", "r")
    assert [tuple(row) for row in df.collect()] == expected


def test_aggregate_seeded_sibling_precedes_generator(spark):
    # Spark keeps an aggregate's other expressions below Generate, so both output rows
    # read the same random value. This differs from an ordinary projection.
    df = spark.range(0, 1, 1, 1).agg(
        F.explode(F.array(F.count("*"), F.count("*") + 1)).alias("e"),
        F.rand(42).alias("r"),
    )
    assert [tuple(row) for row in df.collect()] == [(1, 0.619189370225301), (2, 0.619189370225301)]


@pytest.mark.parametrize("all_metadata", [False, True])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param(
            "transform(arr, x -> x + rand(42))",
            [[1.619189370225301, 3.509601884244648], [5.832525938887152, 7.263228090411723]],
            id="element",
        ),
        pytest.param(
            "transform(arr, (x, i) -> x + i + rand(42))",
            [[1.619189370225301, 4.509601884244648], [5.832525938887152, 8.263228090411724]],
            id="element-and-index",
        ),
        pytest.param(
            "transform(arr, x -> transform(array(1, 2), y -> x + y + id + rand(42)))",
            [
                [[2.619189370225301, 3.509601884244648], [4.832525938887152, 5.263228090411723]],
                [[7.670286769626413, 8.517328354579462], [9.999144164758597, 10.069932337282792]],
            ],
            id="nested-with-outer-captures",
        ),
    ],
)
def test_file_metadata_preserves_seeded_lambda_bindings(spark, tmp_path, all_metadata, expression, expected):
    path = tmp_path / "arrays.parquet"
    pq.write_table(pa.table({"id": [0, 1], "arr": [[1, 3], [5, 7]]}), path)
    columns = ["id", expression + " AS values", "input_file_name() AS file"]
    if all_metadata:
        columns.extend(["input_file_block_start() AS start", "input_file_block_length() AS length"])
    rows = spark.read.parquet(str(path)).selectExpr(*columns).orderBy("id").collect()
    assert [row["values"] for row in rows] == expected
    assert all(row.file.endswith("arrays.parquet") for row in rows)
    if all_metadata:
        assert all(row.start == 0 and row.length > 0 for row in rows)


@pytest.mark.parametrize(
    "seed",
    [
        "NULL",
        pytest.param(
            "CAST(NULL AS BIGINT)",
            marks=pytest.mark.spark(
                skip_if_jvm=True, reason="Sail accepts constant uuid/shuffle seeds; Spark requires literals"
            ),
        ),
        pytest.param(
            "IF(true, CAST(NULL AS BIGINT), 7)",
            marks=pytest.mark.spark(
                skip_if_jvm=True, reason="Sail accepts constant uuid/shuffle seeds; Spark requires literals"
            ),
        ),
    ],
)
@pytest.mark.parametrize("function", ["uuid({seed})", "shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), {seed})"])
def test_null_seed_aliases_remain_independent(spark, seed, function):
    expression = function.format(seed=seed)
    rows = spark.sql(f"SELECT {expression} AS a, {expression} AS b FROM range(0, 20, 1, 1)").collect()  # noqa: S608
    # A NULL seed requests independently chosen seeds. Individual shuffled arrays can
    # coincide, but sharing every value across the two streams is incorrect.
    assert any(row.a != row.b for row in rows)


@pytest.mark.parametrize("function", ["uuid(7)", "shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), 7)"])
def test_constant_seed_aliases_keep_equal_values(spark, function):
    rows = spark.sql(f"SELECT {function} AS a, {function} AS b FROM range(0, 20, 1, 1)").collect()  # noqa: S608
    assert all(row.a == row.b for row in rows)


@pytest.mark.spark(skip_if_jvm=True, reason="Sail rejects correlation that would replay unrelated volatile calls")
@pytest.mark.parametrize(
    ("query", "message"),
    [
        pytest.param(
            "SELECT id, k, (SELECT count(*) FROM range(0, 3, 1, 1) i WHERE i.id < o.k) AS n "
            "FROM (SELECT id, rand(42) + rand() AS k FROM range(0, 12, 1, 1)) o",
            "Physical plan does not support logical expression ScalarSubquery",
            id="outer-key",
        ),
        pytest.param(
            "SELECT id, (SELECT count(*) FROM (SELECT rand(42) + rand() AS k FROM range(0, 10, 1, 1)) i "
            "WHERE i.k < o.id) AS n FROM range(0, 3, 2, 1) o",
            "correlated scalar aggregate with volatile inner expressions",
            id="inner-input",
        ),
    ],
)
def test_correlated_aggregate_does_not_replay_mixed_random_calls(spark, query, message):
    from pyspark.errors import PySparkException  # noqa: PLC0415

    with pytest.raises(PySparkException, match=message):
        spark.sql(query).collect()


@pytest.mark.spark(skip_if_jvm=True, reason="Sail rejects correlation keys with independently chosen seeds")
@pytest.mark.parametrize("seed", ["NULL", "(SELECT CAST(NULL AS BIGINT))"])
@pytest.mark.parametrize("function", ["uuid({seed})", "CAST(shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), {seed}) AS STRING)"])
def test_correlated_aggregate_does_not_replay_null_seeds(spark, seed, function):
    from pyspark.errors import PySparkException  # noqa: PLC0415

    function = function.format(seed=seed)
    # Every key lies strictly between '!' and '~', so a successful query must return 1.
    # Replaying its unseeded portion used to lose the key and return 0.
    query = (
        "SELECT k, (SELECT count(*) FROM VALUES ('!'), ('~') i(v) WHERE i.v < o.k) AS n "
        f"FROM (SELECT concat(CAST(rand(42) AS STRING), {function}) AS k FROM range(0, 4, 1, 1)) o"
    )
    with pytest.raises(PySparkException, match="Physical plan does not support logical expression ScalarSubquery"):
        spark.sql(query).collect()


@pytest.mark.parametrize(
    "expression",
    [
        "rand(42) + rand(43)",
        "rand(40 + 2) + rand(CAST(43 AS BIGINT))",
        "rand(42) + (SELECT rand(7))",
        "rand(42) + (SELECT rand())",
        "(SELECT rand())",
        pytest.param(
            "rand((SELECT 42))",
            marks=pytest.mark.spark(skip_if_jvm=True, reason="Sail supports cached scalar-subquery seeds"),
        ),
        pytest.param(
            "rand((SELECT CAST(NULL AS BIGINT)))",
            marks=pytest.mark.spark(skip_if_jvm=True, reason="Sail supports cached scalar-subquery seeds"),
        ),
    ],
)
def test_correlated_aggregate_can_replay_reproducible_inputs(spark, expression):
    query = (
        "SELECT id, k, (SELECT count(*) FROM range(0, 3, 1, 1) i WHERE i.id < o.k) AS n "
        f"FROM (SELECT id, {expression} AS k FROM range(0, 12, 1, 1)) o"
    )
    rows = spark.sql(query).collect()
    assert len(rows) == 12  # noqa: PLR2004
    assert all(row.n == sum(value < row.k for value in range(3)) for row in rows)


def test_correlated_aggregate_keeps_cached_inner_scalar_subquery(spark):
    # The uncorrelated scalar is cached independently; replaying the surrounding input
    # must not treat its volatile body as a per-row random call.
    rows = spark.sql(
        "SELECT id, (SELECT count(*) FROM (SELECT rand(42) + (SELECT rand() * 0) AS k "
        "FROM range(0, 10, 1, 1)) i WHERE i.k < o.id) AS n FROM range(0, 3, 1, 1) o"
    ).collect()
    assert sorted(tuple(row) for row in rows) == [(0, 0), (1, 10), (2, 10)]


@pytest.mark.spark(skip_if_jvm=True, reason="Sail supports cached scalar-subquery seeds")
@pytest.mark.parametrize(
    "function",
    ["uuid((SELECT 42))", "CAST(shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), (SELECT 42)) AS STRING)"],
)
def test_correlated_aggregate_replays_cached_nonnull_seeds(spark, function):
    query = (
        "SELECT k, (SELECT count(*) FROM VALUES ('!'), ('~') i(v) WHERE i.v < o.k) AS n "
        f"FROM (SELECT concat(CAST(rand(42) AS STRING), {function}) AS k FROM range(0, 12, 1, 1)) o"
    )
    rows = spark.sql(query).collect()
    assert len(rows) == 12  # noqa: PLR2004
    assert all(row.n == 1 for row in rows)
