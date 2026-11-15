"""Tests for random functions with a seed next to expressions that are planned separately.

The expected values were verified on JVM Spark 4.2.0.
"""

from collections.abc import Iterator

import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # A seeded call in a conditional branch advances its generator only for the rows that
        # evaluate the branch, also when another branch has a scalar subquery.
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D) END AS r "
            "FROM range(0, 6, 1, 1) ORDER BY id",
            [(0, 0.619189370225301), (1, 0.0), (2, 0.5096018842446481), (3, 0.0), (4, 0.8325259388871524), (5, 0.0)],
            id="conditional-with-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D) END AS r "
            "FROM range(0, 8, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, 0.0),
                (2, 0.5096018842446481),
                (3, 0.0),
                (4, 0.8017532427858894),
                (5, 0.0),
                (6, 0.6565552949992319),
                (7, 0.0),
            ],
            id="conditional-with-scalar-subquery-partitions",
        ),
        pytest.param(
            "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM range(10) x WHERE x.id = t.id AND x.id % 3 <> 1) "
            "THEN rand(42) END AS r FROM range(0, 6, 1, 1) t ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, None),
                (2, 0.5096018842446481),
                (3, 0.8325259388871524),
                (4, None),
                (5, 0.26322809041172357),
            ],
            id="conditional-with-correlated-exists",
        ),
        # Equal seeded expressions next to a scalar subquery still have equal values.
        pytest.param(
            "SELECT id, rand(42) + (SELECT 0.0D) AS a, rand(42) + (SELECT 0.0D) AS b FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301, 0.619189370225301),
                (1, 0.5096018842446481, 0.5096018842446481),
                (2, 0.8325259388871524, 0.8325259388871524),
                (3, 0.8017532427858894, 0.8017532427858894),
                (4, 0.6565552949992319, 0.6565552949992319),
                (5, 0.2515595782593636, 0.2515595782593636),
            ],
            id="equal-expressions-with-scalar-subquery",
        ),
        # The other expressions of the projection are evaluated for each row of the generator.
        pytest.param(
            "SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 3, 1, 1) ORDER BY id, g",
            [
                (0, 1, 0.619189370225301),
                (0, 2, 0.5096018842446481),
                (1, 1, 0.8325259388871524),
                (1, 2, 0.26322809041172357),
                (2, 1, 0.6702867696264135),
                (2, 2, 0.5173283545794627),
            ],
            id="generator",
        ),
        pytest.param(
            "SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 4, 1, 2) ORDER BY id, g",
            [
                (0, 1, 0.619189370225301),
                (0, 2, 0.5096018842446481),
                (1, 1, 0.8325259388871524),
                (1, 2, 0.26322809041172357),
                (2, 1, 0.8017532427858894),
                (2, 2, 0.6565552949992319),
                (3, 1, 0.2515595782593636),
                (3, 2, 0.2073428376111074),
            ],
            id="generator-partitions",
        ),
        # Each partition of the generator input keeps one generator across all of its batches.
        pytest.param(
            "SELECT count(*) AS n, round(sum(r), 4) AS s "
            "FROM (SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 10000, 1, 1))",
            [(20000, 9941.969)],
            id="generator-batches",
        ),
        pytest.param(
            "SELECT count(*) AS n, round(sum(r), 4) AS s "
            "FROM (SELECT id, explode(array(1, 2)) AS g, rand(42) AS r FROM range(0, 10000, 1, 2))",
            [(20000, 9917.2884)],
            id="generator-batches-partitions",
        ),
        # A lateral alias next to a generator reads the value of each row of the generator.
        pytest.param(
            "SELECT id, rand(42) AS r, explode(array(1, 2)) AS g, r + 1 AS s FROM range(0, 2, 1, 1) ORDER BY id, g",
            [
                (0, 0.619189370225301, 1, 1.619189370225301),
                (0, 0.5096018842446481, 2, 1.5096018842446481),
                (1, 0.8325259388871524, 1, 1.8325259388871524),
                (1, 0.26322809041172357, 2, 1.2632280904117237),
            ],
            id="generator-lateral-alias",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE randn(7) END AS r, explode(array(1, 2)) AS g, "
            "r * 2 AS s FROM range(0, 2, 1, 1) ORDER BY id, g",
            [
                (0, 0.619189370225301, 1, 1.238378740450602),
                (0, 0.5096018842446481, 2, 1.0192037684892963),
                (1, -1.773755556110447, 1, -3.547511112220894),
                (1, -0.32537852999017897, 2, -0.6507570599803579),
            ],
            id="generator-lateral-alias-conditional",
        ),
        pytest.param(
            "SELECT count(*) AS n, round(sum(r), 4) AS r, round(sum(s), 4) AS s FROM (SELECT id, rand(42) AS r, "
            "explode(array(1, 2)) AS g, r + 1 AS s FROM range(0, 10000, 1, 2))",
            [(20000, 9917.2884, 29917.2884)],
            id="generator-lateral-alias-batches-partitions",
        ),
        pytest.param(
            "SELECT id, rand(42) AS r, explode(array(1, 2)) AS g, IF(g = 1, r, -r) AS s "
            "FROM range(0, 2, 1, 2) ORDER BY id, g",
            [
                (0, 0.619189370225301, 1, 0.619189370225301),
                (0, 0.5096018842446481, 2, -0.5096018842446481),
                (1, 0.8017532427858894, 1, 0.8017532427858894),
                (1, 0.6565552949992319, 2, -0.6565552949992319),
            ],
            id="generator-lateral-alias-and-generator-output",
        ),
        pytest.param(
            "SELECT id, rand(42) AS r, explode_outer(IF(id % 2 = 0, array(1, 2), CAST(array() AS ARRAY<INT>))) AS g, "
            "r + 1 AS s FROM range(0, 4, 1, 1) ORDER BY id, g",
            [
                (0, 0.619189370225301, 1, 1.619189370225301),
                (0, 0.5096018842446481, 2, 1.5096018842446481),
                (1, 0.8325259388871524, None, 1.8325259388871524),
                (2, 0.26322809041172357, 1, 1.2632280904117237),
                (2, 0.6702867696264135, 2, 1.6702867696264136),
                (3, 0.5173283545794627, None, 1.5173283545794627),
            ],
            id="generator-lateral-alias-outer-generator",
        ),
        pytest.param(
            "SELECT count(*) AS n, count(DISTINCT r) AS d, round(sum(s), 6) AS s FROM (SELECT explode(array(1, 2)) AS g, "
            "rand(42) AS r, r + 1 AS s FROM range(0, 10000, 1, 1))",
            [(20000, 20000, 29941.96902)],
            id="generator-lateral-alias-after-generator-batches",
        ),
        # Seeded calls on the columns of a correlated subquery.
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t "
            "WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 1) x WHERE x.id = t.id AND rand(1) < 0.5) ORDER BY id",
            [(2,), (3,), (6,), (7,), (8,), (11,)],
            id="correlated-exists",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 1) t "
            "WHERE EXISTS (SELECT 1 FROM range(0, 12, 1, 1) x WHERE x.id = t.id AND x.id + rand(1) > 0.5) ORDER BY id",
            [(i,) for i in range(12)],
            id="correlated-exists-inner-column",
        ),
        pytest.param(
            "WITH t AS (SELECT id, id % 3 AS k, CAST(id AS DOUBLE) AS v FROM range(0, 6, 1, 1)) "
            "SELECT id, (SELECT max(r) FROM (SELECT v + rand(1) AS r FROM t t2 WHERE t2.k = t.k)) AS m "
            "FROM t ORDER BY id",
            [
                (0, 3.076841639054609),
                (1, 4.853921111175545),
                (2, 5.716770421797234),
                (3, 3.076841639054609),
                (4, 4.853921111175545),
                (5, 5.716770421797234),
            ],
            id="correlated-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, rand(1) AS r, (SELECT count(*) FROM range(0, 12, 1, 1) x WHERE x.id % 3 = t.id) AS c "
            "FROM range(0, 6, 1, 2) t ORDER BY id",
            [
                (0, 0.6363787615254752, 4),
                (1, 0.5993846534021868, 4),
                (2, 0.134842710012538, 4),
                (3, 0.5311207224659675, 0),
                (4, 0.2861372051669987, 0),
                (5, 0.49443063728956627, 0),
            ],
            id="correlated-scalar-subquery-computed-key",
        ),
        # An empty scalar subquery is NULL next to a seeded call.
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#24516, #24534)
        pytest.param(
            "SELECT id, rand(42) AS r, s FROM (SELECT id, (SELECT id FROM range(1) WHERE id > 5) AS s "
            "FROM range(0, 3, 1, 1)) ORDER BY id",
            [(0, 0.619189370225301, None), (1, 0.5096018842446481, None), (2, 0.8325259388871524, None)],
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                strict=True,
                reason="DataFusion types an empty scalar subquery as non-nullable",
            ),
            id="empty-scalar-subquery",
        ),
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#24516, #24534)
        pytest.param(
            "SELECT id, rand(42) + s AS r, rand(42) AS q FROM (SELECT id, (SELECT id FROM range(1) WHERE id > 5) AS s "
            "FROM range(0, 3, 1, 1)) ORDER BY id",
            [(0, None, 0.619189370225301), (1, None, 0.5096018842446481), (2, None, 0.8325259388871524)],
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                strict=True,
                reason="DataFusion types an empty scalar subquery as non-nullable",
            ),
            id="empty-scalar-subquery-arithmetic",
        ),
        # An unused seeded column does not drop the input sort that other operators depend on.
        pytest.param(
            "SELECT collect_list(id) AS l FROM (SELECT id, rand(42) AS r "
            "FROM (SELECT * FROM range(0, 6, 1, 1) ORDER BY id DESC))",
            [([5, 4, 3, 2, 1, 0],)],
            id="unused-seeded-column-order-relevant-aggregate",
        ),
        pytest.param(
            "SELECT id, r FROM (SELECT id, a, rand(42) AS r "
            "FROM (SELECT id, rand(1) AS a FROM (SELECT * FROM range(0, 6, 1, 1) ORDER BY id DESC)))",
            [
                (5, 0.619189370225301),
                (4, 0.5096018842446481),
                (3, 0.8325259388871524),
                (2, 0.26322809041172357),
                (1, 0.6702867696264135),
                (0, 0.5173283545794627),
            ],
            id="unused-seeded-column-below-used-one",
        ),
    ],
)
def test_seeded_random_next_to_separately_planned_expressions(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, rand(42) + rand() AS a, rand(42) + rand() AS b FROM range(0, 6, 1, 1)",
        "SELECT id, rand(42) + rand() + (SELECT 0.0D) AS a, rand(42) + rand() + (SELECT 0.0D) AS b "
        "FROM range(0, 6, 1, 1)",
        "SELECT id, IF(id % 2 = 0, rand(42) + rand(), (SELECT 0.0D)) AS a, "
        "IF(id % 2 = 0, rand(42) + rand(), (SELECT 0.0D)) AS b FROM range(0, 6, 1, 1)",
    ],
)
def test_equal_expressions_with_unseeded_calls_are_independent(spark, query):
    # Each call without a seed has its own generator, so equal expressions have different values.
    rows = spark.sql(query).collect()
    assert all(row.a != row.b for row in rows if row.id % 2 == 0)


def test_equal_unaliased_expressions_with_unseeded_calls_are_independent(spark):
    rows = spark.sql("SELECT id, rand(42) + rand(), rand(42) + rand() FROM range(0, 6, 1, 1)").collect()
    assert all(row[1] != row[2] for row in rows)


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_unused_seeded_random_column_does_not_keep_the_input_sort(spark):
    query = "SELECT sum({}) AS s FROM (SELECT id, rand(1) AS r FROM (SELECT * FROM range(0, 100, 1, 4) ORDER BY id DESC)) WHERE id > 5"
    df = spark.sql(query.format("id"))
    assert df.collect()[0].s == 4935  # noqa: PLR2004
    assert "SortExec" not in df._explain_string()  # noqa: SLF001
    # The random values depend on the order of the input when the random column is used.
    assert "SortExec" in spark.sql(query.format("r"))._explain_string()  # noqa: SLF001


def test_seeded_random_case_with_uncorrelated_exists(spark):
    query = (
        "SELECT count(DISTINCT s), round(sum(s), 6) FROM ("
        "SELECT CASE WHEN id % 2 = 0 THEN rand(42) WHEN EXISTS (SELECT 1) THEN -1.0D END s "
        "FROM range(0, 40000, 1, {partitions}))"
    )
    assert tuple(spark.sql(query.format(partitions=4)).collect()[0]) == (20001, -10119.361678)
    assert spark.sql(query.format(partitions=1)).collect()[0][0] == 20001  # noqa: PLR2004


@pytest.mark.parametrize(
    ("branch", "expected"),
    [
        pytest.param(
            "WHEN EXISTS (SELECT 1 FROM range(0, 100, 1, 1) i WHERE i.id = o.id % 50) THEN -1.0D",
            20001,
            id="correlated-exists",
        ),
        pytest.param(
            "WHEN id IN (SELECT id * 2 + 1 FROM range(0, 100, 1, 1)) THEN -1.0D",
            20001,
            id="in",
        ),
        pytest.param(
            "ELSE (SELECT max(i.id) FROM range(0, 100, 1, 1) i WHERE i.id = o.id % 50)",
            20025,
            id="correlated-scalar",
        ),
    ],
)
def test_seeded_random_case_with_subquery(spark, branch, expected):
    query = (
        f"SELECT count(DISTINCT s) FROM (SELECT CASE WHEN id % 2 = 0 THEN rand(42) {branch} END s "  # noqa: S608
        "FROM range(0, 40000, 1, 4) o)"
    )
    assert spark.sql(query).collect()[0][0] == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT id, IF(id NOT IN (SELECT CAST(NULL AS BIGINT)), rand(1), uniform(0.0D, 1.0D, 3)) AS r "
            "FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.25738143505962285),
                (1, 0.6698885713796182),
                (2, 0.9416651198480125),
                (3, 0.9531453492357947),
                (4, 0.06613556112256402),
                (5, 0.144530390957615),
            ],
            id="not-in-null",
        ),
        pytest.param(
            "SELECT count(*), count(DISTINCT r), round(sum(r), 6) FROM (SELECT CASE WHEN id % 2 = 0 THEN rand(42) "
            "ELSE (SELECT max(x.id) FROM range(0, 100, 1, 1) x WHERE x.id = t.id % 50) * 0.0D - 1.0D END AS r "
            "FROM range(0, 40000, 1, 2) t)",
            [(40000, 20001, -10082.711556)],
            id="correlated-scalar-branch-batches",
        ),
        pytest.param(
            "SELECT count(*), count(DISTINCT r), round(sum(r), 6) FROM (SELECT IF(id IN "
            "(SELECT id * 3 FROM range(0, 13334)), rand(42), randn(7)) AS r FROM range(0, 40000, 1, 2))",
            [(40000, 40000, 6428.141711)],
            id="in-condition-batches",
        ),
    ],
)
def test_seeded_random_conditional_with_subquery(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.fixture
def seeded_random_view(spark):
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW seeded_random_view "
        "AS SELECT id, id % 3 AS k, CAST(id AS DOUBLE) / 2 AS v FROM range(0, 12, 1, 1)"
    )
    yield "seeded_random_view"
    spark.sql("DROP VIEW IF EXISTS seeded_random_view")


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        pytest.param(
            "rand(1) > 0.3 OR EXISTS (SELECT 1 FROM {t} t2 WHERE t2.k = t.k AND t2.v > t.v + 3)",
            [0, 1, 2, 4, 5, 8, 9, 10],
            id="or-correlated-exists",
        ),
    ],
)
def test_seeded_random_filter_with_correlated_subquery(spark, seeded_random_view, predicate, expected):
    t = seeded_random_view
    query = f"SELECT id FROM {t} t WHERE {predicate.format(t=t)} ORDER BY id"  # noqa: S608
    assert [row.id for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT id FROM range(0, 16, 1, 2) WHERE CASE WHEN id % 2 = 0 THEN rand(42) < 0.5 "
            "ELSE id IN (SELECT id FROM range(0, 16, 1, 2) WHERE id % 3 = 0) END ORDER BY id",
            [(3,), (6,), (9,), (12,), (14,), (15,)],
            id="case-in",
        ),
        pytest.param(
            "SELECT id FROM range(0, 16, 1, 2) t "
            "WHERE randn(1) > (SELECT max(x.id) FROM range(0, 10) x WHERE x.id = t.id % 5) * 0.1 ORDER BY id",
            [(0,), (1,), (2,), (3,), (7,), (12,)],
            id="correlated-scalar-comparison",
        ),
        pytest.param(
            "SELECT count(*), sum(id) FROM range(0, 40000, 1, 2) WHERE rand(42) < (SELECT 0.5)",
            [(20100, 400673754)],
            id="scalar-subquery-batches",
        ),
        pytest.param(
            "SELECT count(*), sum(id) FROM range(0, 40000, 1, 2) t WHERE rand(42) < 0.5 "
            "AND EXISTS (SELECT 1 FROM range(0, 40000, 1, 3) x WHERE x.id = t.id AND x.id % 3 <> 0)",
            [(13462, 268961640)],
            id="and-correlated-exists-batches",
        ),
    ],
)
def test_seeded_random_filter_with_subquery(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


def test_seeded_random_with_input_file_name(spark, tmp_path):
    path = str(tmp_path / "files")
    spark.range(0, 12, 1, 2).write.parquet(path)
    df = spark.read.parquet(path)
    files = {row.f for row in df.select(F.input_file_name().alias("f")).collect()}
    assert len(files) == 2  # noqa: PLR2004
    for c in [
        F.input_file_name(),
        F.when(F.col("id") % 2 == 0, F.rand(42).cast("string")).otherwise(F.input_file_name()),
    ]:
        rows = df.select("id", F.rand(42).alias("r"), c.alias("c")).collect()
        assert {row.c for row in rows if row.id % 2 == 1} <= files
        assert len({row.r for row in rows}) == 12  # noqa: PLR2004
    rows = df.where(F.rand(42) < 2).select(F.input_file_name().alias("f")).collect()  # noqa: PLR2004
    assert {row.f for row in rows} == files


@pytest.mark.parametrize("function", ["monotonically_increasing_id", "spark_partition_id"])
def test_partition_dependent_function_with_input_file_name(spark, tmp_path, function):
    path = str(tmp_path / "files")
    spark.range(0, 12, 1, 2).write.parquet(path)
    df = spark.read.parquet(path)
    files = {row.f for row in df.select(F.input_file_name().alias("f")).collect()}
    rows = df.select(getattr(F, function)().alias("c"), F.input_file_name().alias("f")).collect()
    assert {row.f for row in rows} == files


# The first values of `rand(42)` in partitions 0 and 1.
_RAND_42 = {
    0: [
        0.619189370225301,
        0.5096018842446481,
        0.8325259388871524,
        0.26322809041172357,
        0.6702867696264135,
        0.5173283545794627,
    ],
    1: [
        0.8017532427858894,
        0.6565552949992319,
        0.2515595782593636,
        0.2073428376111074,
        0.6392921379278927,
        0.8505582285081454,
    ],
}


def _rows_by_partition(df, column):
    """Returns the values of the column for each partition, in the order of the rows in the partition.

    Each input file is read by one partition, but the partition that reads a file, and the number of
    files that a partition reads, depend on the execution. So the partition of each row is taken from
    `monotonically_increasing_id()`, which also gives the position of the row in its partition.
    """
    rows = df.select(
        "id",
        column.alias("c"),
        F.input_file_name().alias("f"),
        F.monotonically_increasing_id().alias("m"),
    ).collect()
    assert len({row.f for row in rows}) == 2  # noqa: PLR2004
    # The rows of each file are in one partition.
    assert all(len({row.m >> 33 for row in rows if row.f == f}) == 1 for f in {row.f for row in rows})
    partitions = {}
    for row in sorted(rows, key=lambda r: r.m):
        partitions.setdefault(row.m >> 33, []).append(row)
    for values in partitions.values():
        assert [row.m & ((1 << 33) - 1) for row in values] == list(range(len(values)))
    return partitions


def test_partition_dependent_values_with_input_file_name(spark, tmp_path):
    path = str(tmp_path / "files")
    spark.range(0, 12, 1, 2).write.parquet(path)
    df = spark.read.parquet(path)

    for partition, rows in _rows_by_partition(df, F.spark_partition_id()).items():
        assert [row.c for row in rows] == [partition] * len(rows)

    # The generator of each partition advances only for the rows that evaluate the seeded branch.
    column = F.when(F.col("id") % 2 == 0, F.rand(42)).otherwise(F.length(F.input_file_name()) * 0.0 - 1.0)
    for partition, rows in _rows_by_partition(df, column).items():
        expected = iter(_RAND_42[partition])
        assert [row.c for row in rows] == [next(expected) if row.id % 2 == 0 else -1.0 for row in rows]


def test_seeded_random_case_with_pandas_iterator_udf(spark):
    @F.pandas_udf("double")
    def negative_one(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for s in it:
            yield s.astype("float64") * 0 - 1

    small = spark.range(0, 6, 1, 2).select(
        "id", F.when(F.col("id") % 2 == 0, F.rand(42)).otherwise(negative_one(F.col("id"))).alias("c")
    )
    assert [tuple(row) for row in small.orderBy("id").collect()] == [
        (0, 0.619189370225301),
        (1, -1.0),
        (2, 0.5096018842446481),
        (3, -1.0),
        (4, 0.8017532427858894),
        (5, -1.0),
    ]
    # The generator advances only for the even rows, without repeating values across batches.
    for argument in [F.col("id"), F.col("id").cast("double")]:
        for partitions, total in [(1, -7581.331889), (2, -7575.783263)]:
            df = spark.range(0, 30000, 1, partitions).select(
                F.when(F.col("id") % 2 == 0, F.rand(42)).otherwise(negative_one(argument)).alias("c")
            )
            assert tuple(df.agg(F.count_distinct("c"), F.round(F.sum("c"), 6)).collect()[0]) == (15001, total)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # A seeded call in a `CASE` with boolean results advances its generator only for the
        # rows that evaluate it, also when the condition has a seeded call.
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count_if(b) AS c, sum(IF(b, id, 0)) AS s FROM (SELECT id, "
            "CASE WHEN rand(42) < 0.5 THEN id % 3 = 0 ELSE rand(7) < 0.5 END AS b FROM range(0, 20000, 1, 1))",
            [(8286, 82652296)],
            id="boolean-case-with-seeded-condition",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count_if(b) AS c, sum(IF(b, id, 0)) AS s FROM (SELECT id, "
            "CASE WHEN rand(42) < 0.5 THEN id % 3 = 0 ELSE rand(7) < 0.5 END AS b FROM range(0, 20000, 1, 3))",
            [(8380, 83907453)],
            id="boolean-case-with-seeded-condition-partitions",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count_if(b) AS c, sum(IF(b, id, 0)) AS s FROM (SELECT id, "
            "CASE WHEN rand(1) < 0.3 THEN true WHEN rand(2) < 0.5 THEN rand(3) < 0.5 ELSE false END AS b "
            "FROM range(0, 20000, 1, 1))",
            [(9438, 94240347)],
            id="boolean-case-with-seeded-conditions",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
        # The right side of an operator whose result is null for a null left side is evaluated
        # only for the rows where the left side is not null.
        pytest.param(
            "SELECT id, IF(id % 3 = 0, NULL, id) + rand(1) AS r FROM range(0, 6, 1, 1) ORDER BY id",
            [
                (0, None),
                (1, 1.6363787615254752),
                (2, 2.5993846534021867),
                (3, None),
                (4, 4.1348427100125384),
                (5, 5.076841639054609),
            ],
            id="null-left-side",
        ),
        pytest.param(
            "SELECT id, IF(id % 3 = 0, NULL, id) + rand(1) AS r FROM range(0, 8, 1, 2) ORDER BY id",
            [
                (0, None),
                (1, 1.6363787615254752),
                (2, 2.5993846534021867),
                (3, None),
                (4, 4.5311207224659675),
                (5, 5.286137205166999),
                (6, None),
                (7, 7.494430637289566),
            ],
            id="null-left-side-partitions",
        ),
        pytest.param(
            "SELECT id, IF(id % 3 = 0, NULL, id) + rand(1) + (SELECT 0.0D) AS r FROM range(0, 6, 1, 1) ORDER BY id",
            [
                (0, None),
                (1, 1.6363787615254752),
                (2, 2.5993846534021867),
                (3, None),
                (4, 4.1348427100125384),
                (5, 5.076841639054609),
            ],
            id="null-left-side-with-scalar-subquery",
        ),
        pytest.param(
            "SELECT id, IF(id % 3 = 0, NULL, id) < rand(2) * 6 AS r FROM range(0, 6, 1, 1) ORDER BY id",
            [(0, None), (1, True), (2, False), (3, None), (4, False), (5, False)],
            id="null-left-side-comparison",
        ),
        pytest.param(
            "SELECT id, (SELECT max(w) FROM VALUES (0, 10), (1, 11), (1, 12), (NULL, 13), (5, 15) AS s(k, w) "
            "WHERE s.k = t.k) + rand(1) AS c FROM (SELECT id, id % 3 AS k FROM range(0, 12, 1, 1)) t ORDER BY id",
            [
                (0, 10.636378761525474),
                (1, 12.599384653402186),
                (2, None),
                (3, 10.134842710012538),
                (4, 12.076841639054608),
                (5, None),
                (6, 10.853921111175545),
                (7, 12.716770421797234),
                (8, None),
                (9, 10.247390240759797),
                (10, 12.136745074185137),
                (11, None),
            ],
            id="null-correlated-scalar-subquery-left-side",
        ),
    ],
)
def test_seeded_random_in_lazily_evaluated_arguments(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    "query",
    [
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count_if(NOT c) FROM (SELECT CASE WHEN rand() < 0.5 THEN id >= 0 ELSE id >= 0 END AS c "
            "FROM range(0, 10000, 1, 2))",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count(*) FROM range(0, 10000, 1, 2) WHERE NOT (CASE WHEN rand() < 0.5 THEN id >= 0 ELSE id >= 0 END)",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
        # TODO: remove xfail after upgrading to DataFusion 56 (apache/datafusion#25143)
        pytest.param(
            "SELECT count_if(NOT c) FROM (SELECT CASE WHEN rand() < 0.3 THEN id >= 0 WHEN rand() < 0.5 THEN id >= 0 "
            "ELSE id >= 0 END AS c FROM range(0, 10000, 1, 2))",
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="DataFusion rewrites boolean CASE with volatile WHEN into AND/OR",
                strict=True,
            ),
        ),
    ],
)
def test_boolean_case_with_volatile_condition_evaluates_the_condition_once(spark, query):
    # Every branch is true, so the result is true however the condition is evaluated, as long
    # as it is evaluated once for each row.
    assert spark.sql(query).collect()[0][0] == 0


def test_seeded_case_subquery_columns_are_shared(spark):
    df = spark.sql(
        "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D AS x) END AS a, "
        "CASE WHEN id % 2 = 0 THEN rand(42) ELSE (SELECT 0.0D AS y) END AS b "
        "FROM range(0, 6, 1, 1) ORDER BY id"
    )
    assert [tuple(row) for row in df.collect()] == [
        (0, 0.619189370225301, 0.619189370225301),
        (1, 0.0, 0.0),
        (2, 0.5096018842446481, 0.5096018842446481),
        (3, 0.0, 0.0),
        (4, 0.8325259388871524, 0.8325259388871524),
        (5, 0.0, 0.0),
    ]
    if not is_jvm_spark():
        lines = [line for line in df._explain_string().splitlines() if "SeededRandomExec:" in line]  # noqa: SLF001
        assert len(lines) == 1
        assert lines[0].count("random(42)") == 1


@pytest.mark.parametrize("condition", ["rand(42) > 0.5D", "rand(42) > (SELECT 0.5D)"])
@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_seeded_column_reuse_preserves_timestamp_types(spark, session_timezone, condition):
    _ = session_timezone
    df = spark.sql(
        f"""SELECT CASE WHEN {condition} THEN TIMESTAMP_NTZ '1970-01-01 00:00:00' END AS a,
        CASE WHEN {condition} THEN TIMESTAMP_LTZ '1970-01-01 00:00:00Z' END AS b
        FROM range(0, 3, 1, 1)"""  # noqa: S608
    )
    assert [field.dataType.simpleString() for field in df.schema] == ["timestamp_ntz", "timestamp"]
    assert len(df.collect()) == 3  # noqa: PLR2004
