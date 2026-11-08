"""Tests for seeding random functions once per partition, as Spark does.

The expected values were verified on JVM Spark 4.2.0. They depend on the partition of each row,
so every input has an explicit number of partitions.
"""

import re

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT id, rand(42) AS r FROM range(0, 12, 1, 4) ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, 0.5096018842446481),
                (2, 0.8325259388871524),
                (3, 0.8017532427858894),
                (4, 0.6565552949992319),
                (5, 0.2515595782593636),
                (6, 0.7985334687187781),
                (7, 0.8679617292463396),
                (8, 0.5068407664758031),
                (9, 0.7971301351894658),
                (10, 0.2422600602366628),
                (11, 0.7097364852287148),
            ],
            id="rand",
        ),
        pytest.param(
            "SELECT id, randn(7) AS r FROM range(0, 9, 1, 3) ORDER BY id",
            [
                (0, -1.773755556110447),
                (1, -0.32537852999017897),
                (2, 1.1637571325924414),
                (3, -0.12108592338857035),
                (4, 1.7346662488921807),
                (5, -1.5758974095572422),
                (6, 1.8039683668407596),
                (7, -0.2840236697878177),
                (8, 0.6603370350220075),
            ],
            id="randn",
        ),
        pytest.param(
            "SELECT id, uniform(0, 100, 3) AS r FROM range(0, 9, 1, 3) ORDER BY id",
            [(0, 25), (1, 66), (2, 94), (3, 95), (4, 6), (5, 14), (6, 2), (7, 9), (8, 77)],
            id="uniform",
        ),
        pytest.param(
            "SELECT id, randstr(4, 5) AS r FROM range(0, 9, 1, 3) ORDER BY id",
            [
                (0, "R0Tm"),
                (1, "gfhb"),
                (2, "aigB"),
                (3, "2Brq"),
                (4, "6zBB"),
                (5, "TClX"),
                (6, "6Ogz"),
                (7, "59i3"),
                (8, "TI1A"),
            ],
            id="randstr",
        ),
        pytest.param(
            "SELECT id, rand(42) AS a, rand(42) AS b FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301, 0.619189370225301),
                (1, 0.5096018842446481, 0.5096018842446481),
                (2, 0.8325259388871524, 0.8325259388871524),
                (3, 0.8017532427858894, 0.8017532427858894),
                (4, 0.6565552949992319, 0.6565552949992319),
                (5, 0.2515595782593636, 0.2515595782593636),
            ],
            id="same-call-twice",
        ),
        pytest.param(
            "SELECT id FROM range(0, 20, 1, 4) WHERE rand(7) < 0.5 ORDER BY id",
            [(0,), (1,), (5,), (7,), (8,), (11,), (12,), (13,), (15,), (19,)],
            id="filter",
        ),
        pytest.param(
            "SELECT id, rand(NULL) AS a, rand(0) AS b FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.7604953758285915, 0.7604953758285915),
                (1, 0.5234194256885571, 0.5234194256885571),
                (2, 0.0953472826424725, 0.0953472826424725),
                (3, 0.6363787615254752, 0.6363787615254752),
                (4, 0.5993846534021868, 0.5993846534021868),
                (5, 0.134842710012538, 0.134842710012538),
            ],
            id="null-seed",
        ),
        pytest.param(
            "SELECT id, rand(42) AS r FROM range(0, 6, 1, 2) ORDER BY id DESC",
            [
                (5, 0.2515595782593636),
                (4, 0.6565552949992319),
                (3, 0.8017532427858894),
                (2, 0.8325259388871524),
                (1, 0.5096018842446481),
                (0, 0.619189370225301),
            ],
            id="order-by-after",
        ),
        pytest.param(
            "SELECT id, r FROM (SELECT id, rand(42) AS r FROM (SELECT id FROM range(0, 6, 1, 2) SORT BY id DESC)) "
            "ORDER BY id",
            [
                (0, 0.8325259388871524),
                (1, 0.5096018842446481),
                (2, 0.619189370225301),
                (3, 0.2515595782593636),
                (4, 0.6565552949992319),
                (5, 0.8017532427858894),
            ],
            id="sort-by-before",
        ),
        pytest.param(
            "SELECT id, r FROM (SELECT id, rand(42) AS r FROM (SELECT * FROM range(0, 10, 1, 1) ORDER BY id DESC)) "
            "ORDER BY id",
            [
                (0, 0.7959575617927873),
                (1, 0.9696695610826327),
                (2, 0.06993233728279169),
                (3, 0.9991441647585968),
                (4, 0.5173283545794627),
                (5, 0.6702867696264135),
                (6, 0.26322809041172357),
                (7, 0.8325259388871524),
                (8, 0.5096018842446481),
                (9, 0.619189370225301),
            ],
            id="order-by-before",
        ),
        pytest.param(
            "SELECT rand(7) < 0.5 AS k, sum(id) AS s FROM range(0, 20, 1, 4) GROUP BY rand(7) < 0.5 ORDER BY k",
            [(False, 99), (True, 91)],
            id="group-by",
        ),
        pytest.param(
            "SELECT id, rand(42) AS r FROM range(0, 6, 1, 2) ORDER BY rand(42)",
            [
                (5, 0.2515595782593636),
                (1, 0.5096018842446481),
                (0, 0.619189370225301),
                (4, 0.6565552949992319),
                (3, 0.8017532427858894),
                (2, 0.8325259388871524),
            ],
            id="order-by-key",
        ),
        pytest.param(
            "SELECT id FROM range(0, 6, 1, 2) ORDER BY rand(42) DESC LIMIT 3",
            [(2,), (3,), (4,)],
            id="order-by-unselected-key",
        ),
        # Spark advances the generator only for the rows that evaluate the call.
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) END AS r FROM range(0, 10, 1, 1) ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, None),
                (2, 0.5096018842446481),
                (3, None),
                (4, 0.8325259388871524),
                (5, None),
                (6, 0.26322809041172357),
                (7, None),
                (8, 0.6702867696264135),
                (9, None),
            ],
            id="case-when",
        ),
        pytest.param(
            "SELECT id, transform(array(1, 2), x -> rand(42)) AS r FROM range(0, 3, 1, 1) ORDER BY id",
            [
                (0, [0.619189370225301, 0.5096018842446481]),
                (1, [0.8325259388871524, 0.26322809041172357]),
                (2, [0.6702867696264135, 0.5173283545794627]),
            ],
            id="lambda",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id >= 0 THEN rand(42) END AS a FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, 0.619189370225301),
                (1, 0.5096018842446481),
                (2, 0.8325259388871524),
                (3, 0.8017532427858894),
                (4, 0.6565552949992319),
                (5, 0.2515595782593636),
            ],
            id="case-when-per-partition",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN CASE WHEN id % 3 = 0 THEN rand(1) END END AS r FROM range(0, 12, 1, 1) ORDER BY id",
            [
                (0, 0.6363787615254752),
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, 0.5993846534021868),
                (7, None),
                (8, None),
                (9, None),
                (10, None),
                (11, None),
            ],
            id="nested-case-when",
        ),
        pytest.param(
            "SELECT id, (CASE WHEN id % 3 = 0 THEN NULL ELSE id % 3 = 1 END) AND rand(5) < 0.5 AS b FROM range(0, 12, 1, 1) ORDER BY id",
            [
                (0, None),
                (1, True),
                (2, False),
                (3, False),
                (4, False),
                (5, False),
                (6, None),
                (7, True),
                (8, False),
                (9, None),
                (10, False),
                (11, False),
            ],
            id="and-with-null",
        ),
        pytest.param(
            "SELECT id FROM range(0, 20, 1, 1) WHERE id > 5 OR rand(7) < 0.5 ORDER BY id",
            [(0,), (1,), (6,), (7,), (8,), (9,), (10,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,)],
            id="or",
        ),
        pytest.param(
            "SELECT id, coalesce(CASE WHEN id % 2 = 0 THEN CAST(id AS DOUBLE) END, rand(3)) AS c FROM range(0, 8, 1, 1) ORDER BY id",
            [
                (0, 0.0),
                (1, 0.25738143505962285),
                (2, 2.0),
                (3, 0.6698885713796182),
                (4, 4.0),
                (5, 0.9416651198480125),
                (6, 6.0),
                (7, 0.7298503424273113),
            ],
            id="coalesce",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(42) END AS a, CASE WHEN id % 2 = 1 THEN rand(42) END AS b FROM range(0, 6, 1, 1) ORDER BY id",
            [
                (0, 0.619189370225301, None),
                (1, None, 0.619189370225301),
                (2, 0.5096018842446481, None),
                (3, None, 0.5096018842446481),
                (4, 0.8325259388871524, None),
                (5, None, 0.8325259388871524),
            ],
            id="same-call-in-two-branches",
        ),
        pytest.param(
            "SELECT id, transform(array(1, 2), x -> x + rand(9)) AS t FROM range(0, 3, 1, 1) ORDER BY id",
            [
                (0, [1.7145670158242736, 2.4662177495071287]),
                (1, [1.0295477847893517, 2.2147438020110704]),
                (2, [1.9470787892248769, 2.739304388587289]),
            ],
            id="lambda-with-column",
        ),
        # Spark evaluates the calls in sort keys and window specifications for every row.
        pytest.param(
            "SELECT id FROM range(0, 10, 1, 1) ORDER BY CASE WHEN id % 2 = 0 THEN rand(42) END, id",
            [(1,), (3,), (5,), (7,), (9,), (0,), (4,), (2,), (8,), (6,)],
            id="order-by-case-when",
        ),
        pytest.param(
            "SELECT id, sum(id) OVER (ORDER BY CASE WHEN id % 2 = 0 THEN rand(42) END, id) AS w "
            "FROM range(0, 6, 1, 1) ORDER BY id",
            [(0, 11), (1, 1), (2, 11), (3, 4), (4, 15), (5, 9)],
            id="window-order-by-case-when",
        ),
        pytest.param(
            "SELECT max(id) AS m, rand(42) AS r, max(id) + monotonically_increasing_id() AS i, spark_partition_id() AS p FROM range(0, 10, 1, 2)",
            [(9, 0.619189370225301, 9, 0)],
            id="aggregate-output",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN rand(3) ELSE sum(id) OVER () END AS v FROM range(0, 6, 1, 1) ORDER BY id",
            [
                (0, 0.25738143505962285),
                (1, 15.0),
                (2, 0.6698885713796182),
                (3, 15.0),
                (4, 0.9416651198480125),
                (5, 15.0),
            ],
            id="case-when-around-window",
        ),
        pytest.param(
            "SELECT k, count(*) AS c FROM (SELECT CASE WHEN id % 2 = 0 THEN floor(rand(8) * 3) END AS k FROM range(0, 20, 1, 1)) GROUP BY k ORDER BY k",
            [(None, 10), (0, 2), (1, 5), (2, 3)],
            id="group-by-case-when-column",
        ),
        pytest.param(
            "SELECT CASE WHEN id % 2 = 0 THEN floor(rand(8) * 3) END AS k, count(*) AS c FROM range(0, 20, 1, 1) GROUP BY CASE WHEN id % 2 = 0 THEN floor(rand(8) * 3) END ORDER BY k",
            [(None, 10), (0, 3), (1, 4), (2, 3)],
            id="group-by-case-when-key",
        ),
    ],
)
def test_seeded_random_functions_per_partition(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


def test_seeded_random_function_on_the_right_side_of_and(spark):
    rows = spark.sql("SELECT id FROM range(0, 20, 1, 1) WHERE id % 2 = 0 AND rand(7) < 0.5 ORDER BY id").collect()
    assert [row.id for row in rows] == [0, 2, 12, 16]
    # A filter evaluates each conjunct for the rows that satisfy the previous ones, while `AND`
    # in other expressions also evaluates the right side when the left side is NULL.
    left = "CASE WHEN id % 3 = 0 THEN NULL ELSE id % 3 = 1 END"
    rows = spark.sql(f"SELECT id FROM range(0, 12, 1, 1) WHERE {left} AND rand(5) < 0.5 ORDER BY id").collect()  # noqa: S608
    assert [row.id for row in rows] == [1, 4]
    rows = spark.sql("SELECT id FROM range(0, 12, 1, 1) WHERE rand(5) < 0.5 AND id % 2 = 0 ORDER BY id").collect()
    assert [row.id for row in rows] == [0, 4, 6, 8]
    rows = spark.sql("SELECT id FROM range(0, 20, 1, 1) WHERE id > 5 AND rand(7) < 0.5 ORDER BY id").collect()
    assert [row.id for row in rows] == [6, 7, 12, 14, 17, 19]
    # The partitions of the rows are kept after a filter with a seeded random function.
    rows = spark.sql(
        "SELECT id, rand(42) AS r, spark_partition_id() AS p, monotonically_increasing_id() AS m "
        "FROM range(0, 6, 1, 2) WHERE id % 2 = 1 AND rand(7) < 0.8 ORDER BY id"
    ).collect()
    assert [tuple(row) for row in rows] == [
        (1, 0.619189370225301, 0, 0),
        (3, 0.8017532427858894, 1, 8589934592),
        (5, 0.6565552949992319, 1, 8589934593),
    ]


def test_tablesample_per_partition(spark):
    spark.sql("CREATE OR REPLACE TEMP VIEW seeded_random_sample_data AS SELECT * FROM range(0, 20, 1, 3)")
    try:
        rows = spark.sql(
            "SELECT id FROM seeded_random_sample_data TABLESAMPLE (50 PERCENT) REPEATABLE (5) ORDER BY id"
        ).collect()
        assert [row.id for row in rows] == [0, 1, 4, 5, 6, 8, 12, 13, 14, 19]
    finally:
        spark.sql("DROP VIEW IF EXISTS seeded_random_sample_data")


def test_dataframe_seeded_random_per_partition(spark):
    df = spark.range(0, 20, 1, 4)
    assert sorted(row.id for row in df.sample(withReplacement=False, fraction=0.5, seed=3).collect()) == [
        0,
        4,
        6,
        7,
        8,
        9,
        10,
        11,
        14,
        15,
        17,
    ]
    assert sorted(row.id for row in df.where(F.rand(9) < F.lit(0.5)).collect()) == [
        1,
        2,
        3,
        5,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        18,
        19,
    ]
    sorted_df = spark.range(0, 20, 1, 2).sortWithinPartitions(F.col("id").desc())
    sample = sorted_df.sample(withReplacement=False, fraction=0.5, seed=3)
    assert sorted(row.id for row in sample.collect()) == [1, 4, 5, 9, 11, 13, 14, 15, 16, 17, 18]
    strata = spark.range(0, 40, 1, 4).withColumn("k", F.col("id") % 2)
    sample = strata.stat.sampleBy("k", {0: 0.5, 1: 0.3}, seed=7)
    assert sorted(row.id for row in sample.collect()) == [0, 6, 8, 10, 12, 17, 18, 22, 23, 28, 29, 30, 34]
    rows = spark.range(0, 6, 1, 2).withColumn("r", F.when(F.col("id") % 2 == 0, F.rand(42))).orderBy("id").collect()
    assert [tuple(row) for row in rows] == [
        (0, 0.619189370225301),
        (1, None),
        (2, 0.5096018842446481),
        (3, None),
        (4, 0.8017532427858894),
        (5, None),
    ]
    rows = spark.range(0, 8, 1, 2).withColumn("r", F.rand(42)).orderBy("id").collect()
    assert [tuple(row) for row in rows] == [
        (0, 0.619189370225301),
        (1, 0.5096018842446481),
        (2, 0.8325259388871524),
        (3, 0.26322809041172357),
        (4, 0.8017532427858894),
        (5, 0.6565552949992319),
        (6, 0.2515595782593636),
        (7, 0.2073428376111074),
    ]


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_only_random_functions_with_a_seed_are_evaluated_per_partition(spark):
    def plan(query):
        return "\n".join(row[0] for row in spark.sql(f"EXPLAIN {query}").collect())

    assert "SeededRandomExec" in plan("SELECT rand(1) AS r FROM range(3)")
    assert "SeededRandomExec" not in plan("SELECT rand() AS r FROM range(3)")
    assert "SeededRandomExec" not in plan("SELECT id + 1 AS r FROM range(3)")


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_seeded_random_functions_do_not_prevent_column_pruning(spark, tmp_path):
    path = str(tmp_path / "wide")
    spark.range(0, 10, 1, 2).selectExpr("id AS a", "id AS b", "id AS c").write.parquet(path)
    df = spark.read.parquet(path)

    def scanned_columns(df):
        plan = df._explain_string()  # noqa: SLF001
        projections = re.findall(r"DataSourceExec:.*?projection=\[([^\]]*)\]", plan)
        return {name for projection in projections for name in re.findall(r"\b([abc])(?:@\d+)?\b", projection)}

    assert scanned_columns(df.select("a", F.rand(42).alias("r"))) == {"a"}
    assert scanned_columns(df.sample(fraction=0.1, seed=1).select("a")) == {"a"}


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_unused_seeded_random_columns_are_pruned(spark, tmp_path):
    path = str(tmp_path / "partitioned")
    spark.range(0, 10, 1, 2).selectExpr("id AS a", "id % 2 AS part").write.partitionBy("part").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("seeded_random_partitioned")
    try:
        # Spark prunes the unused random column, and then pushes the filter down to the scan.
        df = spark.sql("SELECT a FROM (SELECT a, part, rand(42) AS r FROM seeded_random_partitioned) WHERE part = 1")
        assert sorted(row.a for row in df.collect()) == [1, 3, 5, 7, 9]
        plan = df._explain_string()  # noqa: SLF001
        assert "SeededRandomExec" not in plan
        assert "FilterExec" not in plan
        df = spark.sql("SELECT a, r FROM (SELECT a, part, rand(42) AS r FROM seeded_random_partitioned) WHERE part = 1")
        assert "SeededRandomExec" in df._explain_string()  # noqa: SLF001
    finally:
        spark.sql("DROP VIEW IF EXISTS seeded_random_partitioned")


SUBQUERY = "(SELECT id AS k FROM range(0, 12, 3, 1))"
SUBQUERY_PREDICATES = {
    "in": f"id IN (SELECT k FROM {SUBQUERY})",  # noqa: S608
    "exists": f"EXISTS (SELECT 1 FROM {SUBQUERY} i WHERE i.k = o.id)",  # noqa: S608
    "scalar": f"id > (SELECT min(k) FROM {SUBQUERY})",  # noqa: S608
    "corr_scalar": f"id >= (SELECT count(*) FROM {SUBQUERY} i WHERE i.k < o.id)",  # noqa: S608
}


def subquery_predicate(case):
    """Returns the predicate of a case such as `det_and_in_and_rand`."""
    parts = []
    for part in case.split("_and_"):
        if part == "rand":
            parts.append("rand(7) < 0.7")
        elif part == "det":
            parts.append("id % 2 = 1")
        else:
            parts.append(SUBQUERY_PREDICATES[part])
    return " AND ".join(parts)


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        pytest.param("in_and_rand", [0, 3, 6], id="in-and-rand"),
        pytest.param("rand_and_in", [0, 3, 6], id="rand-and-in"),
        pytest.param("det_and_in_and_rand", [3, 9], id="det-and-in-and-rand"),
        pytest.param("exists_and_rand", [0, 3, 6], id="exists-and-rand"),
        pytest.param("rand_and_exists", [0, 3, 6], id="rand-and-exists"),
        pytest.param("det_and_exists_and_rand", [3, 9], id="det-and-exists-and-rand"),
        pytest.param("scalar_and_rand", [1, 2, 4, 5, 7, 9], id="scalar-and-rand"),
        pytest.param("rand_and_scalar", [1, 3, 4, 6, 8, 11], id="rand-and-scalar"),
        pytest.param("det_and_scalar_and_rand", [1, 3, 7, 9], id="det-and-scalar-and-rand"),
    ],
)
def test_seeded_random_filter_with_subquery(spark, case, expected):
    # Spark applies `IN` and correlated `EXISTS` subqueries after the other conjuncts, and
    # evaluates an uncorrelated scalar subquery in order with them.
    query = f"SELECT id FROM range(0, 12, 1, 1) o WHERE {subquery_predicate(case)} ORDER BY id"  # noqa: S608
    assert [row.id for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    "predicate",
    [
        "{c} AND rand(7) < 2",
        "rand(7) < 2 AND {c}",
        "{c} OR rand(7) < -1",
        "rand(7) < -1 OR {c}",
    ],
)
def test_seeded_random_filter_with_correlated_scalar_subquery(spark, predicate):
    c = SUBQUERY_PREDICATES["corr_scalar"]
    query = f"SELECT id FROM range(0, 6, 1, 2) o WHERE {predicate.format(c=c)} ORDER BY id"  # noqa: S608
    assert [row.id for row in spark.sql(query).collect()] == [0, 1, 2, 3, 4, 5]
