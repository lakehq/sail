"""Tests for seeding random functions once per partition, as Spark does.

The expected values were verified on JVM Spark 4.2.0. They depend on the partition of each row,
so every input has an explicit number of partitions.
"""

import re

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


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
    ],
)
def test_seeded_random_functions_per_partition(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail evaluates the right side of AND for every row, which advances the generator",
    strict=True,
)
def test_seeded_random_function_on_the_right_side_of_and(spark):
    rows = spark.sql("SELECT id FROM range(0, 20, 1, 1) WHERE id % 2 = 0 AND rand(7) < 0.5 ORDER BY id").collect()
    assert [row.id for row in rows] == [0, 2, 12, 16]


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
