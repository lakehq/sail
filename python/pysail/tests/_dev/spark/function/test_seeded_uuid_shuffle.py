"""Tests for `uuid` and `shuffle` with a seed, and for the schema of random functions.

The expected values were verified on JVM Spark 4.2.0. Spark seeds `uuid` and `shuffle` with the
seed plus the partition index, so every input has an explicit number of partitions.
"""

import hashlib
import json

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT id, uuid(42) AS u FROM range(0, 12, 1, 4) ORDER BY id",
            [
                (0, "6f155395-c8b9-436b-a39c-d247226bc2b2"),
                (1, "92da2253-2186-4525-a440-17ad3083a275"),
                (2, "24ba1728-e648-487f-8c20-446d4411d15c"),
                (3, "e50eff7c-bd7d-43e5-be46-7ef61d49fb7d"),
                (4, "6c56eb72-2c80-4f64-a187-935dcd1768e0"),
                (5, "36db1e15-8ee7-476e-9c9e-e8ec48007aa6"),
                (6, "d51b4970-3ffc-4ee2-9511-43339d40888c"),
                (7, "63d5c85e-b781-47b3-b2f0-00bbd7ff3371"),
                (8, "970f8962-5d37-451c-952c-f28af59e0c6c"),
                (9, "4a41d402-15d8-41d0-8acd-4a8d59620edc"),
                (10, "44788e2a-9c76-4e1b-8765-4313cdb96af6"),
                (11, "6177fa9c-0ade-4f46-9c58-bb26f9530784"),
            ],
            id="uuid",
        ),
        pytest.param(
            "SELECT id, uuid(-7) AS u FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, "f840fc81-94a7-4780-b833-50470b4d74a9"),
                (1, "66146566-7af5-4f54-9bb0-28a16bff9c2f"),
                (2, "e3d25bfa-9824-40c5-82b6-c0bf187d3fd9"),
                (3, "5c5fd112-4ac3-41e5-b44b-4602902e8f1e"),
                (4, "4f624c34-ca3a-4717-b2d7-30afcf677577"),
                (5, "05e1d14c-2d02-420a-b321-91870982998e"),
            ],
            id="uuid-negative-seed",
        ),
        pytest.param(
            "SELECT id, uuid(9223372036854775807) AS u FROM range(0, 4, 1, 2) ORDER BY id",
            [
                (0, "596c0db4-d77a-4abb-9c91-5e7f323f02b0"),
                (1, "98e2f79d-4b33-4ca4-b094-4ff3c2ee7d7b"),
                (2, "444b2d95-8a54-40b5-950e-9ef88de5a4fd"),
                (3, "e3ff5c8a-242c-467f-b1d3-ca6b6acb483d"),
            ],
            id="uuid-max-seed",
        ),
        pytest.param(
            "SELECT id, shuffle(array(1, 2, 3, 4, 5), 42) AS s FROM range(0, 12, 1, 4) ORDER BY id",
            [
                (0, [5, 3, 2, 1, 4]),
                (1, [4, 2, 3, 1, 5]),
                (2, [3, 1, 4, 2, 5]),
                (3, [4, 2, 3, 1, 5]),
                (4, [5, 1, 3, 2, 4]),
                (5, [3, 5, 2, 1, 4]),
                (6, [2, 3, 4, 5, 1]),
                (7, [5, 2, 4, 1, 3]),
                (8, [5, 2, 4, 3, 1]),
                (9, [2, 5, 1, 3, 4]),
                (10, [1, 4, 3, 2, 5]),
                (11, [1, 2, 4, 3, 5]),
            ],
            id="shuffle",
        ),
        pytest.param(
            "SELECT id, shuffle(sequence(0, CAST(id AS INT)), 7) AS s FROM range(0, 12, 1, 3) ORDER BY id",
            [
                (0, [0]),
                (1, [1, 0]),
                (2, [2, 0, 1]),
                (3, [2, 0, 3, 1]),
                (4, [1, 4, 0, 3, 2]),
                (5, [2, 0, 1, 5, 4, 3]),
                (6, [4, 0, 2, 6, 3, 5, 1]),
                (7, [5, 1, 7, 4, 2, 6, 0, 3]),
                (8, [8, 6, 4, 5, 3, 0, 7, 1, 2]),
                (9, [3, 5, 2, 6, 8, 1, 4, 7, 0, 9]),
                (10, [9, 0, 4, 3, 7, 2, 5, 1, 10, 6, 8]),
                (11, [0, 3, 5, 7, 2, 11, 6, 4, 9, 1, 8, 10]),
            ],
            id="shuffle-column",
        ),
        # A null array does not advance the generator.
        pytest.param(
            "SELECT id, shuffle(CASE WHEN id % 3 = 0 THEN NULL ELSE array(1, NULL, 3, CAST(id AS INT)) END, 5) AS s "
            "FROM range(0, 9, 1, 2) ORDER BY id",
            [
                (0, None),
                (1, [3, 1, 1, None]),
                (2, [3, 2, None, 1]),
                (3, None),
                (4, [1, 4, None, 3]),
                (5, [3, 1, None, 5]),
                (6, None),
                (7, [3, 1, 7, None]),
                (8, [3, None, 8, 1]),
            ],
            id="shuffle-null-arrays",
        ),
        pytest.param(
            "SELECT shuffle(a, 3) AS s FROM VALUES (array(1, 2, 3)), (NULL), (array(4, 5)), (array()), (array(6)), "
            "(array(7, 8, 9)) AS t(a)",
            [([2, 1, 3],), (None,), ([5, 4],), ([],), ([6],), ([9, 7, 8],)],
            id="shuffle-values",
        ),
        pytest.param(
            "SELECT shuffle(CAST(NULL AS ARRAY<INT>), 1) AS s FROM range(0, 3, 1, 1)",
            [(None,), (None,), (None,)],
            id="shuffle-all-null",
        ),
        pytest.param(
            "SELECT id, shuffle(array('a', 'b', 'c', NULL), 3) AS s FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, [None, "a", "c", "b"]),
                (1, ["a", None, "c", "b"]),
                (2, ["a", "c", None, "b"]),
                (3, ["b", "c", None, "a"]),
                (4, [None, "a", "c", "b"]),
                (5, ["b", None, "a", "c"]),
            ],
            id="shuffle-strings",
        ),
        pytest.param(
            "SELECT id, shuffle(array(array(1, 2), array(3), NULL), 11) AS s FROM range(0, 6, 1, 2) ORDER BY id",
            [
                (0, [[3], None, [1, 2]]),
                (1, [[3], None, [1, 2]]),
                (2, [[1, 2], [3], None]),
                (3, [[3], [1, 2], None]),
                (4, [None, [1, 2], [3]]),
                (5, [[1, 2], [3], None]),
            ],
            id="shuffle-nested",
        ),
        pytest.param(
            "SELECT id, shuffle(array(rand(1), rand(2), 0.5D), 3) AS s FROM range(0, 4, 1, 2) ORDER BY id",
            [
                (0, [0.5311207224659675, 0.6363787615254752, 0.5]),
                (1, [0.5, 0.5993846534021868, 0.2861372051669987]),
                (2, [0.25738143505962285, 0.5, 0.5311207224659675]),
                (3, [0.2861372051669987, 0.5, 0.6698885713796182]),
            ],
            id="shuffle-of-rand",
        ),
        # Spark advances the generator only for the rows that evaluate the call.
        pytest.param(
            "SELECT id, CASE WHEN id % 3 = 0 THEN shuffle(sequence(0, CAST(id AS INT)), 4) END AS s "
            "FROM range(0, 10, 1, 1) ORDER BY id",
            [
                (0, [0]),
                (1, None),
                (2, None),
                (3, [2, 0, 1, 3]),
                (4, None),
                (5, None),
                (6, [4, 3, 1, 2, 5, 0, 6]),
                (7, None),
                (8, None),
                (9, [9, 3, 7, 2, 0, 6, 5, 1, 8, 4]),
            ],
            id="shuffle-case-when",
        ),
        pytest.param(
            "SELECT id FROM range(0, 20, 1, 1) WHERE id % 2 = 0 AND uuid(5) < '8' ORDER BY id",
            [(4,), (8,), (10,), (12,)],
            id="uuid-and",
        ),
        pytest.param(
            "SELECT id FROM range(0, 12, 1, 2) WHERE shuffle(array(1, 2, 3), 9)[0] = 1 ORDER BY id",
            [(3,), (5,), (9,)],
            id="shuffle-filter",
        ),
        pytest.param(
            "SELECT id FROM range(0, 8, 1, 2) ORDER BY shuffle(array(1, 2, 3), 5), id",
            [(3,), (4,), (5,), (2,), (0,), (1,), (6,), (7,)],
            id="shuffle-order-by-key",
        ),
    ],
)
def test_seeded_uuid_and_shuffle_per_partition(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # The generators continue across the batches of a partition.
        pytest.param(
            "SELECT id, uuid(1) AS u FROM range(0, 40000, 1, 3) ORDER BY id",
            "5cdf1a6f673a076cf1c070217d5cfd8c",
            id="uuid",
        ),
        pytest.param(
            "SELECT id, shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), 1) AS s FROM range(0, 40000, 1, 3) ORDER BY id",
            "cea667bbaebc147c1b1c15c48887f625",
            id="shuffle",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN uuid(3) END AS u FROM range(0, 20000, 1, 2) ORDER BY id",
            "f5367c85fa03a53c4363f051f4404504",
            id="uuid-case-when",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id % 2 = 0 THEN shuffle(array(1, 2, 3, 4), 3) END AS s "
            "FROM range(0, 20000, 1, 2) ORDER BY id",
            "aa5d420bf0a948fecbdd5afe7d0eb704",
            id="shuffle-case-when",
        ),
    ],
)
def test_seeded_uuid_and_shuffle_many_rows(spark, query, expected):
    rows = [[str(c) for c in row] for row in spark.sql(query).collect()]
    assert hashlib.md5(json.dumps(rows).encode()).hexdigest() == expected  # noqa: S324


def test_seeded_uuid_and_shuffle_dataframe(spark):
    df = spark.range(0, 6, 1, 2).select(
        "id", F.uuid(7).alias("u"), F.shuffle(F.array(F.lit(1), F.lit(2), F.lit(3)), 7).alias("s")
    )
    assert [tuple(row) for row in df.orderBy("id").collect()] == [
        (0, "cee40aac-b68a-440d-b75a-41718652cb36", [1, 2, 3]),
        (1, "50fe06d6-e030-43ea-9ff6-ff3a773b207a", [3, 1, 2]),
        (2, "9bf20e3c-97d1-45bc-8959-2e07ee8cb0f7", [3, 1, 2]),
        (3, "6c186627-616a-45dd-85c2-ac0fd2cf712d", [2, 3, 1]),
        (4, "57eb05c8-7dfb-4f3c-8c90-54cb6d4902b1", [2, 3, 1]),
        (5, "e593bb6b-070f-48c1-b5d1-2fe7477a08e6", [3, 1, 2]),
    ]


def test_seeded_uuid_and_shuffle_column_names(spark):
    df = spark.sql("SELECT uuid(42), shuffle(array(1, 2), 42), uuid(), shuffle(array(1, 2)) FROM range(0, 1)")
    assert df.columns == ["uuid()", "shuffle(array(1, 2))", "uuid()", "shuffle(array(1, 2))"]


def test_unseeded_shuffle_differs_across_rows(spark):
    rows = spark.sql("SELECT shuffle(array(1, 2, 3, 4, 5, 6)) AS s FROM range(0, 20, 1, 1)").collect()
    assert len({tuple(row.s) for row in rows}) > 1


def test_null_seed_is_no_seed(spark):
    rows = spark.sql(
        "SELECT uuid(NULL) AS u, shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), NULL) AS s FROM range(0, 20, 1, 1)"
    ).collect()
    assert len({row.u for row in rows}) == 20  # noqa: PLR2004
    assert len({tuple(row.s) for row in rows}) > 1


def test_random_functions_are_not_nullable(spark):
    df = spark.sql(
        "SELECT rand(1) AS a, randn(1) AS b, rand() AS c, randn() AS d, uniform(1, 10, 1) AS e, "
        "randstr(5, 1) AS f, randstr(5) AS g, uuid() AS h, uuid(1) AS i, shuffle(array(1, 2, 3), 1) AS j"
    )
    assert [(f.name, f.nullable) for f in df.schema.fields] == [
        (name, False) for name in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    ]
