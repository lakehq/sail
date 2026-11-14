"""`DataFrame.tail`, which returns the last rows of one evaluation of the input
(Spark's `CollectTailExec`)."""

import random

import pytest
from pyspark.errors import PySparkException


@pytest.mark.parametrize(
    ("num", "expected"),
    [(0, []), (3, [7, 8, 9]), (20, list(range(10))), (2147483647, list(range(10)))],
    ids=["zero", "some", "more-than-rows", "max-int"],
)
def test_tail(spark, num, expected):
    assert [r.id for r in spark.range(0, 10, 1, 1).tail(num)] == expected
    assert [r.id for r in spark.range(0, 10, 1, 3).orderBy("id").tail(num)] == expected


@pytest.mark.parametrize("num", [3, 20])
def test_tail_of_partitions(spark, num):
    rows = [r.id for r in spark.range(0, 10, 1, 3).tail(num)]
    assert len(rows) == min(num, 10)
    assert set(rows) <= set(range(10))


def test_negative_tail(spark):
    with pytest.raises(PySparkException, match="must be equal to or greater than 0"):
        spark.range(10).tail(-1)


def test_tail_of_empty_input(spark):
    assert spark.range(0).tail(3) == []
    assert spark.range(0, 10, 1, 3).where("id < 0").tail(3) == []


def test_tail_of_sorted_input(spark):
    df = spark.range(0, 1000, 1, 4).orderBy("id", ascending=False)
    assert [r.id for r in df.tail(3)] == [2, 1, 0]


def test_tail_of_sorted_files(spark, tmp_path):
    path = str(tmp_path / "data")
    spark.range(0, 40000, 1, 4).write.parquet(path)
    df = spark.read.parquet(path).orderBy("id")
    assert [r.id for r in df.tail(5)] == list(range(39995, 40000))


@pytest.mark.parametrize("seed", [1729, 9001, 20261113])
def test_tail_of_seeded_nested_rows(spark, seed):
    """A stable total order gives a Python oracle for nullable, nested payloads after
    repartitioning. Counts cut both inside and beyond the retained rows."""
    rng = random.Random(seed)
    rows = [
        (
            i,
            rng.choice([None, -2, -1, 0, 1, 2]),
            None if i % 7 == 0 else (rng.choice([None, "", "é", "x" * 80]), [i, None, -i]),
            None if i % 5 == 0 else {"a": i, "b": None},
            None if i % 3 == 0 else bytes([i, 255 - i]),
        )
        for i in range(37)
    ]
    rng.shuffle(rows)
    df = spark.createDataFrame(
        rows,
        "id INT, k INT, payload STRUCT<label:STRING,items:ARRAY<INT>>, tags MAP<STRING,INT>, raw BINARY",
    ).repartition(3)
    ordered = df.orderBy(df.k.asc_nulls_last(), df.id.desc())
    expected_ids = [row[0] for row in sorted(rows, key=lambda row: (row[1] is None, row[1] or 0, -row[0]))]
    expected = ordered.collect()
    assert [row.id for row in expected] == expected_ids
    for count in [0, 1, 17, len(rows), len(rows) + 1]:
        assert ordered.tail(count) == (expected[-count:] if count else [])
