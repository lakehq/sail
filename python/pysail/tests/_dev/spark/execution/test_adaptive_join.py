# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing.spark.session import spark_connect_server

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail cluster configuration")

ROWS = 1_000_000
STEP = 100_000
OFFSET = 7
KEYS = list(range(OFFSET, ROWS, STEP))


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={"SAIL_MODE": "local-cluster", "SAIL_CLUSTER__ENABLE_ADAPTIVE_JOIN_SELECTION": "true"}
    ) as server:
        yield server.remote


def with_null_row(spark, df):
    schema = StructType([StructField(name, LongType(), True) for name in df.columns])
    return df.unionByName(spark.createDataFrame([tuple(None for _ in df.columns)], schema))


@pytest.fixture
def big(spark):
    df = spark.range(0, ROWS, 1, 4).select(F.col("id").alias("k"), (F.col("id") * 2).alias("v"))
    return with_null_row(spark, df)


@pytest.fixture
def small(spark):
    # The filter is estimated to keep 20% of the rows, but keeps 10.
    df = spark.range(0, ROWS, 1, 4).where(F.col("id") % STEP == OFFSET).select(F.col("id").alias("sk"))
    return with_null_row(spark, df)


def summary(df):
    row = df.agg(
        F.count("*").alias("rows"), F.count("k").alias("k"), F.count("sk").alias("sk"), F.sum("v").alias("v")
    ).collect()[0]
    return row.asDict()


@pytest.mark.timeout(120)
def test_inner_join(big, small):
    rows = big.join(small, big.k == small.sk, "inner").orderBy("k").collect()
    assert rows == [Row(k=k, v=k * 2, sk=k) for k in KEYS]


@pytest.mark.timeout(120)
def test_right_join(big, small):
    df = big.join(small, big.k == small.sk, "right")
    assert summary(df) == {"rows": len(KEYS) + 1, "k": len(KEYS), "sk": len(KEYS), "v": sum(KEYS) * 2}


@pytest.mark.timeout(120)
def test_left_join(big, small):
    df = big.join(small, big.k == small.sk, "left")
    assert summary(df) == {"rows": ROWS + 1, "k": ROWS, "sk": len(KEYS), "v": ROWS * (ROWS - 1)}


@pytest.mark.timeout(120)
def test_full_join(big, small):
    df = big.join(small, big.k == small.sk, "full")
    assert summary(df) == {"rows": ROWS + 2, "k": ROWS, "sk": len(KEYS), "v": ROWS * (ROWS - 1)}


@pytest.mark.timeout(120)
def test_semi_join(big, small):
    rows = big.join(small, big.k == small.sk, "left_semi").orderBy("k").collect()
    assert rows == [Row(k=k, v=k * 2) for k in KEYS]


@pytest.mark.timeout(120)
def test_anti_join(big, small):
    df = big.join(small, big.k == small.sk, "left_anti")
    row = df.agg(F.count("*").alias("rows"), F.count("k").alias("k")).collect()[0]
    assert row.asDict() == {"rows": ROWS + 1 - len(KEYS), "k": ROWS - len(KEYS)}


@pytest.mark.timeout(120)
def test_nested_joins(big, small):
    # A selective join feeding an aggregate join on the same key, as in TPC-H Q17.
    totals = big.groupBy("k").agg(F.sum("v").alias("total"))
    df = big.join(small, big.k == small.sk).join(totals, "k").where(F.col("v") <= F.col("total"))
    assert df.orderBy("k").select("k", "total").collect() == [Row(k=k, total=k * 2) for k in KEYS]


# Large enough that the filtered dimension is estimated above the broadcast row limit,
# so the semijoin reduction is planned speculatively and decided at runtime.
FACT_ROWS = 4_000_000
DIMENSION_KEYS = 800_000
SELECTED = 10
NULL_EVERY = 1000


@pytest.fixture
def q17_tables(spark, tmp_path):
    # Every fact key repeats, some fact keys are NULL, and the dimension has a NULL key
    # and a duplicate of key 0.
    facts = spark.range(0, FACT_ROWS, 1, 4).select(
        F.when(F.col("id") % NULL_EVERY == NULL_EVERY - 1, F.lit(None))
        .otherwise(F.col("id") % DIMENSION_KEYS)
        .alias("k"),
        (F.col("id") % 7).alias("v"),
    )
    dimension = spark.range(0, DIMENSION_KEYS, 1, 4).select(
        F.col("id").alias("k"),
        F.when(F.col("id") % (DIMENSION_KEYS // SELECTED) == 0, "selected").otherwise("other").alias("label"),
    )
    extra = spark.createDataFrame([(None, "selected"), (0, "selected")], "k long, label string")
    facts.write.parquet(str(tmp_path / "facts"))
    dimension.unionByName(extra).write.parquet(str(tmp_path / "dimension"))
    spark.read.parquet(str(tmp_path / "facts")).createOrReplaceTempView("facts")
    spark.read.parquet(str(tmp_path / "dimension")).createOrReplaceTempView("dimension")
    yield
    spark.catalog.dropTempView("facts")
    spark.catalog.dropTempView("dimension")


def q17(labels):
    return f"""
        SELECT COUNT(*) AS n, SUM(f.v) AS total
        FROM facts f
        JOIN dimension d ON f.k = d.k
        JOIN (SELECT k, AVG(v) AS a FROM facts GROUP BY k) g ON d.k = g.k
        WHERE d.label IN ({labels}) AND f.v < g.a
    """  # noqa: S608


def expected_q17(selected):
    values = {}
    for i in range(FACT_ROWS):
        if i % NULL_EVERY != NULL_EVERY - 1:
            values.setdefault(i % DIMENSION_KEYS, []).append(i % 7)
    n = total = 0
    for key in selected:
        key_values = values.get(key, [])
        if not key_values:
            continue
        average = sum(key_values) / len(key_values)
        matches = [v for v in key_values if v < average]
        copies = 2 if key == 0 else 1
        n += len(matches) * copies
        total += sum(matches) * copies
    return {"n": n, "total": total}


@pytest.mark.timeout(300)
def test_selective_reduction_is_kept(spark, q17_tables):  # noqa: ARG001
    query = q17("'selected'")
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert "__early_join_key_0" in plan
    row = spark.sql(query).collect()[0]
    assert row.asDict() == expected_q17(range(0, DIMENSION_KEYS, DIMENSION_KEYS // SELECTED))


@pytest.mark.timeout(300)
def test_unselective_reduction_is_removed(spark, q17_tables):  # noqa: ARG001
    query = q17("'selected', 'other'")
    plan = spark.sql(f"EXPLAIN {query}").collect()[0][0]
    assert "__early_join_key_0" in plan
    row = spark.sql(query).collect()[0]
    assert row.asDict() == expected_q17(range(DIMENSION_KEYS))


@pytest.mark.timeout(120)
def test_join_of_two_large_sides(spark, big):
    # Both sides pass the broadcast threshold, so the join reads both from their sub-jobs.
    other = spark.range(0, ROWS, 1, 4).select(F.col("id").alias("ok"), (F.col("id") * 3).alias("ov"))
    row = big.join(other, big.k == other.ok, "inner").agg(F.count("*").alias("rows"), F.sum("ov").alias("ov"))
    assert row.collect()[0].asDict() == {"rows": ROWS, "ov": 3 * ROWS * (ROWS - 1) // 2}


@pytest.mark.timeout(120)
def test_join_with_large_aggregate_side(big):
    # The aggregate side is partitioned without a repartition on top of it.
    counts = big.where(F.col("k").isNotNull()).groupBy("k").agg(F.count("*").alias("n"))
    row = big.join(counts, "k").agg(F.count("*").alias("rows"), F.sum("n").alias("n"), F.sum("v").alias("v"))
    assert row.collect()[0].asDict() == {"rows": ROWS, "n": ROWS, "v": ROWS * (ROWS - 1)}
