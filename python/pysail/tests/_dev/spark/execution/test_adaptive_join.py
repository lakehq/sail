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
