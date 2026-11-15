"""Nested seeded calls in keys preserve the semantics of Spark's extracted projections.

The direct-key and explicit-projection forms were compared on JVM Spark 4.2.0.
"""

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("partitions", [1, 2])
@pytest.mark.parametrize("operation", ["sort", "local-sort", "group", "repartition"])
@pytest.mark.parametrize(
    ("keys", "hoisted"),
    [
        pytest.param(["shuffle(array(CAST(id % 3 AS DOUBLE), 0D), 3)"], {}, id="flat-control"),
        pytest.param(["shuffle(array(rand(1), 0D), 3)"], {}, id="nested"),
        pytest.param(["shuffle(shuffle(array(rand(1), rand(2), 0D), 3), 4)"], {}, id="deep"),
        pytest.param(
            ["array(shuffle(array(rand(1), 0D), 3), shuffle(array(rand(2), 1D), 4))"],
            {},
            id="siblings",
        ),
        pytest.param(
            ["shuffle(array(rand(1), 0D), 3)", "shuffle(array(rand(2), 0D), 4)"],
            {},
            id="multiple-keys",
        ),
        pytest.param(
            ["shuffle(array(rand(1), 0D), 3)", "shuffle(array(rand(1), 0D), 3)", "rand(1)"],
            {},
            id="repeated-key-and-sibling",
        ),
        pytest.param(
            ["shuffle(array(CASE WHEN id % 2 = 0 THEN rand(1) ELSE -1D END, 0D), 3)"],
            {},
            id="conditional-inside",
        ),
        pytest.param(
            ["shuffle(array(CASE WHEN id % 2 = 0 THEN rand(1) ELSE -1D END, 0D), 3)", "rand(1)"],
            {},
            id="conditional-inside-and-sibling",
        ),
        pytest.param(
            ["shuffle(CASE WHEN id % 2 = 0 THEN array(rand(1), 0D) ELSE CAST(NULL AS ARRAY<DOUBLE>) END, 3)"],
            {},
            id="nullable-inside",
        ),
        pytest.param(
            ["CASE WHEN id % 2 = 0 THEN shuffle(array(rand(1), 0D), 3) ELSE array(-1D, 0D) END"],
            {"_call": "shuffle(array(rand(1), 0D), 3)"},
            id="conditional-outside",
        ),
    ],
)
def test_seeded_random_dependencies_in_keys(spark, partitions, operation, keys, hoisted):
    source = spark.range(0, 8, 1, partitions)
    reference = source
    reference_keys = keys
    # Spark pulls a whole nondeterministic call out of its enclosing condition, while
    # conditions inside that call still consume random values only when evaluated.
    for name, expression in hoisted.items():
        reference = reference.withColumn(name, F.expr(expression))
        reference_keys = [key.replace(expression, name) for key in reference_keys]
    reference = reference.select("id", *[F.expr(key).alias(f"k{i}") for i, key in enumerate(reference_keys)])
    actual_keys = [F.expr(key) for key in keys]
    expected_keys = [F.col(f"k{i}") for i in range(len(keys))]
    actual = _collect_key_operation(source, actual_keys, operation)
    expected = _collect_key_operation(reference, expected_keys, operation)
    assert actual == expected


def _collect_key_operation(df, keys, operation):
    if operation == "sort":
        return df.orderBy(*keys, F.col("id")).select("id").collect()
    if operation == "local-sort":
        return _partition_order(df.sortWithinPartitions(*keys, F.col("id")))
    if operation == "group":
        names = [f"k{i}" for i in range(len(keys))]
        return (
            df.groupBy(*[key.alias(name) for key, name in zip(keys, names, strict=True)])
            .agg(F.sort_array(F.collect_list("id")).alias("ids"))
            .orderBy(*names)
            .collect()
        )
    # Compare partition assignments within each engine; physical partition IDs need not
    # match between Spark and Sail, but the direct and projected keys must agree.
    return df.repartition(3, *keys).select("id", F.spark_partition_id().alias("p")).orderBy("id").collect()


def _partition_order(df):
    rows = df.select("id", F.spark_partition_id().alias("p")).collect()
    # Collection order between partitions is unspecified. Preserve each partition's
    # row order, which is what sortWithinPartitions promises.
    partitions = {}
    for row in rows:
        partitions.setdefault(row.p, []).append(row.id)
    return sorted(partitions.values())


def test_nested_seeded_random_in_sql_order_by(spark):
    actual = spark.sql("SELECT id FROM range(4) ORDER BY shuffle(array(rand(1), 0D), 3)").collect()
    expected = spark.sql(
        "WITH keyed AS (SELECT id, shuffle(array(rand(1), 0D), 3) AS k FROM range(4)) SELECT id FROM keyed ORDER BY k"
    ).collect()
    assert actual == expected


@pytest.mark.parametrize("partitions", [1, 2])
@pytest.mark.parametrize(
    ("query", "reference", "local"),
    [
        pytest.param(
            "SELECT id FROM range(0, 8, 1, {partitions}) ORDER BY shuffle(array(rand(1), 0D), 3)",
            "WITH keyed AS (SELECT id, shuffle(array(rand(1), 0D), 3) AS k "
            "FROM range(0, 8, 1, {partitions})) SELECT id FROM keyed ORDER BY k",
            False,
            id="order-by",
        ),
        pytest.param(
            "SELECT id FROM range(0, 8, 1, {partitions}) SORT BY shuffle(array(rand(1), 0D), 3)",
            "WITH keyed AS (SELECT id, shuffle(array(rand(1), 0D), 3) AS k "
            "FROM range(0, 8, 1, {partitions})) SELECT id FROM keyed SORT BY k",
            True,
            id="sort-by",
        ),
        pytest.param(
            "SELECT shuffle(array(rand(1), 0D), 3) AS k, count(*) AS n "
            "FROM range(0, 8, 1, {partitions}) GROUP BY shuffle(array(rand(1), 0D), 3) ORDER BY k",
            "WITH keyed AS (SELECT id, shuffle(array(rand(1), 0D), 3) AS k "
            "FROM range(0, 8, 1, {partitions})) SELECT k, count(*) AS n FROM keyed GROUP BY k ORDER BY k",
            False,
            id="group-by",
        ),
    ],
)
def test_seeded_random_dependencies_in_sql_keys(spark, partitions, query, reference, local):
    actual = spark.sql(query.format(partitions=partitions))
    expected = spark.sql(reference.format(partitions=partitions))
    if local:
        assert _partition_order(actual) == _partition_order(expected)
    else:
        assert actual.collect() == expected.collect()
