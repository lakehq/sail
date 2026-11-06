from collections import Counter

import pyspark.sql.functions as F  # noqa: N812


def rows(df):
    return Counter(tuple(row) for row in df.collect())


def test_a_cast_before_its_column_leaves_the_column_resolvable(spark):
    source = spark.sql("SELECT * FROM VALUES (1), (2) AS s(k)")

    grouped = source.alias("t1").groupBy(F.col("k").cast("bigint"), "k").count()
    assert rows(grouped) == Counter({(1, 1, 1): 1, (2, 2, 1): 1})
    assert rows(grouped.select("t1.k")) == Counter({(1,): 1, (2,): 1})

    grouped = source.groupBy(F.col("k").cast("bigint"), "k").count()
    assert rows(grouped.select(source["k"])) == Counter({(1,): 1, (2,): 1})


def test_grouping_sets_over_an_unqualified_column_and_its_cast(spark):
    df = spark.range(2)
    key = F.col("id").cast("string")
    subtotals = {(0, "0", 1): 1, (0, None, 1): 1, (1, "1", 1): 1, (1, None, 1): 1, (None, None, 2): 1}

    assert rows(df.rollup("id", key).count()) == Counter(subtotals)
    assert rows(df.cube("id", key).count()) == Counter({**subtotals, (None, "0", 1): 1, (None, "1", 1): 1})


def test_a_repeated_grouping_set_stays_a_grouping_set(spark):
    spark.sql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(k, v)").createOrReplaceTempView("repeated_sets")
    try:
        df = spark.sql("SELECT k, count(*) AS c FROM repeated_sets GROUP BY cube(k), cube(k)")
        assert rows(df) == Counter({(1, 1): 3, (2, 1): 3, (None, 2): 1})

        df = spark.sql("SELECT k, v, count(*) AS c FROM repeated_sets GROUP BY rollup(k, v), rollup(k, v)")
        assert sum(rows(df).values()) == 17  # noqa: PLR2004
    finally:
        spark.sql("DROP VIEW repeated_sets")


def test_a_cast_key_is_one_key_in_every_grouping_set(spark):
    spark.sql("SELECT * FROM VALUES (1), (2) AS t(k)").createOrReplaceTempView("cast_key_sets")
    try:
        df = spark.sql(
            "SELECT k, cast(k AS bigint) AS b, count(*) AS c FROM cast_key_sets "
            "GROUP BY GROUPING SETS ((k), (cast(k AS bigint)), (k, cast(k AS bigint)))"
        )
        assert rows(df) == Counter(
            {
                (1, None, 1): 1,
                (2, None, 1): 1,
                (None, 1, 1): 1,
                (None, 2, 1): 1,
                (1, 1, 1): 1,
                (2, 2, 1): 1,
            }
        )

        df = spark.sql(
            "SELECT k, count(*) AS c FROM cast_key_sets "
            "GROUP BY rollup(k, cast(k AS bigint)), rollup(k, cast(k AS bigint))"
        )
        assert sum(rows(df).values()) == 17  # noqa: PLR2004
    finally:
        spark.sql("DROP VIEW cast_key_sets")


def test_having_refers_to_the_column_and_not_to_its_cast(spark):
    spark.sql("SELECT * FROM VALUES (1), (2) AS t(k)").createOrReplaceTempView("cast_key_having")
    try:
        for keys in ["k, cast(k AS bigint)", "cast(k AS bigint), k"]:
            df = spark.sql(f"SELECT k, count(*) AS c FROM cast_key_having GROUP BY {keys} HAVING k > 1")  # noqa: S608
            assert rows(df) == Counter({(2, 1): 1})
    finally:
        spark.sql("DROP VIEW cast_key_having")


def test_order_by_a_grouping_function_over_a_cast_key(spark):
    spark.sql("SELECT * FROM VALUES (1), (2) AS t(k)").createOrReplaceTempView("cast_key_order")
    query = (
        "SELECT k, cast(k AS bigint) AS b, count(*) AS c FROM cast_key_order "
        "GROUP BY rollup(k, cast(k AS bigint)) ORDER BY {}, k, b"
    )
    try:
        df = spark.sql(query.format("grouping(cast(k AS bigint))"))
        assert [tuple(row) for row in df.collect()] == [
            (1, 1, 1),
            (2, 2, 1),
            (None, None, 2),
            (1, None, 1),
            (2, None, 1),
        ]

        df = spark.sql(query.format("grouping_id(k, cast(k AS bigint))"))
        assert [tuple(row) for row in df.collect()] == [
            (1, 1, 1),
            (2, 2, 1),
            (1, None, 1),
            (2, None, 1),
            (None, None, 2),
        ]
    finally:
        spark.sql("DROP VIEW cast_key_order")
