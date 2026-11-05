import pytest
from pyspark.sql.types import BooleanType, DoubleType, LongType, StructField, StructType


@pytest.fixture
def correlated_views(spark):
    # DataFrame temp views retain a different alias boundary from SQL VALUES views.
    spark.createDataFrame([(key, None) for key in [0, 1, 2, None]], "id BIGINT, v BIGINT").createOrReplaceTempView("o")
    spark.createDataFrame([(0, 10), (0, None), (2, None), (None, 99)], "id BIGINT, v BIGINT").createOrReplaceTempView(
        "i"
    )
    try:
        yield spark
    finally:
        spark.catalog.dropTempView("o")
        spark.catalog.dropTempView("i")


def test_left_lateral_count_empty_input(correlated_views):
    result = correlated_views.sql("""
        SELECT o.id, s.c FROM o LEFT JOIN LATERAL
          (SELECT count(1) AS c FROM i WHERE i.id = o.id) s ON s.c = 0
        ORDER BY o.id NULLS LAST
    """)
    assert result.schema == StructType([StructField("id", LongType()), StructField("c", LongType())])
    assert [tuple(row) for row in result.collect()] == [(0, None), (1, 0), (2, None), (None, 0)]


def regex_output(prefix, random):
    return rf"rlike(concat(r'{prefix}', CAST(CAST(count(1) AS DOUBLE){random} AS STRING)), r'a\\b')"


@pytest.mark.parametrize(("prefix", "matched"), [(r"a\b", True), ("ab", False)])
def test_correlated_grouped_volatile_output(correlated_views, prefix, matched):
    result = correlated_views.sql(f"""
        SELECT o.id FROM o WHERE true IN
          (SELECT {regex_output(prefix, " + rand()")} FROM i WHERE i.id = o.id GROUP BY i.id)
        ORDER BY o.id NULLS LAST
    """)  # noqa: S608
    assert result.schema == StructType([StructField("id", LongType())])
    assert [tuple(row) for row in result.collect()] == ([(0,), (2,)] if matched else [])


@pytest.mark.parametrize(("prefix", "matched"), [(r"a\b", True), ("ab", False)])
def test_correlated_having_keeps_or_rejects_empty_input(correlated_views, prefix, matched):
    result = correlated_views.sql(f"""
        SELECT o.id,
          (SELECT count(*) FROM i WHERE i.id = o.id HAVING {regex_output(prefix, "")}) AS rows
        FROM o ORDER BY o.id NULLS LAST
    """)  # noqa: S608
    assert result.schema == StructType([StructField("id", LongType()), StructField("rows", LongType())])
    assert [tuple(row) for row in result.collect()] == [
        (key, count if matched else None) for key, count in [(0, 2), (1, 0), (2, 1), (None, 0)]
    ]


# Spark 4.2 join-lateral.sql, SPARK-37716: determinism depends on outer cardinality.
@pytest.mark.parametrize(("prefix", "matched"), [(r"a\b", True), ("ab", False)])
@pytest.mark.parametrize("random", ["", " + rand()"], ids=["deterministic", "volatile"])
def test_multirow_lateral_determinism(correlated_views, prefix, matched, random):
    sql = f"""
        SELECT o.id, s.matched FROM o CROSS JOIN LATERAL
          (SELECT {regex_output(prefix, random)} AS matched FROM i WHERE i.id = o.id) s
        ORDER BY o.id NULLS LAST
    """  # noqa: S608
    if random:
        with pytest.raises(Exception, match="NON_DETERMINISTIC_LATERAL_SUBQUERIES"):
            correlated_views.sql(sql).collect()
    else:
        result = correlated_views.sql(sql)
        assert result.schema == StructType(
            [StructField("id", LongType()), StructField("matched", BooleanType(), False)]
        )
        assert [tuple(row) for row in result.collect()] == [(key, matched) for key in [0, 1, 2, None]]


@pytest.mark.parametrize(
    "outer",
    [
        "SELECT CAST(0 AS BIGINT) AS id",
        "SELECT min(id) AS id FROM o",
        "SELECT id FROM o ORDER BY id NULLS LAST LIMIT 1",
    ],
    ids=["one-row", "aggregate", "limit"],
)
def test_single_row_volatile_lateral(correlated_views, outer):
    expression = regex_output(r"a\b", " + rand()")
    result = correlated_views.sql(f"""
        SELECT o.id, s.matched FROM ({outer}) o CROSS JOIN LATERAL
          (SELECT {expression} AS matched FROM i WHERE i.id = o.id) s
    """)  # noqa: S608
    assert [tuple(row) for row in result.collect()] == [(0, True)]
    assert result.schema.simpleString() == "struct<id:bigint,matched:boolean>"
    assert result.schema["matched"].nullable is False


def test_grouped_random_output(correlated_views):
    result = correlated_views.sql("SELECT id, count(1) + rand() AS c FROM i GROUP BY id ORDER BY id NULLS LAST")
    rows = result.collect()
    assert [row.id for row in rows] == [0, 2, None]
    assert all(n <= row.c < n + 1 for row, n in zip(rows, [2, 1, 1], strict=True))
    assert result.schema == StructType([StructField("id", LongType()), StructField("c", DoubleType(), False)])


@pytest.mark.parametrize("grouping", ["", "GROUP BY id"], ids=["global", "grouped"])
@pytest.mark.parametrize("aggregate", ["sum(rand())", "count(1) FILTER (WHERE rand() > 0.5)"])
def test_volatile_aggregate_input_rejected(correlated_views, grouping, aggregate):
    with pytest.raises(Exception, match=r"(?i)(non.deterministic|nondeterministic)"):
        correlated_views.sql(f"SELECT {aggregate} FROM i {grouping}").collect()  # noqa: S608


def test_lateral_volatile_join_condition(correlated_views):
    with pytest.raises(Exception, match="LATERAL_JOIN_CONDITION_NON_DETERMINISTIC"):
        correlated_views.sql("SELECT * FROM o JOIN LATERAL (SELECT id FROM i) s ON o.id + rand() = s.id").collect()


def test_uncorrelated_multirow_volatile_lateral(correlated_views):
    with pytest.raises(Exception, match="NON_DETERMINISTIC_LATERAL_SUBQUERIES"):
        correlated_views.sql("SELECT * FROM o CROSS JOIN LATERAL (SELECT rand() AS r) s").collect()
