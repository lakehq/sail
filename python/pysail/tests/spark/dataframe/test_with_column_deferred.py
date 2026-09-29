"""Pre-existing gaps measured on Spark 4.2, main, HEAD and the staged PR."""

import re

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.fixture(params=["true", "false"], ids=["ansi", "non-ansi"])
def explicit_column_settings(spark, request):
    settings = {
        "spark.sql.ansi.enabled": request.param,
        "spark.sql.caseSensitive": "false",
        "spark.sql.analyzer.strictDataFrameColumnResolution": "true",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
            assert spark.conf.get(key) == value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="Non-strict column resolution requires Spark 4.2")
def test_with_column_non_strict_shadowed_reference(spark, explicit_column_settings):  # noqa: ARG001
    spark.conf.set("spark.sql.analyzer.strictDataFrameColumnResolution", "false")
    original = spark.sql("SELECT 123 AS c")
    result = original.withColumn("c", F.col("c").cast("string")).select(original.c)

    assert result._show_string(truncate=False) == "+---+\n|c  |\n+---+\n|123|\n+---+\n"  # noqa: SLF001
    assert result.schema.simpleString() == "struct<c:string>"


@pytest.mark.skipif(pyspark_version() < (4, 0), reason="DataFrame scalar subqueries require Spark 4")
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail cannot decorrelate outer references in a scalar-subquery projection",
    strict=True,
)
def test_with_column_correlated_scalar_projection(spark, explicit_column_settings):  # noqa: ARG001
    original = spark.sql("SELECT 1 AS c1, 2 AS c2")
    scalar = spark.range(1).select(F.col("c1").outer() + F.col("c2").outer()).scalar()
    result = original.withColumn("scalar", scalar)

    assert result._show_string(truncate=False) == (  # noqa: SLF001
        "+---+---+------+\n|c1 |c2 |scalar|\n+---+---+------+\n|1  |2  |3     |\n+---+---+------+\n"
    )


@pytest.mark.skipif(pyspark_version() < (4, 0), reason="DataFrame scalar subqueries require Spark 4")
def test_a_plan_id_column_that_reaches_a_correlated_subquery_is_an_outer_reference(
    spark,
    explicit_column_settings,  # noqa: ARG001
):
    # The plan ID of `outer.a` belongs to the surrounding query, not to the scalar subquery. When
    # the subquery cannot resolve it locally, Spark keeps the attribute unresolved and resolves it
    # as an outer reference instead of treating the DataFrame column as unrelated.
    outer = spark.range(3).withColumnRenamed("id", "a")
    inner = spark.range(3).withColumnRenamed("id", "b")
    count = inner.filter(inner.b == outer.a).select(F.count("*")).scalar()
    result = outer.select(outer.a, count.alias("c"))

    assert sorted(tuple(row) for row in result.collect()) == [(0, 1), (1, 1), (2, 1)]


# A name in a `GROUP BY` is matched against the aliases of the projection by the same comparison
# the rest of this PR rewrites, and only once the name has failed against the columns of the
# input: a real column of that name wins, and the alias is the fallback. At that point the
# aggregate has no output yet, so there is nothing to be ambiguous about and Spark keeps the FIRST
# alias that matches (`ResolveReferencesInAggregate.resolveGroupByAlias` uses `find`), leaving a
# later rule to report the query on its own terms. The two queries Spark answers are answered
# here too; what remains deferred is the condition it reports afterwards, which Sail does not
# have at all -- and none of these fails differently than it does on `main`.

_REPEATED_ALIAS = "FROM VALUES (1, 2), (1, 3) AS t(a, b)"


def test_a_repeated_alias_in_a_group_by(spark):
    """Spark groups by the first `x`, so the second one only has to be an aggregate."""
    result = spark.sql(f"SELECT a AS x, count(b) AS x {_REPEATED_ALIAS} GROUP BY x")

    assert [tuple(row) for row in result.collect()] == [(1, 2)]
    assert result.schema.simpleString() == "struct<x:int,x:bigint>"


def test_a_repeated_alias_of_one_column_in_a_group_by(spark):
    """Both aliases read `a`, so grouping by the first one leaves the second aggregated too."""
    result = spark.sql(f"SELECT a AS x, a AS x {_REPEATED_ALIAS} GROUP BY x")

    assert [tuple(row) for row in result.collect()] == [(1, 1)]
    assert result.schema.simpleString() == "struct<x:int,x:int>"


def test_a_group_by_name_takes_the_column_over_the_alias(spark):
    """`a` in the grouping is the input column, not the alias `b AS a` of the projection."""
    result = spark.sql(f"SELECT a, count(b) AS other {_REPEATED_ALIAS} GROUP BY a")

    assert [tuple(row) for row in result.collect()] == [(1, 2)]


def test_a_repeated_alias_in_a_having(spark):
    """A `HAVING` reads the output of a built aggregate, where a repeated name IS ambiguous."""
    with pytest.raises(
        Exception,
        match=re.escape("[AMBIGUOUS_REFERENCE] Reference `x` is ambiguous, could be: [`x`, `x`]."),
    ):
        spark.sql(f"SELECT a AS x, b AS x {_REPEATED_ALIAS} GROUP BY a, b HAVING x = 1").collect()


# Sail has no equivalent of Spark's missing-aggregation check, so a column the grouping leaves out
# fails when the projection is built and the message names an internal field id. This is not about
# aliases: the plain `SELECT a, b ... GROUP BY a` below fails the same way, on `main` as well.
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail has no MISSING_AGGREGATION check and names an internal field id instead",
    strict=True,
)
@pytest.mark.parametrize(
    "projection",
    [
        pytest.param("a, b", id="plain"),
        pytest.param("a AS x, b AS x", id="repeated-alias"),
        pytest.param("b AS a, a AS other", id="alias-of-the-grouping-name"),
    ],
)
def test_a_column_outside_the_grouping(spark, projection):
    """Grouping by `a` leaves `b` unaggregated, which is the condition Spark reports."""
    with pytest.raises(Exception, match=re.escape("[MISSING_AGGREGATION]")):
        spark.sql(f"SELECT {projection} {_REPEATED_ALIAS} GROUP BY a").collect()


def test_a_repeated_alias_in_an_order_by(spark):
    """A sort resolves the name against the projection, where neither alias wins outright."""
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        spark.sql(f"SELECT a AS x, b AS x {_REPEATED_ALIAS} ORDER BY x").collect()
