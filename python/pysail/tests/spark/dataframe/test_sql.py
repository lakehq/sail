import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


def test_default_can_be_column_name(spark):
    assert spark.sql("SELECT DEFAULT FROM VALUES (1) AS t(DEFAULT)").collect() == [(1,)]


def test_sql_positional_parameters(spark):
    assert spark.sql("SELECT * FROM range(10) WHERE id > ?", args=[7]).collect() == [(8,), (9,)]
    assert spark.sql("SELECT ? AS v FROM range(10) WHERE id > ? ORDER BY id", args=[1, 7]).collect() == [
        (1,),
        (1,),
    ]
    assert spark.sql("SELECT ? AS v", args=[1, 2]).collect() == [(1,)]


@pytest.mark.parametrize(("marker", "named"), [("?", False), (":value", True)], ids=["positional", "named"])
@pytest.mark.parametrize(
    ("query", "value", "expected"),
    [
        pytest.param(
            "SELECT CASE WHEN id = 0 THEN {marker} WHEN id = 1 THEN 16777217 ELSE CAST(2 AS FLOAT) END AS v "
            "FROM range(3) ORDER BY id",
            1.25,
            [(1.25,), (16777217.0,), (2.0,)],
            id="case",
        ),
        pytest.param(
            "SELECT CASE WHEN id = 0 THEN v ELSE CAST(2 AS FLOAT) END AS v "
            "FROM (SELECT id, {marker} + 1 AS v FROM range(2)) AS q ORDER BY id",
            16777216.0,
            [(16777217.0,), (2.0,)],
            id="case-through-projection",
        ),
        pytest.param(
            "SELECT IF(id = 0, {marker} + 1, CAST(2 AS FLOAT)) AS v FROM range(2) ORDER BY id",
            16777216.0,
            [(16777217.0,), (2.0,)],
            id="if",
        ),
        pytest.param(
            "SELECT nvl2(id, {marker} + 1, CAST(2 AS FLOAT)) AS v FROM range(2) ORDER BY id",
            16777216.0,
            [(16777217.0,), (16777217.0,)],
            id="nvl2",
        ),
    ],
)
def test_sql_conditional_preserves_parameter_branch_precision(spark, marker, named, query, value, expected):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        args = {"value": value} if named else [value]
        df = spark.sql(query.format(marker=marker), args=args)
        assert df.dtypes == [("v", "double")]
        assert df.collect() == expected
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize(("marker", "named"), [("?", False), (":value", True)], ids=["positional", "named"])
@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize(
    ("query", "value"),
    [
        pytest.param(
            "SELECT nvl2(id, 1, CAST(1.5 AS DOUBLE)) AS v "
            "FROM VALUES (0), (NULL) AS t(id) WHERE {marker} ORDER BY v",
            True,
            id="unrelated-parameter",
        ),
        pytest.param(
            "SELECT nvl2(id, 1, {marker}) AS v FROM VALUES (0), (NULL) AS t(id) ORDER BY v",
            1.5,
            id="result-parameter",
        ),
    ],
)
def test_sql_nvl2_preserves_widened_parameter_schema(spark, marker, named, ansi, query, value):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        args = {"value": value} if named else [value]
        df = spark.sql(query.format(marker=marker), args=args)
        assert df.dtypes == [("v", "double")]
        assert df.collect() == [(1.0,), (1.5,)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_sql_case_widens_parameter_marker_branch(spark):
    df = spark.sql("SELECT CASE WHEN id = 0 THEN ? ELSE CAST(2 AS BIGINT) END AS v FROM range(2) ORDER BY id", args=[1])
    assert df.dtypes == [("v", "bigint")]
    assert df.collect() == [(1,), (2,)]


def test_keyword_as_explicit_column_alias(spark):
    # Keywords are not reserved in Spark and can be used as column aliases
    # when the `AS` keyword is explicit.
    df = spark.sql("SELECT 1 AS end")
    assert df.columns == ["end"]
    assert df.collect() == [(1,)]
    assert spark.sql("SELECT 1 AS case, 2 AS when").collect() == [(1, 2)]
    # The implicit-alias ambiguity is still resolved in favor of the expression.
    assert spark.sql("SELECT CASE WHEN true THEN 1 END AS end").collect() == [(1,)]


def test_predicate_negation(spark):
    assert spark.sql("SELECT NOT '' LIKE '%'").collect() == [(False,)]
    assert spark.sql("SELECT NOT ('' LIKE '%')").collect() == [(False,)]
    assert spark.sql("SELECT '' NOT LIKE '%'").collect() == [(False,)]

    assert spark.sql("SELECT NOT 1 BETWEEN 1 AND 2").collect() == [(False,)]
    assert spark.sql("SELECT NOT (1 BETWEEN 1 AND 2)").collect() == [(False,)]
    assert spark.sql("SELECT 1 NOT BETWEEN 1 AND 2").collect() == [(False,)]

    assert spark.sql("SELECT NOT 'a' IS NULL").collect() == [(True,)]
    assert spark.sql("SELECT NOT ('a' IS NULL)").collect() == [(True,)]
    assert spark.sql("SELECT 'a' IS NOT NULL").collect() == [(True,)]

    assert spark.sql("SELECT NOT 1 IN (1, 2)").collect() == [(False,)]
    assert spark.sql("SELECT NOT (1 IN (1, 2))").collect() == [(False,)]
    assert spark.sql("SELECT 1 NOT IN (1, 2)").collect() == [(False,)]

    assert spark.sql("SELECT NOT NOT 1 IN (1, 2)").collect() == [(True,)]
    assert spark.sql("SELECT NOT 1 NOT IN (1, 2)").collect() == [(True,)]
    assert spark.sql("SELECT NOT (1 NOT IN (1, 2))").collect() == [(True,)]
    with pytest.raises(Exception, match="NOT"):
        assert spark.sql("SELECT 1 NOT NOT IN (1, 2)").collect() == [(True,)]
