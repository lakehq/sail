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


# Spark binds a parameter before analysis, so `round` implicitly casts a STRING parameter to DOUBLE.
# Sail resolves a parameter marker as an untyped placeholder and binds its value after planning,
# so `round` never sees the STRING type and fails to plan.
@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_round_of_string_parameter(spark):
    assert spark.sql("SELECT round(:p, 1) AS r", args={"p": "1.25"}).collect() == [(1.3,)]
    assert spark.sql("SELECT round(?, 1) AS r", args=["1.25"]).collect() == [(1.3,)]


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
