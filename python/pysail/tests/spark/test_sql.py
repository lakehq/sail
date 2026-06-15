import pytest


def test_default_can_be_column_name(spark):
    assert spark.sql("SELECT DEFAULT FROM VALUES (1) AS t(DEFAULT)").collect() == [(1,)]


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
