import pytest


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
