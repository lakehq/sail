import pytest


def test_default_can_be_column_name(spark):
    assert spark.sql("SELECT DEFAULT FROM VALUES (1) AS t(DEFAULT)").collect() == [(1,)]


def test_sql_positional_parameters(spark):
    assert spark.sql("SELECT * FROM range(10) WHERE id > ?", args=[7]).collect() == [(8,), (9,)]
    assert spark.sql("SELECT ? AS v FROM range(10) WHERE id > ? ORDER BY id", args=[1, 7]).collect() == [
        (1,),
        (1,),
    ]
    assert spark.sql("SELECT ? AS v", args=[1, 2]).collect() == [(1,)]


def test_sql_timestamp_string_parameters(spark):
    timestamp = "2024-05-01 12:00:00.123456789"
    result = spark.sql(
        """
        SELECT
          TIMESTAMP '2024-05-01 12:00:00.123456' = ? AS comparison,
          TIMESTAMP '2024-05-01 12:00:00.123456' IN (?) AS in_list,
          TIMESTAMP '2024-05-01 12:00:00.123456'
            BETWEEN ? AND ? AS bounded,
          TIMESTAMP '2024-05-01 12:00:00.123456'
            IS NOT DISTINCT FROM ? AS distinctness
        """,
        args=[timestamp] * 5,
    ).collect()
    assert result == [(True, True, True, True)]

    assert spark.sql(
        """
        SELECT TIMESTAMP '2024-05-01 12:00:00.123456' = :candidate
        """,
        args={"candidate": timestamp},
    ).collect() == [(True,)]


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
