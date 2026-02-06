import pytest

from pysail.tests.spark.utils import is_jvm_spark

if is_jvm_spark():
    pytest.skip("JVM spark does not support the rate format in SQL", allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def tables(spark):
    spark.sql("CREATE TABLE t1 USING rate")
    spark.sql("""
        CREATE TABLE t2
        USING rate
        OPTIONS (
            rowsPerSecond '500',
            numPartitions '1'
        )
    """)
    yield
    spark.sql("DROP TABLE t1")
    spark.sql("DROP TABLE t2")


def test_rate_basic(spark):
    assert spark.sql("SELECT * FROM t1 LIMIT 0").collect() == []


def test_rate_with_options(spark):
    actual = spark.sql("SELECT * FROM t2 OFFSET 0 LIMIT 2").collect()

    assert len(actual) == 2  # noqa: PLR2004
    assert actual[1]["timestamp"] >= actual[0]["timestamp"]
    assert actual[0]["value"] >= 0
    assert actual[1]["value"] >= 0


def test_rate_with_projection(spark):
    actual = spark.sql("SELECT value FROM t2 OFFSET 42 LIMIT 1").collect()
    assert actual == [(42,)]

    actual = spark.sql("SELECT value, value, timestamp FROM t2 LIMIT 1").collect()
    assert actual == [(0, 0, actual[0]["timestamp"])]


def test_rate_with_filtering(spark):
    actual = spark.sql("SELECT * FROM t2 WHERE value > 1 LIMIT 2").collect()

    assert len(actual) == 2  # noqa: PLR2004
    assert actual[0]["value"] > 1
    assert actual[1]["value"] > 1


def test_rate_union_all_with_non_streaming_source(spark):
    actual = spark.sql("(SELECT a.value FROM t2 AS a LIMIT 1) UNION ALL (SELECT * FROM VALUES (10), (20))").collect()
    assert sorted(actual) == [(0,), (10,), (20,)]
