import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.tests.spark.utils import is_jvm_spark

# TODO: We should revisit all DML tests once we support lakehouse formats.
#   The tests involving update and delete operations are not supposed to work
#   Parquet files and the Hive metastore.

if is_jvm_spark():
    # Running the DML tests for JVM Spark requires the `spark.sql.catalogImplementation=hive`
    # configuration and a clean warehouse directory.
    pytest.skip("requires extra setup for JVM spark", allow_module_level=True)
else:
    # The data written to the warehouse directory is not cleaned up after the tests,
    # we should fix this.
    pytest.skip("missing clean-up logic", allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def person_table(spark):
    spark.sql("CREATE TABLE person (id INT, name STRING DEFAULT 'Sail', age INT)")
    yield
    spark.sql("DROP TABLE person")


def test_insert_single_value(spark):
    spark.sql("INSERT INTO person VALUES (101, 'Shehab', 99)")
    actual = spark.sql("SELECT * FROM person WHERE id = 101").toPandas()
    expected = pd.DataFrame(
        {"id": [101], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_multiple_values(spark):
    spark.sql("INSERT INTO person VALUES (201, 'Shehab', 99), (202, 'Alice', 2), (203, 'Bob', 9000)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (201, 202, 203)").toPandas()
    expected = pd.DataFrame(
        {"id": [201, 202, 203], "name": ["Shehab", "Alice", "Bob"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_using_column_list(spark):
    spark.sql("INSERT INTO person (id, name, age) VALUES (301, 'Shehab', 99), (302, 'Alice', 2), (303, 'Bob', 9000)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (301, 302, 303)").toPandas()
    expected = pd.DataFrame(
        {"id": [301, 302, 303], "name": ["Shehab", "Alice", "Bob"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)

    spark.sql("INSERT INTO person (name, age, id) VALUES ('Shehab', 99, 321), ('Alice', 2, 322), ('Bob', 9000, 323)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (321, 322, 323)").toPandas()
    expected = pd.DataFrame(
        {"id": [321, 322, 323], "name": ["Shehab", "Alice", "Bob"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_using_implicit_default(spark):
    spark.sql("INSERT INTO person (id, age) VALUES (401, 99), (402, 2), (403, 9000)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (401, 402, 403)").toPandas()
    expected = pd.DataFrame(
        {"id": [401, 402, 403], "name": ["Sail", "Sail", "Sail"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)

    spark.sql("INSERT INTO person (age, id) VALUES (100, 404), (3, 405), (9001, 406)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (404, 405, 406)").toPandas()
    expected = pd.DataFrame(
        {"id": [404, 405, 406], "name": ["Sail", "Sail", "Sail"], "age": [100, 3, 9001]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_with_full_table_name(spark):
    spark.sql("INSERT INTO spark_catalog.default.person VALUES (501, 'Shehab', 99)")
    actual = spark.sql("SELECT * FROM person WHERE id = 501").toPandas()
    expected = pd.DataFrame(
        {"id": [501], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="DELETE is not implemented")
def test_delete_entire_table(spark):
    # FIXME: clean up table after test
    spark.sql("CREATE TABLE meow (id INT, name STRING DEFAULT 'Sail', age INT)")
    spark.sql("INSERT INTO meow VALUES (601, 'Shehab', 99), (602, 'Alice', 2), (603, 'Bob', 9000)")
    spark.sql("DELETE FROM meow")
    actual = spark.sql("SELECT * FROM meow").toPandas()
    expected = pd.DataFrame(columns=["id", "name", "age"]).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="DELETE is not implemented")
def test_delete_with_filter(spark):
    # TODO: Add test for DELETE with table alias and filter
    spark.sql("INSERT INTO person VALUES (701, 'Shehab', 99)")
    spark.sql("DELETE FROM person WHERE id = 701")
    actual = spark.sql("SELECT * FROM person WHERE id = 701").toPandas()
    expected = pd.DataFrame(columns=["id", "name", "age"]).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_single_value(spark):
    spark.sql("INSERT INTO person VALUES (801, 'Shehab', 99)")
    spark.sql("UPDATE person SET id = 802, age = 100 WHERE id = 801")
    actual = spark.sql("SELECT * FROM person WHERE id = 802").toPandas()
    expected = pd.DataFrame(
        {"id": [802], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_multiple_values(spark):
    spark.sql("INSERT INTO person VALUES (901, 'Shehab', 99), (902, 'Alice', 2), (903, 'Bob', 9000)")
    spark.sql("UPDATE person SET id = id + 1, age = 100 WHERE id IN (901, 902, 903)")
    actual = spark.sql("SELECT * FROM person WHERE id IN (902, 903, 904)").toPandas()
    expected = pd.DataFrame(
        {"id": [902, 903, 904], "name": ["Shehab", "Alice", "Bob"], "age": [100, 100, 100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_with_full_table_name(spark):
    spark.sql("INSERT INTO person VALUES (1001, 'Shehab', 99)")
    spark.sql("UPDATE spark_catalog.default.person SET id = 1002, age = 100 WHERE id = 1001")
    actual = spark.sql("SELECT * FROM person WHERE id = 1002").toPandas()
    expected = pd.DataFrame(
        {"id": [1002], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_with_alias(spark):
    spark.sql("INSERT INTO person VALUES (1101, 'Shehab', 99)")
    spark.sql("UPDATE person AS p SET p.id = 1102, p.age = 100 WHERE p.id = 1101")
    actual = spark.sql("SELECT * FROM person WHERE id = 1102").toPandas()
    expected = pd.DataFrame(
        {"id": [1102], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_with_full_table_name_and_alias(spark):
    spark.sql("INSERT INTO person VALUES (1201, 'Shehab', 99)")
    spark.sql("UPDATE spark_catalog.default.person AS p SET p.id = 1202, p.age = 100 WHERE p.id = 1201")
    actual = spark.sql("SELECT * FROM person WHERE id = 1202").toPandas()
    expected = pd.DataFrame(
        {"id": [1202], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE is not implemented")
def test_update_using_column_value(spark):
    spark.sql("INSERT INTO person VALUES (1301, 'Shehab', 99)")
    spark.sql("UPDATE person SET id = id + 1, age = 100 WHERE id = 1301")
    actual = spark.sql("SELECT * FROM person WHERE id = 1302").toPandas()
    expected = pd.DataFrame(
        {"id": [1302], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)
