import random

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module", autouse=True)
def person_table(spark):
    spark.sql("CREATE TABLE person (id INT, name STRING, age INT)")
    yield
    spark.sql("DROP TABLE person")


def test_insert_single_value(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_multiple_values(spark):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Shehab", "Heran", "Chen"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_with_full_table_name(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO spark_catalog.default.person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_single_value(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    p_id1 = p_id + 1
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    spark.sql(f"UPDATE person SET id = {p_id1}, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_multiple_values(spark):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)")  # noqa: S608
    spark.sql(f"UPDATE person SET id = {p_id1+1}, age = 100 WHERE id IN ({p_id1}, {p_id2}, {p_id3})")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id IN ({p_id1+1}, {p_id2+1}, {p_id3+1})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1 + 1, p_id1 + 1, p_id1 + 1], "name": ["Shehab", "Heran", "Chen"], "age": [100, 100, 100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_with_full_table_name(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    spark.sql(f"UPDATE spark_catalog.default.person SET id = {p_id+1}, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_with_alias(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    spark.sql(f"UPDATE person AS p SET p.id = {p_id+1}, p.age = 100 WHERE p.id = {p_id}")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_with_full_table_name_and_alias(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    spark.sql(f"UPDATE spark_catalog.default.person AS p SET p.id = {p_id+1}, p.age = 100 WHERE p.id = {p_id}")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_update_using_column_name(spark):
    p_id = random.randint(0, 1000000)  # noqa: S311
    p_id1 = p_id + 1
    spark.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    spark.sql(f"UPDATE person SET id = id+1, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = spark.sql(f"SELECT * FROM person WHERE id = {p_id1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)
