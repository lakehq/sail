import random

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module", autouse=True)
def person_table(sail):
    sail.sql("CREATE TABLE person (id INT, name STRING DEFAULT 'Sail', age INT)")
    yield
    sail.sql("DROP TABLE person")


def test_insert_single_value(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person where id = {p_id}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_multiple_values(sail):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Shehab", "Heran", "Chen"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_using_column_list(sail):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(
        f"INSERT INTO person (id, name, age) VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)"  # noqa: S608
    )
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Shehab", "Heran", "Chen"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)

    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(
        f"INSERT INTO person (name, age, id) VALUES ('Shehabz', 99, {p_id1}), ('Heranz', 2, {p_id2}), ('Chenz', 9000, {p_id3})"  # noqa: S608
    )
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Shehabz", "Heranz", "Chenz"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_using_implicit_default(sail):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person (id, age) VALUES ({p_id1}, 99), ({p_id2}, 2), ({p_id3}, 9000)")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Sail", "Sail", "Sail"], "age": [99, 2, 9000]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)

    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person (age, id) VALUES (100, {p_id1}), (3, {p_id2}), (9001, {p_id3})")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1}, {p_id2}, {p_id3})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1, p_id2, p_id3], "name": ["Sail", "Sail", "Sail"], "age": [100, 3, 9001]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


def test_insert_with_full_table_name(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO spark_catalog.default.person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="DELETE ExecutionPlan not implemented")
def test_delete_entire_table(sail):
    sail.sql("CREATE TABLE meow (id INT, name STRING DEFAULT 'Sail', age INT)")
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO meow VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)")  # noqa: S608
    sail.sql("DELETE FROM meow")
    actual = sail.sql("SELECT * FROM meow").toPandas()
    expected = pd.DataFrame(columns=["id", "name", "age"]).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="DELETE ExecutionPlan not implemented")
def test_delete_with_filter(sail):
    # TODO: Add test for DELETE with table alias and filter
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"DELETE FROM person WHERE id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id}").toPandas()  # noqa: S608
    expected = pd.DataFrame(columns=["id", "name", "age"]).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_single_value(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    p_id1 = p_id + 1
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"UPDATE person SET id = {p_id1}, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_multiple_values(sail):
    p_id1 = random.randint(0, 1000000)  # noqa: S311
    p_id2 = random.randint(0, 1000000)  # noqa: S311
    p_id3 = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id1}, 'Shehab', 99), ({p_id2}, 'Heran', 2), ({p_id3}, 'Chen', 9000)")  # noqa: S608
    sail.sql(f"UPDATE person SET id = {p_id1+1}, age = 100 WHERE id IN ({p_id1}, {p_id2}, {p_id3})")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id IN ({p_id1+1}, {p_id2+1}, {p_id3+1})").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1 + 1, p_id1 + 1, p_id1 + 1], "name": ["Shehab", "Heran", "Chen"], "age": [100, 100, 100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_with_full_table_name(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"UPDATE spark_catalog.default.person SET id = {p_id+1}, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_with_alias(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"UPDATE person AS p SET p.id = {p_id+1}, p.age = 100 WHERE p.id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_with_full_table_name_and_alias(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"UPDATE spark_catalog.default.person AS p SET p.id = {p_id+1}, p.age = 100 WHERE p.id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id+1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id + 1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="UPDATE ExecutionPlan not implemented")
def test_update_using_column_value(sail):
    p_id = random.randint(0, 1000000)  # noqa: S311
    p_id1 = p_id + 1
    sail.sql(f"INSERT INTO person VALUES ({p_id}, 'Shehab', 99)")  # noqa: S608
    sail.sql(f"UPDATE person SET id = id+1, age = 100 WHERE id = {p_id}")  # noqa: S608
    actual = sail.sql(f"SELECT * FROM person WHERE id = {p_id1}").toPandas()  # noqa: S608
    expected = pd.DataFrame(
        {"id": [p_id1], "name": ["Shehab"], "age": [100]},
    ).astype({"id": "int32", "name": "str", "age": "int32"})
    assert_frame_equal(actual, expected)
