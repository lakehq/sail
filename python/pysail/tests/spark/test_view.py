import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="module", autouse=True)
def base_table(spark):
    spark.sql("CREATE TABLE customers (id INT, name STRING)")
    spark.sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
    yield
    spark.sql("DROP VIEW IF EXISTS active_customers")
    spark.sql("DROP VIEW IF EXISTS all_customers")
    spark.sql("DROP TABLE customers")


class TestPersistentView:
    """Tests for CREATE VIEW and SELECT from persistent views."""

    def test_create_and_read_view(self, spark):
        spark.sql("CREATE VIEW active_customers AS SELECT * FROM customers WHERE id = 1")
        actual = spark.sql("SELECT * FROM active_customers ORDER BY id").toPandas()
        expected = pd.DataFrame(
            {"id": [1], "name": ["Alice"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)

    def test_read_view_all_rows(self, spark):
        spark.sql("CREATE VIEW all_customers AS SELECT * FROM customers")
        actual = spark.sql("SELECT * FROM all_customers ORDER BY id").toPandas()
        expected = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)


class TestTemporaryView:
    """Temporary views work — these serve as a baseline to confirm the test setup is valid."""

    def test_create_and_read_temp_view(self, spark):
        spark.sql("CREATE TEMPORARY VIEW recent_customers AS SELECT * FROM customers WHERE id = 2")
        actual = spark.sql("SELECT * FROM recent_customers ORDER BY id").toPandas()
        expected = pd.DataFrame(
            {"id": [2], "name": ["Bob"]},
        ).astype({"id": "int32", "name": "str"})
        assert_frame_equal(actual, expected)
