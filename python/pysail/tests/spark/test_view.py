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

    def test_join_with_qualified_columns(self, spark):
        spark.sql("CREATE VIEW IF NOT EXISTS active_customers AS SELECT * FROM customers WHERE id = 1")
        spark.sql("CREATE TABLE orders (order_id INT, customer_id INT)")
        spark.sql("INSERT INTO orders VALUES (100, 1), (101, 2)")
        actual = spark.sql(
            "SELECT active_customers.name, orders.order_id "
            "FROM active_customers "
            "JOIN orders ON active_customers.id = orders.customer_id "
            "ORDER BY orders.order_id"
        ).toPandas()
        expected = pd.DataFrame(
            {"name": ["Alice"], "order_id": [100]},
        ).astype({"name": "str", "order_id": "int32"})
        assert_frame_equal(actual, expected)
        spark.sql("DROP TABLE orders")

    def test_view_with_column_aliases(self, spark):
        spark.sql("CREATE VIEW aliased_customers (customer_id, customer_name) AS SELECT id, name FROM customers")
        actual = spark.sql("SELECT customer_id, customer_name FROM aliased_customers ORDER BY customer_id").toPandas()
        expected = pd.DataFrame(
            {"customer_id": [1, 2, 3], "customer_name": ["Alice", "Bob", "Carol"]},
        ).astype({"customer_id": "int32", "customer_name": "str"})
        assert_frame_equal(actual, expected)
        spark.sql("DROP VIEW aliased_customers")
