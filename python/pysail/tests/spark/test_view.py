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


class TestPersistentViewCatalog:
    """Tests for persistent view catalog visibility (SHOW TABLE EXTENDED, DESCRIBE EXTENDED)."""

    @pytest.fixture(autouse=True)
    def catalog_view(self, spark):
        spark.sql("CREATE VIEW IF NOT EXISTS all_customers AS SELECT * FROM customers")
        yield
        spark.sql("DROP VIEW IF EXISTS all_customers")

    def test_show_table_extended_includes_view(self, spark):
        rows = spark.sql("SHOW TABLE EXTENDED LIKE '*'").collect()
        names = [row.tableName for row in rows]
        assert "customers" in names
        assert "all_customers" in names

    def test_show_table_extended_view_type(self, spark):
        rows = spark.sql("SHOW TABLE EXTENDED LIKE 'all_customers'").collect()
        view_row = next(row for row in rows if row.tableName == "all_customers")
        assert "Type: VIEW" in view_row.information

    def test_show_table_extended_view_schema(self, spark):
        rows = spark.sql("SHOW TABLE EXTENDED LIKE 'all_customers'").collect()
        view_row = next(row for row in rows if row.tableName == "all_customers")
        assert "Schema: root" in view_row.information
        assert " |-- id: int (nullable = " in view_row.information
        assert " |-- name: string (nullable = " in view_row.information

    def test_describe_extended_view_columns(self, spark):
        rows = spark.sql("DESCRIBE EXTENDED all_customers").collect()
        col_rows = {row.col_name: row.data_type for row in rows if not row.col_name.startswith("#") and row.col_name}
        assert "id" in col_rows
        assert "name" in col_rows

    def test_describe_extended_view_metadata(self, spark):
        rows = spark.sql("DESCRIBE EXTENDED all_customers").collect()
        metadata_marker = next(
            (row for row in rows if row.col_name == "# Detailed Table Information"),
            None,
        )
        assert metadata_marker is not None
        type_row = next(
            (row for row in rows if row.col_name == "Type"),
            None,
        )
        assert type_row is not None
        assert type_row.data_type == "VIEW"
