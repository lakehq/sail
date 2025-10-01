import pytest
from pyspark.sql.types import BooleanType, IntegerType, Row, StringType, StructField, StructType

from pysail.tests.spark.utils import escape_sql_string_literal


class TestDeltaDelete:
    """Comprehensive Delta Lake DELETE operations tests"""

    @pytest.fixture(scope="class")
    def delta_delete_test_data(self):
        """Test data for DELETE operations"""
        return [
            Row(id=1, name="Alice", age=25, department="Engineering", salary=75000, active=True),
            Row(id=2, name="Bob", age=30, department="Marketing", salary=65000, active=True),
            Row(id=3, name="Charlie", age=35, department="Engineering", salary=85000, active=False),
            Row(id=4, name="Diana", age=28, department="Sales", salary=55000, active=True),
            Row(id=5, name="Eve", age=32, department="Marketing", salary=70000, active=True),
            Row(id=6, name="Frank", age=40, department="Engineering", salary=95000, active=False),
            Row(id=7, name="Grace", age=27, department="Sales", salary=50000, active=True),
            Row(id=8, name="Henry", age=33, department="HR", salary=60000, active=True),
        ]

    @pytest.fixture(scope="class")
    def partitioned_test_data(self):
        """Test data for partitioned table DELETE operations"""
        return [
            Row(id=1, name="Alice", year=2023, month=1, value=100),
            Row(id=2, name="Bob", year=2023, month=1, value=200),
            Row(id=3, name="Charlie", year=2023, month=2, value=300),
            Row(id=4, name="Diana", year=2023, month=2, value=400),
            Row(id=5, name="Eve", year=2024, month=1, value=500),
            Row(id=6, name="Frank", year=2024, month=1, value=600),
            Row(id=7, name="Grace", year=2024, month=2, value=700),
            Row(id=8, name="Henry", year=2024, month=2, value=800),
        ]

    def test_delta_delete_basic_condition(self, spark, delta_delete_test_data, tmp_path):
        """Test basic DELETE with WHERE condition"""
        delta_path = tmp_path / "delta_delete_basic"
        table_name = "delta_delete_basic_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(delta_delete_test_data, schema)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Verify initial count
            initial_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()  # noqa: S608
            initial_count = initial_result[0]["count"]
            assert initial_count == 8, f"Expected 8 rows initially, got {initial_count}"  # noqa: PLR2004

            # Delete rows where department = 'Engineering'
            spark.sql(f"DELETE FROM {table_name} WHERE department = 'Engineering'")  # noqa: S608

            # Verify results
            result_df = spark.sql(f"SELECT * FROM {table_name}")  # noqa: S608
            remaining_count = result_df.count()
            assert remaining_count == 5, f"Expected 5 rows after deletion, got {remaining_count}"  # noqa: PLR2004

            remaining_departments = [row.department for row in result_df.collect()]
            assert "Engineering" not in remaining_departments, "Engineering department should be completely removed"
            assert set(remaining_departments) == {
                "Marketing",
                "Sales",
                "HR",
            }, "Only Marketing, Sales, and HR should remain"

            remaining_names = sorted([row.name for row in result_df.collect()])
            expected_names = ["Bob", "Diana", "Eve", "Grace", "Henry"]
            assert remaining_names == expected_names, f"Expected {expected_names}, got {remaining_names}"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_multiple_conditions(self, spark, delta_delete_test_data, tmp_path):
        """Test DELETE with multiple AND/OR conditions"""
        delta_path = tmp_path / "delta_delete_multiple"
        table_name = "delta_delete_multiple_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(delta_delete_test_data, schema)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete rows where (department = 'Marketing' AND age > 30) OR (department = 'Sales' AND salary < 60000)
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE (department = 'Marketing' AND age > 30)
                   OR (department = 'Sales' AND salary < 60000)
            """)  # noqa: S608

            result_df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")  # noqa: S608
            remaining_rows = result_df.collect()

            # Should remove: Eve (Marketing, age 32), Diana and Grace (Sales, salary < 60000)
            # Should keep: Alice, Bob, Charlie, Frank, Henry
            expected_ids = [1, 2, 3, 6, 8]
            actual_ids = [row.id for row in remaining_rows]
            assert actual_ids == expected_ids, f"Expected IDs {expected_ids}, got {actual_ids}"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_no_matches(self, spark, delta_delete_test_data, tmp_path):
        """Test DELETE with condition that matches no rows"""
        delta_path = tmp_path / "delta_delete_no_matches"
        table_name = "delta_delete_no_matches_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(delta_delete_test_data, schema)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            initial_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()  # noqa: S608
            initial_count = initial_result[0]["count"]

            # Delete with condition that matches no rows
            spark.sql(f"DELETE FROM {table_name} WHERE age > 100")  # noqa: S608

            final_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()  # noqa: S608
            final_count = final_result[0]["count"]

            assert final_count == initial_count, f"Count should remain {initial_count}, got {final_count}"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_all_rows(self, spark, delta_delete_test_data, tmp_path):
        """Test DELETE that removes all rows"""
        delta_path = tmp_path / "delta_delete_all"
        table_name = "delta_delete_all_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(delta_delete_test_data, schema)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete all rows
            spark.sql(f"DELETE FROM {table_name} WHERE age > 0")  # noqa: S608

            final_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()  # noqa: S608
            final_count = final_result[0]["count"]
            assert final_count == 0, f"Table should be empty, got {final_count} rows"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_partitioned_table(self, spark, partitioned_test_data, tmp_path):
        """Test DELETE operations on partitioned Delta tables"""
        delta_path = tmp_path / "delta_delete_partitioned"
        table_name = "delta_delete_partitioned_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        df = spark.createDataFrame(partitioned_test_data, schema)
        df.write.format("delta").partitionBy("year", "month").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete all data from 2023
            spark.sql(f"DELETE FROM {table_name} WHERE year = 2023")  # noqa: S608

            result_df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")  # noqa: S608
            remaining_rows = result_df.collect()

            # Should only have 2024 data
            years = [row.year for row in remaining_rows]
            assert all(year == 2024 for year in years), f"All remaining years should be 2024, got {set(years)}"  # noqa: PLR2004

            # Should have 4 rows (all 2024 data)
            assert len(remaining_rows) == 4, f"Expected 4 rows from 2024, got {len(remaining_rows)}"  # noqa: PLR2004

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_with_complex_condition(self, spark, delta_delete_test_data, tmp_path):
        """Test DELETE with complex conditions (simplified version without subquery)"""
        delta_path = tmp_path / "delta_delete_complex"
        table_name = "delta_delete_complex_table"

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("department", StringType(), True),
                StructField("salary", IntegerType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        df = spark.createDataFrame(delta_delete_test_data, schema)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete employees with salary above average (manually calculated: 69375)
            # Should remove: Alice (75000), Charlie (85000), Eve (70000), Frank (95000)
            spark.sql(f"DELETE FROM {table_name} WHERE salary > 69375")  # noqa: S608

            result_df = spark.sql(f"SELECT * FROM {table_name} ORDER BY salary")  # noqa: S608
            remaining_rows = result_df.collect()

            # Should keep: Bob (65000), Diana (55000), Grace (50000), Henry (60000)
            expected_names = ["Bob", "Diana", "Grace", "Henry"]
            remaining_names = [row.name for row in remaining_rows]
            assert sorted(remaining_names) == sorted(
                expected_names
            ), f"Expected {expected_names}, got {remaining_names}"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_case_sensitivity(self, spark, tmp_path):
        """Test DELETE with case-sensitive string comparisons"""
        delta_path = tmp_path / "delta_delete_case"
        table_name = "delta_delete_case_table"

        # Setup table with mixed case data
        test_data = [
            Row(id=1, name="Alice", department="Engineering"),
            Row(id=2, name="alice", department="engineering"),
            Row(id=3, name="ALICE", department="ENGINEERING"),
            Row(id=4, name="Bob", department="Marketing"),
        ]

        df = spark.createDataFrame(test_data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete exact case match
            spark.sql(f"DELETE FROM {table_name} WHERE name = 'Alice'")  # noqa: S608

            result_df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")  # noqa: S608
            remaining_rows = result_df.collect()

            # Should only remove exact match "Alice", keep "alice" and "ALICE"
            remaining_names = [row.name for row in remaining_rows]
            assert "Alice" not in remaining_names, "Exact match 'Alice' should be removed"
            assert "alice" in remaining_names, "Different case 'alice' should remain"
            assert "ALICE" in remaining_names, "Different case 'ALICE' should remain"
            assert "Bob" in remaining_names, "Bob should remain"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_delete_with_null_values(self, spark, tmp_path):
        """Test DELETE operations with NULL values"""
        delta_path = tmp_path / "delta_delete_null"
        table_name = "delta_delete_null_table"

        # Setup table with NULL values
        test_data = [
            Row(id=1, name="Alice", department="Engineering"),
            Row(id=2, name="Bob", department=None),
            Row(id=3, name=None, department="Marketing"),
            Row(id=4, name="Diana", department="Sales"),
            Row(id=5, name=None, department=None),
        ]

        df = spark.createDataFrame(test_data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'")

        try:
            # Delete rows where department IS NULL
            spark.sql(f"DELETE FROM {table_name} WHERE department IS NULL")  # noqa: S608

            result_df = spark.sql(f"SELECT * FROM {table_name} ORDER BY id")  # noqa: S608
            remaining_rows = result_df.collect()

            # Should remove Bob and the row with both nulls, keep Alice, marketing person, and Diana
            remaining_ids = [row.id for row in remaining_rows]
            expected_ids = [1, 3, 4]  # Alice, person with null name but marketing dept, Diana
            assert remaining_ids == expected_ids, f"Expected IDs {expected_ids}, got {remaining_ids}"

            # Verify no remaining rows have NULL department
            departments = [row.department for row in remaining_rows]
            assert None not in departments, f"No NULL departments should remain, got {departments}"

        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
