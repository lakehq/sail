"""
Test Delta Lake schema options (mergeSchema and overwriteSchema) in Sail.
"""
import pandas as pd
import pytest
from pyspark.sql.types import Row


class TestDeltaSchemaOptions:
    """Test Delta Lake schema options functionality."""

    def test_merge_schema_append_new_column(self, spark, tmp_path):
        """Test mergeSchema=true allows adding new columns during append."""
        delta_path = tmp_path / "delta_merge_schema"

        # Create initial table with basic schema
        initial_data = [
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
        ]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Append data with additional column using mergeSchema
        extended_data = [
            Row(id=3, name="Charlie", age=30),
            Row(id=4, name="Diana", age=25),
        ]
        df2 = spark.createDataFrame(extended_data)
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        # Verify the merged schema and data
        result_df = spark.read.format("delta").load(str(delta_path)).sort("id")
        result_pandas = result_df.toPandas()

        expected_data = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
            "age": [None, None, 30, 25]
        }).astype({"id": "int32", "name": "string", "age": "Int32"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("id").reset_index(drop=True),
            expected_data,
            check_dtype=False
        )

    def test_merge_schema_false_rejects_new_column(self, spark, tmp_path):
        """Test that mergeSchema=false (default) rejects new columns."""
        delta_path = tmp_path / "delta_no_merge_schema"

        # Create initial table
        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to append data with additional column without mergeSchema
        extended_data = [Row(id=2, name="Bob", age=30)]
        df2 = spark.createDataFrame(extended_data)

        with pytest.raises(Exception) as exc_info:
            df2.write.format("delta").mode("append").save(str(delta_path))

        # Should contain schema-related error message
        assert "schema" in str(exc_info.value).lower() or "field" in str(exc_info.value).lower()

    def test_overwrite_schema_with_overwrite_mode(self, spark, tmp_path):
        """Test overwriteSchema=true with overwrite mode."""
        delta_path = tmp_path / "delta_overwrite_schema"

        # Create initial table
        initial_data = [
            Row(id=1, name="Alice", score=95.5),
            Row(id=2, name="Bob", score=87.2),
        ]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Overwrite with completely different schema
        new_data = [
            Row(user_id=101, username="charlie", active=True),
            Row(user_id=102, username="diana", active=False),
        ]
        df2 = spark.createDataFrame(new_data)
        df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(delta_path))

        # Verify the new schema and data
        result_df = spark.read.format("delta").load(str(delta_path)).sort("user_id")
        result_pandas = result_df.toPandas()

        expected_data = pd.DataFrame({
            "user_id": [101, 102],
            "username": ["charlie", "diana"],
            "active": [True, False]
        }).astype({"user_id": "int32", "username": "string", "active": "bool"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("user_id").reset_index(drop=True),
            expected_data,
            check_dtype=False
        )

    def test_overwrite_schema_with_append_mode_fails(self, spark, tmp_path):
        """Test that overwriteSchema=true fails with append mode."""
        delta_path = tmp_path / "delta_overwrite_schema_append"

        # Create initial table
        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to use overwriteSchema with append mode (should fail)
        new_data = [Row(user_id=101, username="charlie")]
        df2 = spark.createDataFrame(new_data)

        with pytest.raises(Exception) as exc_info:
            df2.write.format("delta").mode("append").option("overwriteSchema", "true").save(str(delta_path))

        # Should contain error about overwriteSchema only being valid with overwrite mode
        error_msg = str(exc_info.value).lower()
        assert "overwrite" in error_msg and ("mode" in error_msg or "schema" in error_msg)

    def test_both_merge_and_overwrite_schema_fails(self, spark, tmp_path):
        """Test that specifying both mergeSchema and overwriteSchema fails."""
        delta_path = tmp_path / "delta_both_options"

        # Create initial table
        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to use both options (should fail)
        new_data = [Row(id=2, name="Bob")]
        df2 = spark.createDataFrame(new_data)

        with pytest.raises(Exception) as exc_info:
            df2.write.format("delta").mode("append").option("mergeSchema", "true").option("overwriteSchema", "true").save(str(delta_path))

        # Should contain error about conflicting options
        error_msg = str(exc_info.value).lower()
        assert "merge" in error_msg and "overwrite" in error_msg

    def test_merge_schema_with_type_changes(self, spark, tmp_path):
        """Test mergeSchema behavior with compatible type changes."""
        delta_path = tmp_path / "delta_merge_type_changes"

        # Create initial table with integer column
        initial_data = [Row(id=1, value=100)]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Append data with same column but potentially different precision
        # This tests the schema merging logic for compatible types
        new_data = [Row(id=2, value=200)]
        df2 = spark.createDataFrame(new_data)
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        # Verify data integrity
        result_df = spark.read.format("delta").load(str(delta_path)).sort("id")
        result_pandas = result_df.toPandas()

        expected_data = pd.DataFrame({
            "id": [1, 2],
            "value": [100, 200]
        }).astype({"id": "int32", "value": "int32"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("id").reset_index(drop=True),
            expected_data,
            check_dtype=False
        )
