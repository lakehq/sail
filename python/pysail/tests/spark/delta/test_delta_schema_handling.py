"""
Test Delta Lake schema handling (mergeSchema, overwriteSchema, evolution) in Sail.
"""

import pandas as pd
import pytest
from pyspark.sql.types import Row


class TestDeltaSchemaHandling:
    """Test Delta Lake schema handling functionality."""

    def test_delta_schema_read_with_custom_schema(self, spark, tmp_path):
        """Test with a custom schema and filter conditions."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        delta_path = tmp_path / "delta_custom_schema"
        delta_table_path = f"file://{delta_path}"

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("score", IntegerType(), True),
            ]
        )

        data = [(1, "Alice", 90), (2, "Bob", None), (3, "Charlie", 85)]
        spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).filter("name IS NOT NULL AND score > 80")
        result = result_df.collect()
        assert len(result) == 2  # noqa: PLR2004
        assert {row.name for row in result} == {"Alice", "Charlie"}

        loaded_schema = result_df.schema
        assert loaded_schema == schema, f"Schema mismatch: expected {schema}, got {loaded_schema}"

    def test_delta_schema_evolution_with_merge_schema(self, spark, tmp_path):
        """Test mergeSchema=true allows adding new columns during append."""
        delta_path = tmp_path / "delta_merge_schema"

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

        expected_data = pd.DataFrame(
            {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "Diana"], "age": [None, None, 30, 25]}
        ).astype({"id": "int32", "name": "string", "age": "Int32"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=False
        )

    def test_delta_schema_enforcement_without_merge_schema(self, spark, tmp_path):
        """Test that mergeSchema=false (default) rejects new columns."""
        delta_path = tmp_path / "delta_no_merge_schema"

        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to append data with additional column without mergeSchema
        extended_data = [Row(id=2, name="Bob", age=30)]
        df2 = spark.createDataFrame(extended_data)

        with pytest.raises(Exception, match=r"(?i)(schema|field)"):
            df2.write.format("delta").mode("append").save(str(delta_path))

    def test_delta_schema_overwrite_with_overwrite_schema(self, spark, tmp_path):
        """Test overwriteSchema=true with overwrite mode."""
        delta_path = tmp_path / "delta_overwrite_schema"

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

        expected_data = pd.DataFrame(
            {"user_id": [101, 102], "username": ["charlie", "diana"], "active": [True, False]}
        ).astype({"user_id": "int32", "username": "string", "active": "bool"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("user_id").reset_index(drop=True), expected_data, check_dtype=False
        )

    def test_delta_schema_overwrite_fails_with_append_mode(self, spark, tmp_path):
        """Test that overwriteSchema=true fails with append mode."""
        delta_path = tmp_path / "delta_overwrite_schema_append"

        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to use overwriteSchema with append mode (should fail)
        new_data = [Row(user_id=101, username="charlie")]
        df2 = spark.createDataFrame(new_data)

        with pytest.raises(Exception, match=r"(?i)overwrite.*(?:mode|schema)"):
            df2.write.format("delta").mode("append").option("overwriteSchema", "true").save(str(delta_path))

    def test_delta_schema_merge_and_overwrite_fails_together(self, spark, tmp_path):
        """Test that specifying both mergeSchema and overwriteSchema fails."""
        delta_path = tmp_path / "delta_both_options"

        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to use both options (should fail)
        new_data = [Row(id=2, name="Bob")]
        df2 = spark.createDataFrame(new_data)

        with pytest.raises(Exception, match=r"(?i).*merge.*overwrite.*"):
            df2.write.format("delta").mode("append").option("mergeSchema", "true").option(
                "overwriteSchema", "true"
            ).save(str(delta_path))

    def test_delta_schema_merge_with_compatible_types(self, spark, tmp_path):
        """Test mergeSchema behavior with compatible type changes."""
        from pyspark.sql.types import IntegerType, LongType, StructField, StructType

        delta_path = tmp_path / "delta_merge_type_changes"

        initial_schema = StructType([StructField("id", LongType(), True), StructField("value", IntegerType(), True)])
        initial_data = [Row(id=1, value=100)]
        df1 = spark.createDataFrame(initial_data, schema=initial_schema)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        initial_table_schema = spark.read.format("delta").load(str(delta_path)).schema
        assert initial_table_schema["value"].dataType == IntegerType()

        new_schema = StructType([StructField("id", LongType(), True), StructField("value", LongType(), True)])
        new_data = [Row(id=2, value=200)]
        df2 = spark.createDataFrame(new_data, schema=new_schema)

        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        result_df = spark.read.format("delta").load(str(delta_path))

        final_schema = result_df.schema

        assert final_schema["value"].dataType == LongType()

        result_pandas = result_df.sort("id").toPandas()

        expected_data = pd.DataFrame({"id": [1, 2], "value": [100, 200]})

        expected_data = expected_data.astype({"id": "int64", "value": "int64"})

        pd.testing.assert_frame_equal(
            result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=True
        )

    def test_delta_schema_merge_float_promotion(self, spark, tmp_path):
        """Test mergeSchema with Float32 to Float64 promotion."""
        from pyspark.sql.types import DoubleType, FloatType, IntegerType, StructField, StructType

        delta_path = tmp_path / "delta_float_promotion"

        # Create table with Float32
        initial_schema = StructType([StructField("id", IntegerType(), True), StructField("value", FloatType(), True)])
        initial_data = [Row(id=1, value=1.5)]
        df1 = spark.createDataFrame(initial_data, schema=initial_schema)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Append data with Float64
        new_schema = StructType([StructField("id", IntegerType(), True), StructField("value", DoubleType(), True)])
        new_data = [Row(id=2, value=2.5)]
        df2 = spark.createDataFrame(new_data, schema=new_schema)
        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        # Verify schema was promoted to Float64
        result_df = spark.read.format("delta").load(str(delta_path))
        final_schema = result_df.schema
        assert final_schema["value"].dataType == DoubleType()

        # Verify data integrity
        result_pandas = result_df.sort("id").toPandas()
        expected_data = pd.DataFrame({"id": [1, 2], "value": [1.5, 2.5]})
        expected_data = expected_data.astype({"id": "int32", "value": "float64"})
        pd.testing.assert_frame_equal(
            result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=True
        )
