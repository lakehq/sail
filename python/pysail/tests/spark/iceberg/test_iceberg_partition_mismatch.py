"""Test partition column mismatch validation for Iceberg tables"""

import json
from pathlib import Path

import pytest
from pyspark.sql.types import Row
from pyspark.sql.utils import AnalysisException


class TestIcebergPartitionMismatch:
    """Test cases for partition column mismatch validation in Iceberg"""

    def test_append_with_different_partition_columns_raises_error(self, spark, tmp_path):
        """Test that appending with different partition columns raises an error"""
        iceberg_path = f"file://{tmp_path}/partitioned_table"

        # Create initial partitioned table with partition column 'category'
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
            Row(id=3, category="A", value=300),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("category").save(iceberg_path)

        # Try to append with different partition column 'region'
        append_data = [
            Row(id=4, category="C", value=400, region="East"),
            Row(id=5, category="D", value=500, region="West"),
        ]
        df_append = spark.createDataFrame(append_data)

        # This should raise an error about partition column mismatch
        with pytest.raises(AnalysisException, match="Partition column mismatch"):
            df_append.write.format("iceberg").mode("append").partitionBy("region").save(iceberg_path)

    def test_append_with_multiple_different_partition_columns_raises_error(self, spark, tmp_path):
        """Test that appending with different multi-column partitioning raises an error"""
        iceberg_path = f"file://{tmp_path}/multi_partitioned_table"

        # Create initial table partitioned by 'year' and 'month'
        initial_data = [
            Row(id=1, year=2023, month=1, day=15, value=100),
            Row(id=2, year=2023, month=2, day=20, value=200),
            Row(id=3, year=2024, month=1, day=10, value=300),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("year", "month").save(iceberg_path)

        # Try to append with different partition columns 'year' and 'day'
        append_data = [
            Row(id=4, year=2024, month=3, day=25, value=400),
            Row(id=5, year=2024, month=4, day=30, value=500),
        ]
        df_append = spark.createDataFrame(append_data)

        # This should raise an error
        with pytest.raises(AnalysisException, match="Partition column mismatch"):
            df_append.write.format("iceberg").mode("append").partitionBy("year", "day").save(iceberg_path)

    def test_append_with_reordered_partition_columns_raises_error(self, spark, tmp_path):
        """Test that appending with the same columns but different order is rejected"""
        iceberg_path = f"file://{tmp_path}/reordered_partition_table"

        initial_data = [
            Row(id=1, year=2023, month=1, value=10),
            Row(id=2, year=2023, month=2, value=20),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("year", "month").save(iceberg_path)

        append_data = [
            Row(id=3, year=2024, month=3, value=30),
            Row(id=4, year=2024, month=4, value=40),
        ]
        df_append = spark.createDataFrame(append_data)

        with pytest.raises(AnalysisException, match="Partition column mismatch"):
            df_append.write.format("iceberg").mode("append").partitionBy("month", "year").save(iceberg_path)

    def test_append_with_same_partition_columns_succeeds(self, spark, tmp_path):
        """Test that appending with same partition columns succeeds"""
        iceberg_path = f"file://{tmp_path}/consistent_partitioned_table"

        # Create initial partitioned table
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("category").save(iceberg_path)

        # Append with same partition column
        append_data = [
            Row(id=3, category="C", value=300),
            Row(id=4, category="D", value=400),
        ]
        df_append = spark.createDataFrame(append_data)
        df_append.write.format("iceberg").mode("append").partitionBy("category").save(iceberg_path)

        # Verify the data
        result_df = spark.read.format("iceberg").load(iceberg_path).sort("id")
        result_count = result_df.count()
        assert result_count == 4, f"Expected 4 rows, got {result_count}"  # noqa: PLR2004

    def test_append_without_specifying_partition_columns_inherits_partitioning(self, spark, tmp_path):
        """Appending without partition spec should inherit existing partitioning"""
        iceberg_path = f"file://{tmp_path}/auto_partitioned_table"

        # Create initial partitioned table
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("category").save(iceberg_path)

        # Append without specifying partition columns (should inherit from table)
        append_data = [
            Row(id=3, category="C", value=300),
            Row(id=4, category="D", value=400),
        ]
        df_append = spark.createDataFrame(append_data)
        df_append.write.format("iceberg").mode("append").save(iceberg_path)

        # Verify the data
        result_df = spark.read.format("iceberg").load(iceberg_path).sort("id")
        result_count = result_df.count()
        assert result_count == 4, f"Expected 4 rows, got {result_count}"  # noqa: PLR2004

        # Verify table metadata still reflects the original partition spec
        table_dir = Path(iceberg_path.replace("file://", ""))
        metadata_files = sorted(table_dir.joinpath("metadata").glob("*.metadata.json"))
        latest_meta = json.loads(metadata_files[-1].read_text())
        default_spec_id = latest_meta["default-spec-id"]
        default_spec = next(spec for spec in latest_meta["partition-specs"] if spec["spec-id"] == default_spec_id)
        partition_names = [field["name"] for field in default_spec.get("fields", [])]
        assert partition_names == ["category"], f"Expected partition spec to remain ['category'], got {partition_names}"

    def test_overwrite_with_different_partition_without_schema_overwrite_raises_error(self, spark, tmp_path):
        """Test that overwriting with different partition columns without overwriteSchema raises error"""
        iceberg_path = f"file://{tmp_path}/overwrite_partition_table"

        # Create initial partitioned table
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("category").save(iceberg_path)

        # Try to overwrite with different partition column without overwriteSchema
        overwrite_data = [
            Row(id=3, category="C", value=300, region="East"),
            Row(id=4, category="D", value=400, region="West"),
        ]
        df_overwrite = spark.createDataFrame(overwrite_data)

        with pytest.raises(AnalysisException, match="Partition column mismatch"):
            df_overwrite.write.format("iceberg").mode("overwrite").partitionBy("region").save(iceberg_path)

    def test_overwrite_with_different_partition_and_schema_overwrite_succeeds(self, spark, tmp_path):
        """Test that overwriting with different partition columns with overwriteSchema=true succeeds"""
        iceberg_path = f"file://{tmp_path}/schema_overwrite_table"

        # Create initial partitioned table
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").partitionBy("category").save(iceberg_path)

        # Overwrite with different partition column with overwriteSchema=true
        # Keep compatible columns to avoid schema incompatibility issues
        overwrite_data = [
            Row(id=3, category="C", value=300, region="East"),
            Row(id=4, category="D", value=400, region="West"),
        ]
        df_overwrite = spark.createDataFrame(overwrite_data)
        df_overwrite.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").partitionBy(
            "region"
        ).save(iceberg_path)

        # Verify the data and new partitioning
        result_df = spark.read.format("iceberg").load(iceberg_path).sort("id")
        result_count = result_df.count()
        assert result_count == 2, f"Expected 2 rows after overwrite, got {result_count}"  # noqa: PLR2004

        # Verify schema contains new partition column
        columns = result_df.columns
        assert "region" in columns, "New partition column 'region' should exist"
        assert "category" in columns, "Column 'category' should still exist in data"

        # Verify table metadata reflects the new partition spec
        table_dir = Path(iceberg_path.replace("file://", ""))
        metadata_files = sorted(table_dir.joinpath("metadata").glob("*.metadata.json"))
        latest_meta = metadata_files[-1]
        meta = json.loads(latest_meta.read_text())
        default_spec_id = meta["default-spec-id"]
        default_spec = next(spec for spec in meta["partition-specs"] if spec["spec-id"] == default_spec_id)
        partition_names = [field["name"] for field in default_spec.get("fields", [])]
        assert partition_names == ["region"], f"Expected default partition spec to be ['region'], got {partition_names}"

    def test_append_to_unpartitioned_table_with_partitioning_raises_error(self, spark, tmp_path):
        """Test that appending with partitioning to an unpartitioned table raises error"""
        iceberg_path = f"file://{tmp_path}/unpartitioned_table"

        # Create initial unpartitioned table
        initial_data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df_initial = spark.createDataFrame(initial_data)
        df_initial.write.format("iceberg").mode("overwrite").save(iceberg_path)

        # Try to append with partition columns
        append_data = [
            Row(id=3, category="C", value=300),
            Row(id=4, category="D", value=400),
        ]
        df_append = spark.createDataFrame(append_data)

        with pytest.raises(AnalysisException, match="Partition column mismatch"):
            df_append.write.format("iceberg").mode("append").partitionBy("category").save(iceberg_path)

    def test_create_partition_with_unknown_column_raises_error(self, spark, tmp_path):
        """Test that creating a table with a non-existent partition column raises an error"""
        iceberg_path = f"file://{tmp_path}/unknown_partition_column_table"

        data = [
            Row(id=1, category="A", value=100),
            Row(id=2, category="B", value=200),
        ]
        df = spark.createDataFrame(data)

        with pytest.raises(AnalysisException, match="Partition column"):
            df.write.format("iceberg").mode("overwrite").partitionBy("missing_col").save(iceberg_path)
