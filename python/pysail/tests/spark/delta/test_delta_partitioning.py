import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from ..utils import assert_file_count_in_partitions, get_partition_structure  # noqa: TID252


class TestDeltaPartitioning:
    """Delta Lake partitioning functionality tests"""

    @pytest.fixture(scope="class")
    def delta_test_data(self):
        """Test data"""
        return [
            Row(id=10, event="A", score=0.98),
            Row(id=11, event="B", score=0.54),
            Row(id=12, event="A", score=0.76),
        ]

    def test_delta_partitioning_by_single_column(self, spark, tmp_path):
        """Test Delta Lake partitioning functionality"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"
        # Create partition data
        partition_data = [
            Row(id=1, event="A", year=2025, score=0.8),
            Row(id=2, event="B", year=2025, score=0.9),
            Row(id=3, event="A", year=2026, score=0.7),
            Row(id=4, event="B", year=2026, score=0.6),
        ]
        df = spark.createDataFrame(partition_data)

        # Write partitioned table
        df.write.format("delta").mode("overwrite").partitionBy("year").save(str(delta_path))

        # Physical artifact assertion: verify partition directory structure
        partition_dirs = get_partition_structure(str(delta_path))
        assert partition_dirs == {"year=2025", "year=2026"}, f"Expected year partitions, got {partition_dirs}"

        # Read entire table
        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "event": ["A", "B", "A", "B"],
                "score": [0.8, 0.9, 0.7, 0.6],
                "year": [2025, 2025, 2026, 2026],
            }
        ).astype({"id": "int32", "event": "string", "score": "float64", "year": "int32"})

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2025").sort("id")
        expected = 2025
        expected_filtered = expected_data[expected_data["year"] == expected].sort_values("id").reset_index(drop=True)

        assert_frame_equal(filtered_df.toPandas(), expected_filtered, check_dtype=False)
        assert filtered_df.count() == 2, "Partition pruning should return exactly 2 records for year=2025"  # noqa: PLR2004

        filtered_df_ne = spark.read.format("delta").load(delta_table_path).filter("year != 2025")
        assert filtered_df_ne.count() == 2, "NOT EQUAL filter should return 2 records for year!=2025"  # noqa: PLR2004

        filtered_df_gt = spark.read.format("delta").load(delta_table_path).filter("year > 2025")
        assert filtered_df_gt.count() == 2, "GREATER THAN filter should return 2 records for year>2025"  # noqa: PLR2004

    def test_delta_partitioning_creates_correct_directory_structure(self, spark, delta_test_data, tmp_path):
        delta_path = tmp_path / "partitioned_delta_table"
        delta_table_path = f"{delta_path}"

        df = spark.createDataFrame(delta_test_data)

        df.write.format("delta").mode("overwrite").partitionBy("id").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame(
            {"event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76], "id": [10, 11, 12]}
        ).astype({"event": "string", "score": "float64", "id": "int32"})

        result_pandas = result_df.toPandas()
        result_pandas = result_pandas.sort_values("id").reset_index(drop=True)
        expected_data = expected_data.sort_values("id").reset_index(drop=True)

        assert_frame_equal(result_pandas, expected_data, check_dtype=False)

        partition_dirs = []
        for item in os.listdir(delta_path):
            item_path = delta_path / item
            if os.path.isdir(item_path) and item.startswith("id="):
                partition_dirs.append(item)

        expected_partitions = {"id=10", "id=11", "id=12"}
        actual_partitions = set(partition_dirs)
        assert actual_partitions == expected_partitions, f"Expected {expected_partitions}, got {actual_partitions}"

        assert_file_count_in_partitions(str(delta_path), expected_files_per_partition=1)

    def test_delta_partitioning_by_multiple_columns(self, spark, tmp_path):
        """Test multi-column partitioning behavior."""
        delta_path = tmp_path / "multi_partitioned_delta_table"

        multi_partition_data = [
            Row(id=1, region=1, category=1, value=100),
            Row(id=2, region=1, category=2, value=200),
            Row(id=3, region=2, category=1, value=300),
            Row(id=4, region=2, category=2, value=400),
        ]

        df = spark.createDataFrame(multi_partition_data)

        df.write.format("delta").mode("overwrite").partitionBy("region", "category").save(str(delta_path))

        result_df = spark.read.format("delta").load(f"{delta_path}").sort("id")

        expected_data = pd.DataFrame(
            {"id": [1, 2, 3, 4], "value": [100, 200, 300, 400], "region": [1, 1, 2, 2], "category": [1, 2, 1, 2]}
        ).astype({"id": "int32", "value": "int32", "region": "int32", "category": "int32"})

        result_pandas = result_df.toPandas().sort_values("id").reset_index(drop=True)
        expected_data = expected_data.sort_values("id").reset_index(drop=True)

        assert_frame_equal(result_pandas, expected_data, check_dtype=False)

        expected_partition_structure = {
            "region=1/category=1",
            "region=1/category=2",
            "region=2/category=1",
            "region=2/category=2",
        }

        actual_partitions = set()
        for region_dir in os.listdir(delta_path):
            if region_dir.startswith("region="):
                region_path = delta_path / region_dir
                if os.path.isdir(region_path):
                    for category_dir in os.listdir(region_path):
                        if category_dir.startswith("category="):
                            actual_partitions.add(f"{region_dir}/{category_dir}")

        assert actual_partitions == expected_partition_structure, (
            f"Expected {expected_partition_structure}, got {actual_partitions}"
        )

        df_region1 = spark.read.format("delta").load(f"{delta_path}").filter("region = 1")
        assert df_region1.count() == 2, "Region 1 should have 2 records"  # noqa: PLR2004

        df_region2_cat2 = spark.read.format("delta").load(f"{delta_path}").filter("region = 2 AND category = 2")
        assert df_region2_cat2.count() == 1, "Region 2, Category 2 should have 1 record"

        df_region_ge2 = spark.read.format("delta").load(f"{delta_path}").filter("region >= 2")
        assert df_region_ge2.count() == 2, "Region >= 2 should have 2 records"  # noqa: PLR2004
