import os
from datetime import UTC, date, datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from pysail.tests.spark.utils import is_jvm_spark


class TestDeltaLake:
    """Delta Lake data source read/write tests"""

    @pytest.fixture(scope="class")
    def delta_test_data(self):
        """Test data"""
        return [
            Row(id=10, event="A", score=0.98),
            Row(id=11, event="B", score=0.54),
            Row(id=12, event="A", score=0.76),
        ]

    @pytest.fixture(scope="class")
    def expected_pandas_df(self):
        """Expected pandas DataFrame"""
        return pd.DataFrame({"id": [10, 11, 12], "event": ["A", "B", "A"], "score": [0.98, 0.54, 0.76]}).astype(
            {"id": "int32", "event": "string", "score": "float64"}
        )

    def test_delta_write_and_read_basic(self, spark, delta_test_data, expected_pandas_df, tmp_path):
        """Test basic Delta Lake write and read operations"""
        delta_path = tmp_path / "delta_table"

        df = spark.createDataFrame(delta_test_data)

        df.write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(f"file://{delta_path}").sort("id")

        assert_frame_equal(
            result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_write_and_read_with_sql(self, spark, delta_test_data, expected_pandas_df, tmp_path):
        """Test creating Delta table with SQL and querying"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        df = spark.createDataFrame(delta_test_data)

        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE TABLE my_delta USING delta LOCATION '{delta_table_path}'")

        try:
            result_df = spark.sql("SELECT * FROM my_delta").sort("id")

            assert_frame_equal(
                result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
            )
        finally:
            spark.sql("DROP TABLE IF EXISTS my_delta")

    def test_delta_append_mode(self, spark, delta_test_data, tmp_path):
        """Test Delta Lake append mode"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        df1 = spark.createDataFrame(delta_test_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        append_data = [
            Row(id=13, event="C", score=0.89),
            Row(id=14, event="D", score=0.67),
        ]
        df2 = spark.createDataFrame(append_data)

        df2.write.format("delta").mode("append").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame(
            {
                "id": [10, 11, 12, 13, 14],
                "event": ["A", "B", "A", "C", "D"],
                "score": [0.98, 0.54, 0.76, 0.89, 0.67],
            }
        ).astype({"id": "int32", "event": "string", "score": "float64"})

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_overwrite_mode(self, spark, delta_test_data, tmp_path):
        """Test Delta Lake overwrite mode"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        df1 = spark.createDataFrame(delta_test_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        new_data = [
            Row(id=20, event="X", score=0.95),
            Row(id=21, event="Y", score=0.88),
        ]
        df2 = spark.createDataFrame(new_data)

        df2.write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame({"id": [20, 21], "event": ["X", "Y"], "score": [0.95, 0.88]}).astype(
            {"id": "int32", "event": "string", "score": "float64"}
        )

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    @pytest.mark.skip(reason="Temporarily skipped")
    def test_delta_schema_evolution(self, spark, tmp_path):
        """Test Delta Lake schema evolution"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        initial_data = [
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
        ]
        df1 = spark.createDataFrame(initial_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        new_data = [
            Row(id=3, name="Charlie", age=30),
            Row(id=4, name="Diana", age=25),
        ]
        df2 = spark.createDataFrame(new_data)

        df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame(
            {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "Diana"], "age": [None, None, 30, 25]}
        ).astype({"id": "int32", "name": "string", "age": "Int32"})

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_partition_by(self, spark, tmp_path):
        """Test Delta Lake partitioning functionality"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
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

        # Test partition filtering
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2025").sort("id")
        expected = 2025
        expected_filtered = expected_data[expected_data["year"] == expected].sort_values("id").reset_index(drop=True)

        assert_frame_equal(filtered_df.toPandas(), expected_filtered, check_dtype=False)

    def test_delta_partition_behavior(self, spark, delta_test_data, tmp_path):
        delta_path = tmp_path / "partitioned_delta_table"
        delta_table_path = f"file://{delta_path}"

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

        for partition_dir in partition_dirs:
            partition_path = delta_path / partition_dir
            parquet_files = [f for f in os.listdir(partition_path) if f.endswith(".parquet")]
            assert len(parquet_files) > 0, f"No parquet files found in partition directory {partition_dir}"

    def test_delta_multi_column_partitioning(self, spark, tmp_path):
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

        result_df = spark.read.format("delta").load(f"file://{delta_path}").sort("id")

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

        assert (
            actual_partitions == expected_partition_structure
        ), f"Expected {expected_partition_structure}, got {actual_partitions}"

    def test_delta_with_different_data_types(self, spark, tmp_path):
        """Test Delta Lake support for different data types"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        complex_data = [
            Row(
                id=1,
                name="Alice",
                age=30,
                salary=50000.50,
                is_active=True,
                birth_date=date(1993, 5, 15),
                created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=UTC),
                tags=["python", "spark"],
                metadata={"department": "engineering", "level": "senior"},
            ),
            Row(
                id=2,
                name="Bob",
                age=25,
                salary=45000.75,
                is_active=False,
                birth_date=date(1998, 8, 22),
                created_at=datetime(2025, 2, 1, 14, 45, 0, tzinfo=UTC),
                tags=["java", "scala"],
                metadata={"department": "product", "level": "junior"},
            ),
        ]

        df = spark.createDataFrame(complex_data)

        df.write.format("delta").mode("overwrite").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        result_pandas = result_df.toPandas()

        expected = 2
        assert len(result_pandas) == expected
        assert result_pandas["id"].tolist() == [1, 2]
        assert result_pandas["name"].tolist() == ["Alice", "Bob"]
        assert result_pandas["age"].tolist() == [30, 25]
        assert result_pandas["is_active"].tolist() == [True, False]

    @pytest.mark.skip(reason="Temporarily skipped")
    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake time travel")
    def test_delta_time_travel(self, spark, tmp_path):
        """Test Delta Lake time travel functionality"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        # Version 0: Initial data
        v0_data = [Row(id=1, value="v0")]
        df0 = spark.createDataFrame(v0_data)
        df0.write.format("delta").mode("overwrite").save(str(delta_path))

        # Version 1: Add data
        v1_data = [Row(id=2, value="v1")]
        df1 = spark.createDataFrame(v1_data)
        df1.write.format("delta").mode("append").save(str(delta_path))

        # Version 2: Overwrite data
        v2_data = [Row(id=3, value="v2")]
        df2 = spark.createDataFrame(v2_data)
        df2.write.format("delta").mode("overwrite").save(str(delta_path))

        # Read latest version
        latest_df = spark.read.format("delta").load(delta_table_path)
        assert latest_df.collect() == [Row(id=3, value="v2")]

        # Read version 0
        v0_df = spark.read.format("delta").option("versionAsOf", "0").load(delta_table_path)
        assert v0_df.collect() == [Row(id=1, value="v0")]

        # Read version 1
        v1_df = spark.read.format("delta").option("versionAsOf", "1").load(delta_table_path).sort("id")
        expected_v1 = [Row(id=1, value="v0"), Row(id=2, value="v1")]
        assert v1_df.collect() == expected_v1

    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake error handling")
    def test_delta_error_handling(self, spark, tmp_path):
        """Test Delta Lake error handling"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"
        # Try to read non-existent Delta table
        with pytest.raises(Exception, match=".*"):
            spark.read.format("delta").load(delta_table_path).collect()

        # Create table and try again
        test_data = [Row(id=1, name="test")]
        df = spark.createDataFrame(test_data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        result = spark.read.format("delta").load(delta_table_path).collect()
        assert result == [Row(id=1, name="test")]
