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

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2025").sort("id")
        expected = 2025
        expected_filtered = expected_data[expected_data["year"] == expected].sort_values("id").reset_index(drop=True)

        assert_frame_equal(filtered_df.toPandas(), expected_filtered, check_dtype=False)
        assert filtered_df.count() == 2, "Partition pruning should return exactly 2 records for year=2025"  # noqa: PLR2004

        filtered_df_ne = spark.read.format("delta").load(delta_table_path).filter("year != 2025")
        assert filtered_df_ne.count() == 2, "NOT EQUAL filter should return 2 records for year!=2025"  # noqa: PLR2004

        filtered_df_gt = spark.read.format("delta").load(delta_table_path).filter("year > 2025")
        assert filtered_df_gt.count() == 2, "GREATER THAN filter should return 2 records for year>2025"  # noqa: PLR2004

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

    def test_delta_partition_pruning_equality(self, spark, tmp_path):
        """Test partition pruning with equality operators"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = [
            Row(id=1, event="A", year=2023, month=1, score=0.8),
            Row(id=2, event="B", year=2023, month=1, score=0.9),
            Row(id=3, event="A", year=2023, month=2, score=0.7),
            Row(id=4, event="B", year=2023, month=2, score=0.6),
            Row(id=5, event="C", year=2024, month=1, score=0.85),
            Row(id=6, event="D", year=2024, month=1, score=0.95),
            Row(id=7, event="C", year=2024, month=2, score=0.75),
            Row(id=8, event="D", year=2024, month=2, score=0.65),
        ]
        df = spark.createDataFrame(partition_data)

        df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2023")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for year=2023, got {result_count}"  # noqa: PLR2004

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2023 AND month = 1")
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for year=2023 AND month=1, got {result_count}"  # noqa: PLR2004

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year != 2023")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for year!=2023, got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_comparison_operators(self, spark, tmp_path):
        """Test partition pruning with comparison operators (>, >=, <, <=)"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for year in [2020, 2021, 2022, 2023, 2024]:
            for month in [1, 6, 12]:
                for i in range(2):
                    partition_data.append(
                        Row(id=len(partition_data), value=f"data_{year}_{month}_{i}", year=year, month=month)
                    )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(str(delta_path))

        # Test greater than
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year > 2022")
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for year>2022, got {result_count}"  # noqa: PLR2004

        # Test greater than or equal
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year >= 2023")
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for year>=2023, got {result_count}"  # noqa: PLR2004

        # Test less than
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year < 2022")
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for year<2022, got {result_count}"  # noqa: PLR2004

        # Test less than or equal
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year <= 2021")
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for year<=2021, got {result_count}"  # noqa: PLR2004

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year >= 2022 AND month >= 6")
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for year>=2022 AND month>=6, got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_in_operator(self, spark, tmp_path):
        """Test partition pruning with IN operator"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = [
            Row(id=1, event="A", category="cat1", priority=1, value="data1"),
            Row(id=2, event="B", category="cat1", priority=2, value="data2"),
            Row(id=3, event="C", category="cat2", priority=1, value="data3"),
            Row(id=4, event="A", category="cat2", priority=3, value="data4"),
            Row(id=5, event="B", category="cat3", priority=2, value="data5"),
            Row(id=6, event="D", category="cat3", priority=1, value="data6"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("event", "category").save(str(delta_path))

        # Test IN with string values
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("event IN ('A', 'B')")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for event IN ('A', 'B'), got {result_count}"  # noqa: PLR2004

        # Test IN with single value (should behave like equality)
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("category IN ('cat1')")
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for category IN ('cat1'), got {result_count}"  # noqa: PLR2004

        # Test NOT IN
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("event NOT IN ('A')")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for event NOT IN ('A'), got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_between_operator(self, spark, tmp_path):
        """Test partition pruning with BETWEEN operator"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for year in range(2020, 2025):
            for month in range(1, 13):
                partition_data.append(
                    Row(id=len(partition_data), year=year, month=month, value=f"data_{year}_{month:02d}")
                )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(str(delta_path))

        # Test BETWEEN on year
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year BETWEEN 2022 AND 2023")
        result_count = filtered_df.count()
        assert result_count == 24, f"Expected 24 rows for year BETWEEN 2022 AND 2023, got {result_count}"  # noqa: PLR2004

        # Test BETWEEN on month
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("month BETWEEN 6 AND 8")
        result_count = filtered_df.count()
        assert result_count == 15, f"Expected 15 rows for month BETWEEN 6 AND 8, got {result_count}"  # noqa: PLR2004

        # Test NOT BETWEEN
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year NOT BETWEEN 2021 AND 2022")
        result_count = filtered_df.count()
        assert result_count == 36, f"Expected 36 rows for year NOT BETWEEN 2021 AND 2022, got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_null_handling(self, spark, tmp_path):
        """Test partition pruning with NULL values and IS NULL/IS NOT NULL"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = [
            Row(id=1, region="US", category="A", value="data1"),
            Row(id=2, region="EU", category="A", value="data2"),
            Row(id=3, region=None, category="B", value="data3"),
            Row(id=4, region="US", category=None, value="data4"),
            Row(id=5, region=None, category=None, value="data5"),
            Row(id=6, region="ASIA", category="C", value="data6"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("region", "category").save(str(delta_path))

        # Test IS NULL on single column
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region IS NULL")
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for region IS NULL, got {result_count}"  # noqa: PLR2004

        # Test IS NOT NULL
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region IS NOT NULL")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for region IS NOT NULL, got {result_count}"  # noqa: PLR2004

        # Test combination of NULL and equality
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region IS NOT NULL AND category = 'A'")
        result_count = filtered_df.count()
        print(filtered_df.explain())
        assert result_count == 2, f"Expected 2 rows for region IS NOT NULL AND category = 'A', got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_complex_expressions(self, spark, tmp_path):
        """Test partition pruning with complex boolean expressions"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for year in [2022, 2023, 2024]:
            for quarter in [1, 2, 3, 4]:
                for region in ["US", "EU", "ASIA"]:
                    for i in range(2):
                        partition_data.append(
                            Row(
                                id=len(partition_data),
                                year=year,
                                quarter=quarter,
                                region=region,
                                value=f"data_{year}_{quarter}_{region}_{i}",
                            )
                        )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "quarter", "region").save(str(delta_path))

        # Test OR conditions
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2022 OR year = 2024")
        result_count = filtered_df.count()
        assert result_count == 48, f"Expected 48 rows for year = 2022 OR year = 2024, got {result_count}"  # noqa: PLR2004

        # Test AND conditions across multiple partition columns
        filtered_df = (
            spark.read.format("delta")
            .load(delta_table_path)
            .filter("year >= 2023 AND quarter IN (1, 4) AND region != 'ASIA'")
        )
        result_count = filtered_df.count()
        assert result_count == 16, f"Expected 16 rows for complex AND condition, got {result_count}"  # noqa: PLR2004

        # Test mixed AND/OR with parentheses
        filtered_df = (
            spark.read.format("delta")
            .load(delta_table_path)
            .filter("(year = 2022 AND quarter = 1) OR (year = 2024 AND quarter = 4)")
        )
        result_count = filtered_df.count()
        assert result_count == 12, f"Expected 12 rows for mixed AND/OR condition, got {result_count}"  # noqa: PLR2004

        # Test NOT with complex expression
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("NOT (year = 2023 AND region = 'US')")
        result_count = filtered_df.count()
        assert result_count == 64, f"Expected 64 rows for NOT condition, got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_string_partitions(self, spark, tmp_path):
        """Test partition pruning with string partition values"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = [
            Row(id=1, department="engineering", team="backend", name="Alice"),
            Row(id=2, department="engineering", team="frontend", name="Bob"),
            Row(id=3, department="engineering", team="ml", name="Charlie"),
            Row(id=4, department="marketing", team="growth", name="Diana"),
            Row(id=5, department="marketing", team="content", name="Eve"),
            Row(id=6, department="sales", team="enterprise", name="Frank"),
            Row(id=7, department="sales", team="smb", name="Grace"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("department", "team").save(str(delta_path))

        # Test string equality
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("department = 'engineering'")
        result_count = filtered_df.count()
        assert result_count == 3, f"Expected 3 rows for department = 'engineering', got {result_count}"  # noqa: PLR2004

        # Test string IN operator
        filtered_df = (
            spark.read.format("delta").load(delta_table_path).filter("team IN ('backend', 'frontend', 'growth')")
        )
        result_count = filtered_df.count()
        assert result_count == 3, f"Expected 3 rows for team IN list, got {result_count}"  # noqa: PLR2004

        # Test string comparison (lexicographic order)
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("department > 'engineering'")
        result_count = filtered_df.count()
        assert result_count == 4, f"Expected 4 rows for department > 'engineering', got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_edge_cases(self, spark, tmp_path):
        """Test partition pruning edge cases and boundary conditions"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        # Create data with edge case values
        partition_data = [
            Row(id=1, num_partition=0, str_partition="", value="data1"),
            Row(id=2, num_partition=-1, str_partition="a", value="data2"),
            Row(id=3, num_partition=1, str_partition="z", value="data3"),
            Row(id=4, num_partition=999999, str_partition="UPPERCASE", value="data4"),
            Row(id=5, num_partition=-999999, str_partition="lowercase", value="data5"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("num_partition", "str_partition").save(str(delta_path))

        # Test zero value
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("num_partition = 0")
        result_count = filtered_df.count()
        assert result_count == 1, f"Expected 1 row for num_partition = 0, got {result_count}"

        # Test negative values
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("num_partition < 0")
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for num_partition < 0, got {result_count}"  # noqa: PLR2004

        # Test empty string
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("str_partition = ''")
        result_count = filtered_df.count()
        assert result_count == 1, f"Expected 1 row for str_partition = '', got {result_count}"

        # Test large values
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("num_partition > 1000")
        result_count = filtered_df.count()
        assert result_count == 1, f"Expected 1 row for num_partition > 1000, got {result_count}"

    def test_delta_partition_pruning_no_matching_partitions(self, spark, tmp_path):
        """Test partition pruning when no partitions match the filter"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        # Create simple partitioned data
        partition_data = [
            Row(id=1, year=2023, month=1, value="data1"),
            Row(id=2, year=2023, month=2, value="data2"),
            Row(id=3, year=2024, month=1, value="data3"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(str(delta_path))

        # Test filter that matches no partitions
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2025")
        result_count = filtered_df.count()
        assert result_count == 0, f"Expected 0 rows for year = 2025, got {result_count}"

        # Test filter with impossible condition
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2023 AND year = 2024")
        result_count = filtered_df.count()
        assert result_count == 0, f"Expected 0 rows for impossible condition, got {result_count}"

    def test_delta_partition_pruning_mixed_types(self, spark, tmp_path):
        """Test partition pruning with different data types"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        # Create data with different partition column types
        partition_data = [
            Row(id=1, int_col=100, str_col="alpha", bool_col=True, value="data1"),
            Row(id=2, int_col=200, str_col="beta", bool_col=False, value="data2"),
            Row(id=3, int_col=300, str_col="gamma", bool_col=True, value="data3"),
            Row(id=4, int_col=400, str_col="delta", bool_col=False, value="data4"),
        ]
        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("int_col", "str_col", "bool_col").save(str(delta_path))

        # Test integer partition filtering
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("int_col >= 200")
        result_count = filtered_df.count()
        assert result_count == 3, f"Expected 3 rows for int_col >= 200, got {result_count}"  # noqa: PLR2004

        # Test boolean partition filtering
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("bool_col = true")
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for bool_col = true, got {result_count}"  # noqa: PLR2004

        # Test mixed type conditions
        filtered_df = (
            spark.read.format("delta").load(delta_table_path).filter("int_col IN (100, 300) AND bool_col = true")
        )
        result_count = filtered_df.count()
        assert result_count == 2, f"Expected 2 rows for mixed type condition, got {result_count}"  # noqa: PLR2004

    def test_delta_partition_pruning_performance_validation(self, spark, tmp_path):
        """Test partition pruning effectiveness with performance validation"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for year in range(2020, 2025):  # 5 years
            for month in range(1, 13):  # 12 months each
                for day in [1, 15]:  # 2 days per month
                    for i in range(10):  # 10 records per day
                        partition_data.append(
                            Row(
                                id=len(partition_data),
                                year=year,
                                month=month,
                                day=day,
                                data=f"record_{year}_{month:02d}_{day:02d}_{i}",
                                value=i * year,
                            )
                        )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "month", "day").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2023")
        result_count = filtered_df.count()
        expected_count = 12 * 2 * 10  # 12 months * 2 days * 10 records
        assert result_count == expected_count, f"Expected {expected_count} rows for year=2023, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2022 AND month = 6")
        result_count = filtered_df.count()
        expected_count = 2 * 10  # 2 days * 10 records
        assert (
            result_count == expected_count
        ), f"Expected {expected_count} rows for year=2022 AND month=6, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2021 AND month = 3 AND day = 15")
        result_count = filtered_df.count()
        expected_count = 10  # 10 records
        assert result_count == expected_count, f"Expected {expected_count} rows for specific date, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year >= 2023 AND month <= 3")
        result_count = filtered_df.count()
        expected_count = 2 * 3 * 2 * 10  # 2 years * 3 months * 2 days * 10 records
        assert result_count == expected_count, f"Expected {expected_count} rows for range filter, got {result_count}"

        filtered_df = (
            spark.read.format("delta").load(delta_table_path).filter("year IN (2020, 2024) AND month IN (1, 12)")
        )
        result_count = filtered_df.count()
        expected_count = 2 * 2 * 2 * 10  # 2 years * 2 months * 2 days * 10 records
        assert result_count == expected_count, f"Expected {expected_count} rows for IN filter, got {result_count}"

        filtered_df = (
            spark.read.format("delta")
            .load(delta_table_path)
            .filter("(year = 2020 AND month = 1) OR (year = 2024 AND month = 12)")
        )
        result_count = filtered_df.count()
        expected_count = 2 * 2 * 10  # 2 conditions * 2 days * 10 records
        assert result_count == expected_count, f"Expected {expected_count} rows for OR condition, got {result_count}"

    def test_delta_partition_pruning_with_non_partition_columns(self, spark, tmp_path):
        """Test that partition pruning works correctly when combined with non-partition column filters"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for year in [2023, 2024]:
            for category in ["A", "B", "C"]:
                for i in range(20):
                    partition_data.append(
                        Row(
                            id=len(partition_data),
                            year=year,
                            category=category,
                            score=i % 10,  # Non-partition column
                            status="active" if i % 2 == 0 else "inactive",  # Non-partition column
                            data=f"record_{year}_{category}_{i}",
                        )
                    )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("year", "category").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year = 2023 AND score >= 5")
        result_count = filtered_df.count()
        expected_count = 3 * 10  # 3 categories * 10 records with score >= 5
        assert result_count == expected_count, f"Expected {expected_count} rows, got {result_count}"

        filtered_df = (
            spark.read.format("delta")
            .load(delta_table_path)
            .filter("category IN ('A', 'B') AND status = 'active' AND year = 2024")
        )
        result_count = filtered_df.count()
        expected_count = 2 * 10  # 2 categories * 10 active records each
        assert result_count == expected_count, f"Expected {expected_count} rows, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("score = 7")
        result_count = filtered_df.count()
        expected_count = 2 * 3 * 2  # 2 years * 3 categories * 2 records with score = 7
        assert result_count == expected_count, f"Expected {expected_count} rows, got {result_count}"

    def test_delta_partition_pruning_optimization_verification(self, spark, tmp_path):
        """Test to verify partition pruning optimization is working by comparing filtered vs unfiltered scans"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        partition_data = []
        for region in ["US", "EU", "ASIA", "LATAM", "AFRICA"]:
            for year in [2022, 2023, 2024]:
                for month in range(1, 13):
                    for i in range(5):
                        partition_data.append(
                            Row(
                                id=len(partition_data),
                                region=region,
                                year=year,
                                month=month,
                                metric_value=i * 100 + month,
                                description=f"metric_{region}_{year}_{month}_{i}",
                            )
                        )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").mode("overwrite").partitionBy("region", "year").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region = 'US' AND year = 2023")
        result_count = filtered_df.count()
        expected_count = 12 * 5  # 12 months * 5 records per month
        assert (
            result_count == expected_count
        ), f"Expected {expected_count} rows for selective filter, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region IN ('US', 'EU')")
        result_count = filtered_df.count()
        expected_count = 2 * 3 * 12 * 5  # 2 regions * 3 years * 12 months * 5 records
        assert result_count == expected_count, f"Expected {expected_count} rows for medium filter, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("year >= 2022")
        result_count = filtered_df.count()
        expected_count = 5 * 3 * 12 * 5  # 5 regions * 3 years * 12 months * 5 records
        assert result_count == expected_count, f"Expected {expected_count} rows for broad filter, got {result_count}"

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("region = 'NONEXISTENT'")
        result_count = filtered_df.count()
        assert result_count == 0, f"Expected 0 rows for non-existent region, got {result_count}"

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
