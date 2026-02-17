import pytest
from pyspark.sql.types import Row


class TestDeltaPartitionPruning:
    """Delta Lake partition pruning tests"""

    def test_delta_pruning_with_equality(self, spark, tmp_path):
        """Test partition pruning with equality operators"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_comparison(self, spark, tmp_path):
        """Test partition pruning with comparison operators (>, >=, <, <=)"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_in_operator(self, spark, tmp_path):
        """Test partition pruning with IN operator"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_between_operator(self, spark, tmp_path):
        """Test partition pruning with BETWEEN operator"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_null_handling(self, spark, tmp_path):
        """Test partition pruning with NULL values and IS NULL/IS NOT NULL"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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
        assert result_count == 2, f"Expected 2 rows for region IS NOT NULL AND category = 'A', got {result_count}"  # noqa: PLR2004

    def test_delta_pruning_with_complex_expressions(self, spark, tmp_path):
        """Test partition pruning with complex boolean expressions"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_string_partitions(self, spark, tmp_path):
        """Test partition pruning with string partition values"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_edge_cases(self, spark, tmp_path):
        """Test partition pruning edge cases and boundary conditions"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

        # Test large values
        filtered_df = spark.read.format("delta").load(delta_table_path).filter("num_partition > 1000")
        result_count = filtered_df.count()
        assert result_count == 1, f"Expected 1 row for num_partition > 1000, got {result_count}"

    def test_delta_pruning_with_no_matching_partitions(self, spark, tmp_path):
        """Test partition pruning when no partitions match the filter"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_with_mixed_types(self, spark, tmp_path):
        """Test partition pruning with different data types"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    def test_delta_pruning_correctness(self, spark, tmp_path):
        """Test partition pruning correctness"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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
        assert result_count == expected_count, (
            f"Expected {expected_count} rows for year=2022 AND month=6, got {result_count}"
        )

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

    def test_delta_pruning_with_non_partition_columns(self, spark, tmp_path):
        """Test that partition pruning works correctly when combined with non-partition column filters"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"

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

    @pytest.fixture(scope="class")
    def setup_partitioned_table(self, spark, tmp_path_factory):
        """Fixture to create a partitioned table for parametrized tests"""
        delta_path = tmp_path_factory.mktemp("partitioned_table")

        partition_data = []
        for year in [2022, 2023]:
            for month in range(1, 13):  # 12 months
                for category in ["A", "B"]:
                    # 10 records per partition
                    for i in range(10):
                        partition_data.append(
                            Row(
                                id=len(partition_data),
                                year=year,
                                month=month,
                                category=category,
                                value=f"data_{year}_{month}_{category}_{i}",
                                score=i % 5,  # scores 0-4
                            )
                        )

        df = spark.createDataFrame(partition_data)
        df.write.format("delta").partitionBy("year", "month").mode("overwrite").save(str(delta_path))

        return f"{delta_path}"

    @pytest.mark.parametrize(
        ("filter_str", "expected_count"),
        [
            ("year = 2023", 12 * 2 * 10),  # 1 year * 12 months * 2 categories * 10 records
            ("year = 2022 AND month = 6", 2 * 10),  # 1 year * 1 month * 2 categories * 10 records
            ("year >= 2023 AND month <= 3", 1 * 3 * 2 * 10),  # 1 year * 3 months * 2 categories * 10 records
            (
                "year IN (2022, 2023) AND month IN (1, 12)",
                2 * 2 * 2 * 10,
            ),  # 2 years * 2 months * 2 categories * 10 records
            (
                "year BETWEEN 2022 AND 2023 AND month BETWEEN 6 AND 8",
                2 * 3 * 2 * 10,
            ),  # 2 years * 3 months * 2 categories * 10 records
            ("year != 2022", 1 * 12 * 2 * 10),  # 1 year * 12 months * 2 categories * 10 records
            ("month > 10", 2 * 2 * 2 * 10),  # 2 years * 2 months (11,12) * 2 categories * 10 records
            ("month < 3", 2 * 2 * 2 * 10),  # 2 years * 2 months (1,2) * 2 categories * 10 records
        ],
    )
    def test_delta_pruning_parametrized_scenarios(self, spark, setup_partitioned_table, filter_str, expected_count):
        """Parametrized test for partition pruning scenarios"""
        delta_table_path = setup_partitioned_table

        filtered_df = spark.read.format("delta").load(delta_table_path).filter(filter_str)
        actual_count = filtered_df.count()

        assert actual_count == expected_count, (
            f"Filter '{filter_str}': expected {expected_count} records, got {actual_count}"
        )

    @pytest.mark.parametrize(
        ("filter_condition", "expected_count", "description"),
        [
            ("year = 2025", 2, "Single year filter"),
            ("year = 2026", 2, "Different year filter"),
            ("year >= 2026", 2, "Greater than or equal filter"),
            ("year < 2026", 2, "Less than filter"),
            ("year != 2025", 2, "Not equal filter"),
            ("year IN (2025)", 2, "IN clause with single value"),
            ("year IN (2025, 2026)", 4, "IN clause with multiple values"),
        ],
    )
    def test_delta_pruning_basic_scenarios(self, spark, tmp_path, filter_condition, expected_count, description):
        """Parametrized test for various partition filtering scenarios"""
        delta_path = tmp_path / "delta_partition_filter_test"

        partition_data = [
            Row(id=1, event="A", year=2025, score=0.8),
            Row(id=2, event="B", year=2025, score=0.9),
            Row(id=3, event="A", year=2026, score=0.7),
            Row(id=4, event="B", year=2026, score=0.6),
        ]
        df = spark.createDataFrame(partition_data)

        df.write.format("delta").mode("overwrite").partitionBy("year").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(f"{delta_path}").filter(filter_condition)
        actual_count = filtered_df.count()

        assert actual_count == expected_count, f"{description}: expected {expected_count} records, got {actual_count}"
