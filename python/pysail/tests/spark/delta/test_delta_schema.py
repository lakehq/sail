from datetime import UTC, date, datetime

import pandas as pd
import pytest
from pyspark.sql.types import Row

from ..utils import get_data_files


class TestDeltaSchema:
    """Delta Lake schema-related tests"""

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

        from pandas.testing import assert_frame_equal

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

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

    def test_delta_query_with_custom_schema(self, spark, tmp_path):
        """Test with a custom schema and filter conditions."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        delta_path = tmp_path / "delta_custom_schema"
        delta_table_path = f"file://{delta_path}"

        # User-defined schema
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

    def test_delta_write_batch_size_option(self, spark, tmp_path):
        """Test Delta Lake write with write_batch_size option"""
        delta_path = tmp_path / "delta_write_batch_size"

        # Create test data with enough rows to test batching
        test_data = [
            Row(id=i, name=f"user_{i}", score=float(i * 0.1))
            for i in range(1, 101)  # 100 rows
        ]
        df = spark.createDataFrame(test_data)

        # Test with different write_batch_size values
        for batch_size in [10, 25, 50]:
            batch_path = delta_path / f"batch_{batch_size}"

            # Write with specific batch size
            df.write.format("delta").mode("overwrite").option("write_batch_size", str(batch_size)).save(str(batch_path))

            # Read back and verify data integrity
            result_df = spark.read.format("delta").load(f"file://{batch_path}").sort("id")
            result_data = result_df.collect()

            # Verify all data is present and correct
            assert len(result_data) == 100
            assert result_data[0].id == 1
            assert result_data[-1].id == 100
            assert result_data[0].name == "user_1"
            assert result_data[-1].name == "user_100"

    def test_delta_target_file_size_option(self, spark, tmp_path):
        """Test Delta Lake write with target_file_size option"""
        delta_path = tmp_path / "delta_target_file_size"

        # Create test data with enough content to test file sizing
        test_data = [
            Row(id=i, name=f"user_with_long_name_{i}" * 10, description=f"description_{i}" * 20)
            for i in range(1, 201)  # 200 rows with larger content
        ]
        df = spark.createDataFrame(test_data)

        # Test with different target_file_size values
        for file_size in [1024, 4096, 8192]:  # Small sizes to force multiple files
            size_path = delta_path / f"size_{file_size}"

            # Write with specific target file size
            df.write.format("delta").mode("overwrite").option("target_file_size", str(file_size)).save(str(size_path))

            # Physical artifact assertion: small target_file_size should create multiple files
            data_files = get_data_files(str(size_path))
            assert len(data_files) > 1, f"A small target_file_size ({file_size}) should create multiple output files, got {len(data_files)}"

            # Read back and verify data integrity
            result_df = spark.read.format("delta").load(f"file://{size_path}").sort("id")
            result_data = result_df.collect()

            # Verify all data is present and correct
            assert len(result_data) == 200
            assert result_data[0].id == 1
            assert result_data[-1].id == 200
            assert "user_with_long_name_1" in result_data[0].name
            assert "user_with_long_name_200" in result_data[-1].name

    def test_delta_combined_write_options(self, spark, tmp_path):
        """Test Delta Lake write with both write_batch_size and target_file_size options"""
        delta_path = tmp_path / "delta_combined_options"

        # Create test data
        test_data = [
            Row(id=i, name=f"user_{i}", data=f"data_content_{i}" * 5, score=float(i * 0.01))
            for i in range(1, 151)  # 150 rows
        ]
        df = spark.createDataFrame(test_data)

        # Write with both options
        df.write.format("delta").mode("overwrite") \
            .option("write_batch_size", "20") \
            .option("target_file_size", "2048") \
            .save(str(delta_path))

        # Read back and verify data integrity
        result_df = spark.read.format("delta").load(f"file://{delta_path}").sort("id")
        result_data = result_df.collect()

        # Verify all data is present and correct
        assert len(result_data) == 150
        assert result_data[0].id == 1
        assert result_data[-1].id == 150
        assert result_data[0].name == "user_1"
        assert result_data[-1].name == "user_150"

        # Verify data types are preserved
        assert isinstance(result_data[0].score, float)
        assert result_data[0].score == 0.01
        assert result_data[-1].score == 1.5

    @pytest.mark.skip(reason="Temporarily skipped")
    def test_delta_write_options_with_partitioning(self, spark, tmp_path):
        """Test Delta Lake write options combined with partitioning"""
        delta_path = tmp_path / "delta_options_partitioned"

        # Create partitioned test data
        test_data = [
            Row(id=i, category=f"cat_{i % 3}", name=f"user_{i}", value=i * 10)
            for i in range(1, 61)  # 60 rows across 3 partitions
        ]
        df = spark.createDataFrame(test_data)

        # Write with options and partitioning
        df.write.format("delta").mode("overwrite") \
            .option("write_batch_size", "15") \
            .option("target_file_size", "1024") \
            .partitionBy("category") \
            .save(str(delta_path))

        # Read back and verify data integrity
        result_df = spark.read.format("delta").load(f"file://{delta_path}").sort("id")
        result_data = result_df.collect()

        # Verify all data is present and correct
        assert len(result_data) == 60
        assert result_data[0].id == 1
        assert result_data[-1].id == 60

        # Verify partitioning worked
        categories = {row.category for row in result_data}
        assert categories == {"cat_0", "cat_1", "cat_2"}

        # Test partition filtering
        filtered_df = spark.read.format("delta").load(f"file://{delta_path}").filter("category = 'cat_0'")
        filtered_data = filtered_df.collect()
        assert len(filtered_data) == 20  # 60 rows / 3 categories = 20 rows per category
