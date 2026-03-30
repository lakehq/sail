"""
Test Delta Lake write options (write_batch_size, target_file_size) in Sail.
"""

from pyspark.sql.types import Row


class TestDeltaWriteOptions:
    """Test Delta Lake write options functionality."""

    def test_delta_write_option_write_batch_size(self, spark, tmp_path):
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
            result_df = spark.read.format("delta").load(f"{batch_path}").sort("id")
            result_data = result_df.collect()

            # Verify all data is present and correct
            assert len(result_data) == 100  # noqa: PLR2004
            assert result_data[0].id == 1
            assert result_data[-1].id == 100  # noqa: PLR2004
            assert result_data[0].name == "user_1"
            assert result_data[-1].name == "user_100"

    def test_delta_write_option_target_file_size(self, spark, tmp_path):
        """Test Delta Lake write with target_file_size option"""
        delta_path = tmp_path / "delta_target_file_size"

        test_data = [
            Row(
                id=i,
                name=f"user_with_very_long_name_{i}" * 15,
                description=f"detailed_description_content_{i}" * 10,
                metadata=f"additional_metadata_field_{i}" * 8,
            )
            for i in range(1, 1001)
        ]
        df = spark.createDataFrame(test_data)

        for file_size in [50000, 100000, 150000]:
            size_path = delta_path / f"size_{file_size}"

            df.write.format("delta").mode("overwrite").option("target_file_size", str(file_size)).option(
                "write_batch_size", "100"
            ).save(str(size_path))

            result_df = spark.read.format("delta").load(f"{size_path}").sort("id")
            result_data = result_df.collect()

            assert len(result_data) == 1000  # noqa: PLR2004
            assert result_data[0].id == 1
            assert result_data[-1].id == 1000  # noqa: PLR2004
            assert "user_with_very_long_name_1" in result_data[0].name
            assert "user_with_very_long_name_1000" in result_data[-1].name

    def test_delta_write_options_combined(self, spark, tmp_path):
        """Test Delta Lake write with both write_batch_size and target_file_size options"""
        delta_path = tmp_path / "delta_combined_options"

        # Create test data
        test_data = [
            Row(id=i, name=f"user_{i}", data=f"data_content_{i}" * 5, score=float(i * 0.01))
            for i in range(1, 151)  # 150 rows
        ]
        df = spark.createDataFrame(test_data)

        # Write with both options
        df.write.format("delta").mode("overwrite").option("write_batch_size", "20").option(
            "target_file_size", "2048"
        ).save(str(delta_path))

        # Read back and verify data integrity
        result_df = spark.read.format("delta").load(f"{delta_path}").sort("id")
        result_data = result_df.collect()

        # Verify all data is present and correct
        assert len(result_data) == 150  # noqa: PLR2004
        assert result_data[0].id == 1
        assert result_data[-1].id == 150  # noqa: PLR2004
        assert result_data[0].name == "user_1"
        assert result_data[-1].name == "user_150"

        # Verify data types are preserved
        assert isinstance(result_data[0].score, float)
        assert result_data[0].score == 0.01  # noqa: PLR2004
        assert result_data[-1].score == 1.5  # noqa: PLR2004

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
        df.write.format("delta").mode("overwrite").option("write_batch_size", "15").option(
            "target_file_size", "1024"
        ).partitionBy("category").save(str(delta_path))

        # Read back and verify data integrity
        result_df = spark.read.format("delta").load(f"{delta_path}").sort("id")
        result_data = result_df.collect()

        # Verify all data is present and correct
        assert len(result_data) == 60  # noqa: PLR2004
        assert result_data[0].id == 1
        assert result_data[-1].id == 60  # noqa: PLR2004

        # Verify partitioning worked
        categories = {row.category for row in result_data}
        assert categories == {"cat_0", "cat_1", "cat_2"}

        # Test partition filtering
        filtered_df = spark.read.format("delta").load(f"{delta_path}").filter("category = 'cat_0'")
        filtered_data = filtered_df.collect()
        assert len(filtered_data) == 20  # 60 rows / 3 categories = 20 rows per category  # noqa: PLR2004
