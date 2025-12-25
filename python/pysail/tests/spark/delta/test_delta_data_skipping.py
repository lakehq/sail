from datetime import date

from pyspark.sql.types import LongType, Row, StringType, StructField, StructType


class TestDeltaDataSkipping:
    """Delta Lake data skipping (file pruning) tests"""

    def test_delta_skipping_on_numeric_column(self, spark, tmp_path):
        """Test data skipping (file pruning) on a non-partitioned numeric column."""
        delta_path = tmp_path / "delta_data_skipping_numeric"
        delta_table_path = f"{delta_path}"

        df1 = spark.createDataFrame([Row(id=i, value=float(i)) for i in range(1, 11)])
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        df2 = spark.createDataFrame([Row(id=i, value=float(i)) for i in range(100001, 100011)])
        df2.write.format("delta").mode("append").save(str(delta_path))

        df3 = spark.createDataFrame([Row(id=i, value=float(i)) for i in range(200001, 200011)])
        df3.write.format("delta").mode("append").save(str(delta_path))

        filtered_df = spark.read.format("delta").load(delta_table_path).filter("value > 200000.0")

        assert filtered_df.count() == 10, "Should return exactly 10 records from the third file"  # noqa: PLR2004
        assert filtered_df.agg({"value": "min"}).collect()[0][0] == 200001.0, "Minimum value should be 200001"  # noqa: PLR2004
        assert filtered_df.agg({"value": "max"}).collect()[0][0] == 200010.0, "Maximum value should be 200010"  # noqa: PLR2004

    def test_delta_skipping_on_string_and_date(self, spark, tmp_path):
        """Test data skipping on string and date columns."""
        delta_path = tmp_path / "delta_data_skipping_str_date"
        delta_table_path = f"{delta_path}"

        df1_data = [Row(event_name=chr(65 + i), event_date=date(2023, 1, 1 + i)) for i in range(3)]
        spark.createDataFrame(df1_data).write.format("delta").mode("overwrite").save(str(delta_path))

        df2_data = [Row(event_name=chr(77 + i), event_date=date(2023, 6, 1 + i)) for i in range(3)]
        spark.createDataFrame(df2_data).write.format("delta").mode("append").save(str(delta_path))

        df3_data = [Row(event_name=chr(88 + i), event_date=date(2023, 12, 1 + i)) for i in range(3)]
        spark.createDataFrame(df3_data).write.format("delta").mode("append").save(str(delta_path))

        filtered_df_str = spark.read.format("delta").load(delta_table_path).filter("event_name > 'W'")

        assert filtered_df_str.count() == 3  # noqa: PLR2004

        filtered_df_date = spark.read.format("delta").load(delta_table_path).filter("event_date < '2023-03-01'")
        assert filtered_df_date.count() == 3  # noqa: PLR2004

    def test_delta_skipping_with_null_counts(self, spark, tmp_path):
        """Test data skipping using null_count statistics for IS NULL and IS NOT NULL queries."""
        delta_path = tmp_path / "delta_data_skipping_null"
        delta_table_path = f"{delta_path}"

        df1_data = [Row(id=i, optional_col=f"value_{i}") for i in range(10)]
        spark.createDataFrame(df1_data).write.format("delta").mode("overwrite").save(str(delta_path))

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("optional_col", StringType(), True),
            ]
        )
        df2_data = [(i + 10, None) for i in range(10)]
        spark.createDataFrame(df2_data, schema=schema).write.format("delta").mode("append").save(str(delta_path))

        df3_data = [Row(id=i + 20, optional_col=f"value_{i}" if i % 2 == 0 else None) for i in range(10)]
        spark.createDataFrame(df3_data).write.format("delta").mode("append").save(str(delta_path))

        filtered_df_not_null = spark.read.format("delta").load(delta_table_path).filter("optional_col IS NOT NULL")
        assert filtered_df_not_null.count() == 10 + 5  # File 1 (10) + File 3 (5)

        filtered_df_is_null = spark.read.format("delta").load(delta_table_path).filter("optional_col IS NULL")
        assert filtered_df_is_null.count() == 10 + 5  # File 2 (10) + File 3 (5)
