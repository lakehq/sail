import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from ..utils import assert_file_lifecycle, get_data_files  # noqa: TID252


class TestDeltaBasicOperations:
    """Delta Lake basic read/write operations tests"""

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

        # Physical artifact assertion: verify file creation
        data_files = get_data_files(str(delta_path))
        assert len(data_files) == 1, f"Expected exactly 1 data file, got {len(data_files)}"

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

        # Record files after first write
        files_v0 = set(get_data_files(str(delta_path)))
        assert len(files_v0) > 0, "Initial write should create files"

        append_data = [
            Row(id=13, event="C", score=0.89),
            Row(id=14, event="D", score=0.67),
        ]
        df2 = spark.createDataFrame(append_data)

        df2.write.format("delta").mode("append").save(str(delta_path))

        # Physical artifact assertion: verify file lifecycle for append
        files_v1 = set(get_data_files(str(delta_path)))
        assert_file_lifecycle(files_v0, files_v1, "append")

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

        # Record files after first write
        files_v0 = set(get_data_files(str(delta_path)))
        assert len(files_v0) > 0, "Initial write should create files"

        new_data = [
            Row(id=20, event="X", score=0.95),
            Row(id=21, event="Y", score=0.88),
        ]
        df2 = spark.createDataFrame(new_data)

        df2.write.format("delta").mode("overwrite").save(str(delta_path))

        # Physical artifact assertion: Delta Lake overwrite creates new files
        # Note: Old files remain physically but are marked as removed in transaction log
        files_v1 = set(get_data_files(str(delta_path)))
        assert len(files_v1) > len(files_v0), "Overwrite should create new data files"

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame({"id": [20, 21], "event": ["X", "Y"], "score": [0.95, 0.88]}).astype(
            {"id": "int32", "event": "string", "score": "float64"}
        )

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_overwrite_with_replace_where(self, spark, tmp_path):
        """Test Delta Lake overwrite with replaceWhere option."""
        from pyspark.sql.types import Row

        delta_path = tmp_path / "delta_replace_where"
        delta_table_path = f"file://{delta_path}"

        data = [
            Row(id=1, category="A", value=10),
            Row(id=2, category="B", value=20),
            Row(id=3, category="A", value=30),
            Row(id=4, category="B", value=40),
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        new_data = [
            Row(id=5, category="A", value=100),
            Row(id=6, category="A", value=200),
        ]
        new_df = spark.createDataFrame(new_data)
        new_df.write.format("delta").mode("overwrite").option("replaceWhere", "category = 'A'").save(str(delta_path))

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")
        result = result_df.collect()

        assert {row.id for row in result} == {2, 4, 5, 6}
        assert {row.category for row in result if row.category == "A"} == {"A"}
        assert {row.value for row in result if row.category == "A"} == {100, 200}
        assert {row.value for row in result if row.category == "B"} == {20, 40}
