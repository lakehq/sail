import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from ..utils import assert_file_lifecycle, get_data_files  # noqa: TID252


class TestDeltaIO:
    """Delta Lake I/O operations tests"""

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

    def test_delta_io_basic_overwrite_and_read(self, spark, delta_test_data, expected_pandas_df, tmp_path):
        """Test basic Delta Lake write and read operations"""
        delta_path = tmp_path / "delta_table"

        df = spark.createDataFrame(delta_test_data)

        df.write.format("delta").mode("overwrite").save(str(delta_path))

        data_files = get_data_files(str(delta_path))
        assert len(data_files) == 1, f"Expected exactly 1 data file, got {len(data_files)}"

        result_df = spark.read.format("delta").load(f"file://{delta_path}").sort("id")

        assert_frame_equal(
            result_df.toPandas(), expected_pandas_df.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_io_create_table_with_sql(self, spark, delta_test_data, expected_pandas_df, tmp_path):
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

    def test_delta_io_append_mode(self, spark, delta_test_data, tmp_path):
        """Test Delta Lake append mode"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        df1 = spark.createDataFrame(delta_test_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        files_v0 = set(get_data_files(str(delta_path)))
        assert len(files_v0) > 0, "Initial write should create files"

        append_data = [
            Row(id=13, event="C", score=0.89),
            Row(id=14, event="D", score=0.67),
        ]
        df2 = spark.createDataFrame(append_data)

        df2.write.format("delta").mode("append").save(str(delta_path))

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

    def test_delta_io_overwrite_mode(self, spark, delta_test_data, tmp_path):
        """Test Delta Lake overwrite mode"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        df1 = spark.createDataFrame(delta_test_data)

        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        files_v0 = set(get_data_files(str(delta_path)))
        assert len(files_v0) > 0, "Initial write should create files"

        new_data = [
            Row(id=20, event="X", score=0.95),
            Row(id=21, event="Y", score=0.88),
        ]
        df2 = spark.createDataFrame(new_data)

        df2.write.format("delta").mode("overwrite").save(str(delta_path))

        files_v1 = set(get_data_files(str(delta_path)))
        assert len(files_v1) > len(files_v0), "Overwrite should create new data files"

        result_df = spark.read.format("delta").load(delta_table_path).sort("id")

        expected_data = pd.DataFrame({"id": [20, 21], "event": ["X", "Y"], "score": [0.95, 0.88]}).astype(
            {"id": "int32", "event": "string", "score": "float64"}
        )

        assert_frame_equal(
            result_df.toPandas(), expected_data.sort_values("id").reset_index(drop=True), check_dtype=False
        )

    def test_delta_io_overwrite_partitions_with_replace_where(self, spark, tmp_path):
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

    def test_delta_io_overwrite_partitions_with_v2_api(self, spark, tmp_path):
        """Test Delta Lake overwrite with a condition using the V2 API."""
        from pyspark.sql import functions as F  # noqa: N812
        from pyspark.sql.types import Row

        delta_path = tmp_path / "delta_condition_v2"
        delta_table_path = f"file://{delta_path}"
        table_name = "delta_v2_overwrite_test"

        table_columns = "(id bigint, category string, value bigint)"

        data = [
            Row(id=1, category="A", value=10),
            Row(id=2, category="B", value=20),
            Row(id=3, category="A", value=30),
            Row(id=4, category="B", value=40),
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        spark.sql(f"CREATE OR REPLACE TABLE {table_name} {table_columns} USING DELTA LOCATION '{delta_table_path}'")

        try:
            new_data = [
                Row(id=5, category="A", value=100),
                Row(id=6, category="A", value=200),
            ]
            new_df = spark.createDataFrame(new_data)

            new_df.writeTo(table_name).overwrite(F.col("category") == "A")
            # spark.sql(f"INSERT INTO TABLE {table_name} REPLACE WHERE category = 'A' SELECT * FROM VALUES (5, 'A', 100), (6, 'A', 200)")

            result_df = spark.read.format("delta").load(delta_table_path).sort("id")
            result = result_df.collect()

            assert {row.id for row in result} == {2, 4, 5, 6}
            assert {row.category for row in result if row.category == "A"} == {"A"}
            assert {row.value for row in result if row.category == "A"} == {100, 200}
            assert {row.value for row in result if row.category == "B"} == {20, 40}
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_delta_io_all_data_types(self, spark, tmp_path):
        """Test Delta Lake support for different data types"""
        from datetime import UTC, date, datetime

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

    def test_delta_io_error_on_read_nonexistent_table(self, spark, tmp_path):
        """Test Delta Lake error handling"""
        from pysail.tests.spark.utils import is_jvm_spark

        delta_path = tmp_path / "delta_table"
        delta_table_path = f"file://{delta_path}"

        # Skip for JVM Spark as error handling may differ
        if not is_jvm_spark():
            # Try to read non-existent Delta table
            with pytest.raises(Exception, match=".*"):
                spark.read.format("delta").load(delta_table_path).collect()

        # Create table and try again
        test_data = [Row(id=1, name="test")]
        df = spark.createDataFrame(test_data)
        df.write.format("delta").mode("overwrite").save(str(delta_path))

        result = spark.read.format("delta").load(delta_table_path).collect()
        assert result == [Row(id=1, name="test")]
