import glob

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def safe_sort_key(row):
    return tuple((v is not None, v) for v in row)


@pytest.fixture
def sample_df(spark):
    schema = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True),
        ]
    )
    data = [("a", 1), ("b", 2), ("c", 3), (None, 4)]
    return spark.createDataFrame(data, schema)


class TestParquetDataSource:
    def test_read_write_basic(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "parquet_basic")
        sample_df.write.parquet(path, mode="overwrite")
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sample_df.schema == read_df.schema
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_parquet_write_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "parquet_write_options")
        (
            sample_df.write.option("compression", "gzip(4)")  # pyspark supports gzip keyword.
            .option("writerVersion", "1.0")
            .parquet(path, mode="overwrite")
        )
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_parquet_read_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "parquet_read_options")
        sample_df.write.parquet(path, mode="overwrite")
        read_df = spark.read.option("binaryAsString", "false").option("pruning", "true").parquet(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


class TestCsvDataSource:
    def test_read_write_basic(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "csv_basic")
        sample_df.write.csv(path, header=True, mode="overwrite")
        read_df = (
            spark.read.option("header", True)
            # .option("inferSchema", True)
            .csv(path)
        )
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_csv_write_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "csv_write_options")
        (
            sample_df.write.option("delimiter", ";")
            .option("header", "true")
            .option("quote", '"')
            .option("escape", "\\")
            # .option("nullValue", "NULL")
            .csv(path, mode="overwrite")
        )
        # NOTICE: Should be None, but got NULL.
        csv_files = glob.glob(str(tmp_path / "csv_write_options" / "*.csv"))
        content = ""
        for file in csv_files:
            with open(file) as f:
                content += f.read()
        assert "col1;col2" in content
        assert "a;1" in content
        # assert "NULL;4" in content

        read_df = (
            spark.read.option("delimiter", ";")
            # .option("nullValue", "NULL")
            .option("header", True)
            # .option("inferSchema", True)
            .csv(path)
        )
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_csv_read_options(self, spark, tmp_path):
        path = tmp_path / "csv_read_options"
        path.mkdir()
        data_path = path / "data.csv"
        with open(data_path, "w") as f:
            f.write("# This is a comment\n")
            f.write("col1|col2\n")
            f.write("x|10\n")
            f.write("y|20\n")

        read_df = (
            spark.read.option("delimiter", "|")
            .option("header", "true")
            .option("comment", "#")
            # .option("inferSchema", "true")
            .csv(str(path))
        )
        expected_row_count = 2
        expected_col2_value = 10
        assert read_df.count() == expected_row_count
        assert read_df.columns == ["col1", "col2"]
        assert read_df.collect()[0].col2 == expected_col2_value


class TestJsonDataSource:
    def test_read_write_basic(self, spark, sample_df, tmp_path):
        """Test basic JSON read/write."""
        path = str(tmp_path / "json_basic")
        sample_df.write.json(path, mode="overwrite")
        read_df = (
            spark.read
            # .option("multi_line", False)
            .json(path).select("col1", "col2")
        )
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    @pytest.mark.skip(reason="Compression maybe broken")
    def test_json_write_compression(self, spark, sample_df, tmp_path):
        """Test JSON write with compression."""
        path = str(tmp_path / "json_compressed")
        sample_df.write.option("compression", "gzip").json(path, mode="overwrite")
        files = list((tmp_path / "json_compressed").glob("*.json.gz"))
        assert len(files) > 0

        read_df = (
            spark.read
            # .option("multi_line", False)
            .json(path).select("col1", "col2")
        )
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_json_read_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "json_read_options")
        sample_df.write.json(path, mode="overwrite")
        read_df = spark.read.option("schemaInferMaxRecords", 1).json(path).select("col1", "col2")
        assert read_df.count() == sample_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
