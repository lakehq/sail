import glob
import io

import numpy as np
import pandas as pd
import pytest
from PIL import Image
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
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


@pytest.fixture
def sample_pandas_df():
    data = [("a", 1), ("b", 2), ("c", 3), (None, 4)]
    columns = ["col1", "col2"]
    return pd.DataFrame(data, columns=columns).astype(
        {
            "col1": "string",
            "col2": "Int32",
        }
    )


class TestParquetDataSource:
    def test_read_write_basic(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "parquet_basic")
        sample_df.write.parquet(path, mode="overwrite")
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sample_df.schema == read_df.schema
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_read_write_compressed(self, spark, sample_df, sample_pandas_df, tmp_path):
        # Test reading a compressed Parquet file written by Sail
        path = str(tmp_path / "parquet_compressed_zstd")
        sample_df.write.option("compression", "zstd(4)").parquet(path, mode="overwrite")
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "parquet_compressed_zstd").glob("*.zst.parquet"))) > 0

        # Test reading a compressed Parquet file written by Pandas.
        path = tmp_path / "parquet_compressed_gzip_pandas_1"
        path.mkdir()
        path = str(path)
        sample_pandas_df.to_parquet(f"{path}/sample_pandas_df.parquet", compression="gzip")
        read_df = spark.read.parquet(path)
        assert len(sample_pandas_df) == read_df.count()
        assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
            read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
        )
        assert len(list((tmp_path / "parquet_compressed_gzip_pandas_1").glob("*.parquet"))) > 0

        # Test reading a compressed Parquet file written by Pandas with `.gz` in the filename.
        path = tmp_path / "parquet_compressed_gzip_pandas_2"
        path.mkdir()
        path = str(path)
        sample_pandas_df.to_parquet(f"{path}/sample_pandas_df.gz.parquet", compression="gzip")
        read_df = spark.read.parquet(path)
        assert len(sample_pandas_df) == read_df.count()
        assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
            read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
        )
        assert len(list((tmp_path / "parquet_compressed_gzip_pandas_2").glob("*.gz.parquet"))) > 0

    def test_parquet_write_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "parquet_write_options")
        sample_df.write.option("writerVersion", "1.0").parquet(path, mode="overwrite", compression="gzip(4)")
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "parquet_write_options").glob("*.gz.parquet"))) > 0

        path = str(tmp_path / "parquet_write_options_1")
        sample_df.write.option("writerVersion", "1.0").parquet(path, mode="overwrite", compression="snappy")
        read_df = spark.read.parquet(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "parquet_write_options_1").glob("*.snappy.parquet"))) > 0

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

    def test_read_write_compressed(self, spark, sample_df, sample_pandas_df, tmp_path):
        # Test reading a compressed CSV file written by Sail
        path = str(tmp_path / "csv_compressed_gzip")
        sample_df.write.option("header", "true").option("compression", "gzip").csv(path, mode="overwrite")
        read_df = spark.read.format("csv").option("header", "true").option("compression", "gzip").load(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "csv_compressed_gzip").glob("*.csv.gz"))) > 0

        # Compression type not explicitly set.
        read_df = spark.read.format("csv").option("header", "true").load(path)
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "csv_compressed_gzip").glob("*.csv.gz"))) > 0

        # Test reading a compressed CSV file written by Pandas.
        path = tmp_path / "csv_compressed_gzip_pandas_1"
        path.mkdir()
        path = str(path)
        sample_pandas_df.to_csv(f"{path}/sample_pandas_df.csv.gz", index=False, compression="gzip")
        read_df = spark.read.format("csv").option("header", "true").option("compression", "gzip").load(path)
        assert len(sample_pandas_df) == read_df.count()
        assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
            read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
        )
        assert len(list((tmp_path / "csv_compressed_gzip_pandas_1").glob("*.csv.gz"))) > 0

        # Test reading a compressed CSV file written by Pandas with `.gz` in the filename.
        path = tmp_path / "csv_compressed_gzip_pandas_2"
        path.mkdir()
        path = str(path)
        sample_pandas_df.to_csv(f"{path}/sample_pandas_df.gz.csv", index=False, compression="gzip")
        read_df = spark.read.format("csv").option("header", "true").option("compression", "gzip").load(path)
        assert len(sample_pandas_df) == read_df.count()
        assert sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key) == sorted(
            read_df.toPandas().to_dict(orient="records"), key=safe_sort_key
        )
        assert len(list((tmp_path / "csv_compressed_gzip_pandas_2").glob("*.gz.csv"))) > 0

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
        read_df = spark.read.json(path).select("col1", "col2")
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_json_write_compression(self, spark, sample_df, tmp_path):
        """Test JSON write with compression."""
        path = str(tmp_path / "json_compressed")
        sample_df.write.option("compression", "gzip").json(path, mode="overwrite")
        files = list((tmp_path / "json_compressed").glob("*.json.gz"))
        assert len(files) > 0

        read_df = spark.read.format("json").option("compression", "gzip").load(path).select("col1", "col2")
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

        # Compression type not explicitly set.
        read_df = spark.read.format("json").load(path).select("col1", "col2")
        assert sample_df.count() == read_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_json_read_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "json_read_options")
        sample_df.write.json(path, mode="overwrite")
        read_df = spark.read.option("schemaInferMaxRecords", 1).json(path).select("col1", "col2")
        assert read_df.count() == sample_df.count()
        assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


class TestTextDataSource:
    def test_read_write_basic(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "text_basic")
        sample_df = sample_df.select("col1")
        sample_df.write.text(path)

        read_df = spark.read.text(path, wholetext=False)
        new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
        new_df = spark.createDataFrame(new_rows)
        assert new_df.count() == read_df.count()
        assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

        read_df = spark.read.text(path, wholetext=True)
        joined = "\n".join([r["col1"] if r["col1"] is not None else "\n" for r in sample_df.collect()])
        joined_df = spark.createDataFrame([Row(value=joined)])
        assert joined_df.count() == read_df.count()
        assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_read_write_compressed(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "text_compressed_gzip")
        sample_df = sample_df.select("col1")
        sample_df.write.option("compression", "gzip").text(path)

        read_df = spark.read.format("text").option("whole_text", False).load(path)
        new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
        new_df = spark.createDataFrame(new_rows)
        assert new_df.count() == read_df.count()
        assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
        assert len(list((tmp_path / "text_compressed_gzip").glob("*.txt.gz"))) > 0

        read_df = spark.read.format("text").option("whole_text", True).load(path)
        joined = "\n".join([r["col1"] if r["col1"] is not None else "\n" for r in sample_df.collect()])
        joined_df = spark.createDataFrame([Row(value=joined)])
        assert joined_df.count() == read_df.count()
        assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_text_write_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "text_write_options")
        sample_df = sample_df.select("col1")
        sample_df.write.option("line_sep", ";").option("compression", "gzip").text(path)

        read_df = spark.read.option("line_sep", ";").text(path)
        new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
        new_df = spark.createDataFrame(new_rows)
        assert new_df.count() == read_df.count()
        assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_text_read_options(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "text_write_options")
        sample_df = sample_df.select("col1")
        sample_df.write.option("line_sep", ";").option("compression", "gzip").text(path)

        read_df = spark.read.format("text").option("line_sep", ";").option("whole_text", False).load(path)
        new_rows = [Row(value=(r["col1"] if r["col1"] is not None else "")) for r in sample_df.collect()]
        new_df = spark.createDataFrame(new_rows)
        assert new_df.count() == read_df.count()
        assert sorted(new_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

        read_df = spark.read.format("text").option("line_sep", ";").option("whole_text", True).load(path)
        joined = ";".join([r["col1"] if r["col1"] is not None else ";" for r in sample_df.collect()])
        joined_df = spark.createDataFrame([Row(value=joined)])
        assert joined_df.count() == read_df.count()
        assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

        read_df = spark.read.text(path, wholetext=False)
        assert joined_df.count() == read_df.count()
        assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

        read_df = spark.read.text(path, wholetext=True)
        assert joined_df.count() == read_df.count()
        assert sorted(joined_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)

    def test_read_projections(self, spark, sample_df, tmp_path):
        path = str(tmp_path / "text_projections")
        sample_df = sample_df.select("col1")
        sample_df.write.text(path)

        read_df = spark.read.text(path)

        projected_df = read_df.select("value")
        assert projected_df.columns == ["value"]
        assert projected_df.count() == sample_df.count()

        count = read_df.select("value").count()
        assert count == sample_df.count()

        values = [r.value for r in projected_df.collect()]
        expected = [r["col1"] if r["col1"] is not None else "" for r in sample_df.collect()]
        assert sorted(values) == sorted(expected)


class TestBinaryDataSource:
    @staticmethod
    def _create_test_files(tmp_path):
        files = {}

        png_path = tmp_path / "test.png"
        png_array = np.zeros((10, 10, 3), dtype=np.uint8)
        png_array[:, :, 0] = np.linspace(0, 255, 10).astype(np.uint8).reshape(1, 10)
        png_array[:, :, 1] = np.linspace(0, 255, 10).astype(np.uint8).reshape(10, 1)
        png_array[:, :, 2] = 128
        png_img = Image.fromarray(png_array)
        png_buffer = io.BytesIO()
        png_img.save(png_buffer, format="PNG")
        png_content = png_buffer.getvalue()
        png_path.write_bytes(png_content)
        files["png"] = (png_path, png_content)

        jpeg_path = tmp_path / "test.jpeg"
        jpeg_array = np.random.randint(0, 256, (20, 20), dtype=np.uint8)
        jpeg_img = Image.fromarray(jpeg_array)
        jpeg_buffer = io.BytesIO()
        jpeg_img.save(jpeg_buffer, format="JPEG", quality=85)
        jpeg_content = jpeg_buffer.getvalue()
        jpeg_path.write_bytes(jpeg_content)
        files["jpeg"] = (jpeg_path, jpeg_content)

        pdf_path = tmp_path / "test.pdf"
        pdf_content = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\nxref\n0 3\n0000000000 65535 f\n0000000009 00000 n\n0000000058 00000 n\ntrailer\n<< /Size 3 /Root 1 0 R >>\nstartxref\n115\n%%EOF"
        pdf_path.write_bytes(pdf_content)
        files["pdf"] = (pdf_path, pdf_content)

        bin_path = tmp_path / "data.bin"
        bin_array = np.random.randint(0, 256, 256, dtype=np.uint8)
        bin_content = bin_array.tobytes()
        bin_path.write_bytes(bin_content)
        files["bin"] = (bin_path, bin_content)

        return files

    def test_read_basic(self, spark, tmp_path):
        files = TestBinaryDataSource._create_test_files(tmp_path)

        read_df = spark.read.format("binaryFile").load(str(tmp_path))
        assert read_df.columns == ["path", "modificationTime", "length", "content"]
        assert read_df.count() == len(files)

        rows = read_df.collect()
        for row in rows:
            filename = row.path.split("/")[-1]
            for file_path, expected_content in files.values():
                if file_path.name == filename:
                    assert row.content == expected_content
                    assert row.length == len(expected_content)
                    break

    def test_binary_read_options(self, spark, tmp_path):
        (tmp_path / "image1.png").write_bytes(b"PNG1")
        (tmp_path / "image2.png").write_bytes(b"PNG2")
        (tmp_path / "document.pdf").write_bytes(b"PDF")
        (tmp_path / "photo.jpeg").write_bytes(b"JPEG")
        (tmp_path / "data.bin").write_bytes(b"BIN")
        (tmp_path / "test1.dat").write_bytes(b"DAT1")
        (tmp_path / "test2.dat").write_bytes(b"DAT2")

        png_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load(str(tmp_path))
        expected_png_count = 2
        assert png_df.count() == expected_png_count
        paths = [row.path for row in png_df.collect()]
        assert all("png" in p for p in paths)

        test_df = spark.read.format("binaryFile").option("pathGlobFilter", "test*").load(str(tmp_path))
        expected_test_count = 2
        assert test_df.count() == expected_test_count
        paths = [row.path for row in test_df.collect()]
        assert all("test" in p for p in paths)

        dat_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.dat").load(str(tmp_path))
        expected_dat_count = 2
        assert dat_df.count() == expected_dat_count

        single_char_df = spark.read.format("binaryFile").option("pathGlobFilter", "image?.png").load(str(tmp_path))
        expected_single_count = 2
        assert single_char_df.count() == expected_single_count

        all_df = spark.read.format("binaryFile").load(str(tmp_path))
        expected_all_count = 7
        assert all_df.count() == expected_all_count

    def test_read_projections(self, spark, tmp_path):
        files = TestBinaryDataSource._create_test_files(tmp_path)

        read_df = spark.read.format("binaryFile").load(str(tmp_path))

        metadata_df = read_df.select("path", "length")
        assert metadata_df.columns == ["path", "length"]
        assert metadata_df.count() == len(files)

        rows = metadata_df.collect()
        for row in rows:
            assert hasattr(row, "path")
            assert hasattr(row, "length")
            assert not hasattr(row, "content")

        content_df = read_df.select("content")
        assert content_df.columns == ["content"]
        assert content_df.count() == len(files)

        reordered_df = read_df.select("length", "path", "modificationTime")
        assert reordered_df.columns == ["length", "path", "modificationTime"]

        count = read_df.select("path").count()
        assert count == len(files)

        total_size = read_df.agg(F.sum("length").alias("total_bytes")).collect()[0].total_bytes
        expected_size = sum(len(content) for _, content in files.values())
        assert total_size == expected_size

        min_file_size = 100
        large_files = read_df.filter(read_df.length > min_file_size).select("path", "length")
        large_count = large_files.count()
        expected_large = sum(1 for _, content in files.values() if len(content) > min_file_size)
        assert large_count == expected_large
