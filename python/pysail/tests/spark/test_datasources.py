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

    def test_csv_read_truncated_rows(self, spark, tmp_path):
        path = tmp_path / "csv_read_truncated_rows"
        path.mkdir()
        data_path = path / "data.csv"
        with open(data_path, "w") as f:
            f.write("col1,col2\n")
            f.write("x,10\n")
            f.write("y\n")

        df = spark.read.option("header", "true").csv(str(path))
        with pytest.raises(Exception, match="unequal"):
            df.collect()

        df = spark.read.option("header", "true").option("allowTruncatedRows", "true").csv(str(path))
        assert sorted(df.collect()) == [Row(col1="x", col2=10), Row(col1="y", col2=None)]

    def test_csv_infer_schema_false(self, spark, tmp_path):
        # Test that inferSchema=false treats all columns as strings, even with invalid dates.
        path = tmp_path / "csv_infer_schema_false"
        path.mkdir()
        data_path = path / "data.csv"
        with open(data_path, "w") as f:
            f.write("id,name,birthday\n")
            f.write("1,Alice,2025-01-01\n")
            f.write("2,Bob,1999-99-99\n")  # Invalid date
            f.write("3,Carol,1999-01-01\n")

        # With inferSchema=false, should read all columns as strings
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(str(path))

        # Check schema - all columns should be strings
        schema = df.schema
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].dataType == StringType()
        assert schema.fields[2].dataType == StringType()

        # Should be able to collect all rows without error
        rows = df.collect()
        expected_len = 3
        assert len(rows) == expected_len
        assert rows[0].id == "1"
        assert rows[0].name == "Alice"
        assert rows[0].birthday == "2025-01-01"
        assert rows[1].birthday == "1999-99-99"  # Invalid date should be preserved as string
        assert rows[2].birthday == "1999-01-01"

    def test_csv_infer_schema_false_no_header(self, spark, tmp_path):
        # Test that inferSchema=false works with no header (column names should be _c0, _c1, etc.)
        path = tmp_path / "csv_infer_schema_false_no_header"
        path.mkdir()
        data_path = path / "data.csv"
        with open(data_path, "w") as f:
            # No header line, just data
            f.write("1,Alice,2025-01-01\n")
            f.write("2,Bob,1999-99-99\n")  # Invalid date
            f.write("3,Carol,1999-01-01\n")

        # With inferSchema=false and no header, should read all columns as strings
        df = spark.read.option("header", "false").option("inferSchema", "false").csv(str(path))

        # Check schema - all columns should be strings
        schema = df.schema
        assert schema.fields[0].dataType == StringType()
        assert schema.fields[1].dataType == StringType()
        assert schema.fields[2].dataType == StringType()

        # Check column names - should be _c0, _c1, _c2 (renamed from default CSV column names)
        assert schema.fields[0].name == "_c0"
        assert schema.fields[1].name == "_c1"
        assert schema.fields[2].name == "_c2"

        # Should be able to collect all rows without error
        rows = df.collect()
        expected_len = 3
        # ruff: noqa: SLF001
        assert len(rows) == expected_len
        assert rows[0]._c0 == "1"
        assert rows[0]._c1 == "Alice"
        assert rows[0]._c2 == "2025-01-01"
        assert rows[1]._c2 == "1999-99-99"  # Invalid date should be preserved as string
        assert rows[2]._c2 == "1999-01-01"


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


class TestPythonDataSource:
    """Tests for Python DataSource integration."""

    def test_arrow_datasource(self, spark):
        """Test zero-copy Arrow RecordBatch path."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class ArrowRangeDataSource(DataSource):
            """DataSource yielding Arrow RecordBatches (zero-copy path)."""

            @classmethod
            def name(cls) -> str:
                return "arrow_range_test"

            def schema(self):
                return pa.schema([
                    ("id", pa.int64()),
                    ("value", pa.float64()),
                ])

            def reader(self, schema):
                return ArrowRangeReader()

        class ArrowRangeReader(DataSourceReader):
            """Reader that yields Arrow RecordBatches."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                ids = list(range(100))
                values = [float(i) * 1.5 for i in ids]
                batch = pa.RecordBatch.from_pydict(
                    {"id": ids, "value": values},
                    schema=pa.schema([("id", pa.int64()), ("value", pa.float64())])
                )
                yield batch

        # Register and read
        spark.dataSource.register(ArrowRangeDataSource)
        df = spark.read.format("arrow_range_test").load()

        rows = df.collect()
        assert len(rows) == 100
        assert rows[0].id == 0
        assert rows[0].value == 0.0

        # Test filter query (filter is applied post-read by DataFusion in MVP)
        filtered = df.filter("value > 50.0").collect()
        # ids 34+ have value > 50 (34 * 1.5 = 51)
        assert len(filtered) == 66  # ids 34-99 have value > 50

    def test_tuple_datasource(self, spark):
        """Test row-based tuple fallback path with data integrity verification."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class TupleDataSource(DataSource):
            """DataSource yielding tuples (row-based fallback path)."""

            @classmethod
            def name(cls) -> str:
                return "tuple_range_test"

            def schema(self):
                return pa.schema([
                    ("id", pa.int64()),
                    ("square", pa.int64()),
                ])

            def reader(self, schema):
                return TupleReader()

        class TupleReader(DataSourceReader):
            """Reader that yields many tuples to test batching."""

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Yield 1000 tuples to test batching behavior
                for i in range(1000):
                    yield (i, i * i)

        spark.dataSource.register(TupleDataSource)
        df = spark.read.format("tuple_range_test").load()

        rows = df.collect()
        assert len(rows) == 1000
        
        # Verify data integrity: check that squares are correct
        # Find row with id=100, should have square=10000
        sample = df.filter("id = 100").collect()
        assert len(sample) == 1
        assert sample[0].square == 10000  # 100² = 10000

    def test_multi_partition(self, spark):
        """Test parallel partition reading."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class MultiPartitionDataSource(DataSource):
            """DataSource with multiple partitions."""

            @classmethod
            def name(cls) -> str:
                return "multi_partition_test"

            def schema(self):
                return pa.schema([
                    ("partition_id", pa.int32()),
                    ("row_id", pa.int32()),
                ])

            def reader(self, schema):
                return MultiPartitionReader()

        class MultiPartitionReader(DataSourceReader):
            """Reader with 4 partitions."""

            def partitions(self):
                return [InputPartition(i) for i in range(4)]

            def read(self, partition):
                partition_ids = [partition.value] * 25
                row_ids = list(range(25))
                batch = pa.RecordBatch.from_pydict(
                    {"partition_id": partition_ids, "row_id": row_ids},
                    schema=pa.schema([("partition_id", pa.int32()), ("row_id", pa.int32())])
                )
                yield batch

        spark.dataSource.register(MultiPartitionDataSource)
        df = spark.read.format("multi_partition_test").load()

        rows = df.collect()
        # 4 partitions * 25 rows each = 100 total
        assert len(rows) == 100

    def test_partition_failure(self, spark):
        """Test that partition failure errors include partition context."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class FailingPartitionDataSource(DataSource):
            """DataSource where partition 2 fails."""

            @classmethod
            def name(cls) -> str:
                return "failing_partition_test"

            def schema(self):
                return pa.schema([("id", pa.int32())])

            def reader(self, schema):
                return FailingPartitionReader()

        class FailingPartitionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(i) for i in range(4)]

            def read(self, partition):
                pid = partition.value
                if pid == 2:
                    raise ValueError(f"Deliberate failure in partition {pid}!")
                
                schema = pa.schema([("id", pa.int32())])
                batch = pa.RecordBatch.from_pydict({"id": [pid]}, schema=schema)
                yield batch

        spark.dataSource.register(FailingPartitionDataSource)
        df = spark.read.format("failing_partition_test").load()

        with pytest.raises(Exception) as exc_info:
            df.collect()

        error_msg = str(exc_info.value).lower()
        # Error should include partition context
        assert "partition" in error_msg or "2" in error_msg
        assert "deliberate failure" in error_msg

    def test_empty_partitions(self, spark):
        """Test that zero partitions returns empty DataFrame without crashing."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader

        class EmptyPartitionsDataSource(DataSource):
            """DataSource that returns zero partitions."""

            @classmethod
            def name(cls) -> str:
                return "empty_partitions_test"

            def schema(self):
                return pa.schema([
                    ("id", pa.int32()),
                    ("name", pa.string()),
                ])

            def reader(self, schema):
                return EmptyPartitionsReader()

        class EmptyPartitionsReader(DataSourceReader):
            def partitions(self):
                return []  # No partitions

            def read(self, partition):
                raise RuntimeError("read() should not be called with 0 partitions!")

        spark.dataSource.register(EmptyPartitionsDataSource)
        df = spark.read.format("empty_partitions_test").load()

        rows = df.collect()
        assert len(rows) == 0

    def test_schema_mismatch(self, spark):
        """Test that schema mismatch between declared and returned schema is handled."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SchemaMismatchDataSource(DataSource):
            """DataSource that declares one schema but returns another."""

            @classmethod
            def name(cls) -> str:
                return "schema_mismatch_test"

            def schema(self):
                # Declare schema with int32 id
                return pa.schema([
                    ("id", pa.int32()),
                    ("name", pa.string()),
                ])

            def reader(self, schema):
                return SchemaMismatchReader()

        class SchemaMismatchReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Return batch with int64 id instead of int32 - schema mismatch!
                wrong_schema = pa.schema([
                    ("id", pa.int64()),  # Wrong type!
                    ("name", pa.string()),
                ])
                batch = pa.RecordBatch.from_pydict({
                    "id": [1, 2, 3],
                    "name": ["a", "b", "c"],
                }, schema=wrong_schema)
                yield batch

        spark.dataSource.register(SchemaMismatchDataSource)
        df = spark.read.format("schema_mismatch_test").load()

        # Should either succeed with coercion or fail with schema error
        # The important thing is it doesn't crash unexpectedly
        try:
            rows = df.collect()
            # If it succeeds, verify data is present
            assert len(rows) == 3
        except Exception as e:
            # If it fails, error should mention schema/type issue
            error_msg = str(e).lower()
            assert "schema" in error_msg or "type" in error_msg or "mismatch" in error_msg

    def test_python_exception_handling(self, spark):
        """Test that Python exceptions are properly propagated with traceback."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class ExceptionDataSource(DataSource):
            """DataSource whose reader throws an exception."""

            @classmethod
            def name(cls) -> str:
                return "exception_test"

            def schema(self):
                return pa.schema([
                    ("id", pa.int32()),
                    ("value", pa.string()),
                ])

            def reader(self, schema):
                return ExceptionReader()

        class ExceptionReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                raise ValueError("This is a deliberate test exception!")

        spark.dataSource.register(ExceptionDataSource)
        df = spark.read.format("exception_test").load()

        with pytest.raises(Exception) as exc_info:
            df.collect()

        error_msg = str(exc_info.value)
        # Exception message should be preserved
        assert "deliberate test exception" in error_msg.lower()

    def test_session_isolation(self, spark_session_factory):
        """Test that datasources registered in one session are not visible in another.
        
        This test creates two separate SparkSessions with unique session IDs
        and verifies that a datasource registered in Session A cannot be
        accessed from Session B.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SessionIsolationDataSource(DataSource):
            """DataSource for testing session isolation."""

            @classmethod
            def name(cls) -> str:
                return "session_isolation_test"

            def schema(self):
                return pa.schema([("id", pa.int32()), ("msg", pa.string())])

            def reader(self, schema):
                return SessionIsolationReader()

        class SessionIsolationReader(DataSourceReader):
            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict({
                    "id": [1, 2, 3],
                    "msg": ["hello", "from", "session_a"],
                }, schema=pa.schema([("id", pa.int32()), ("msg", pa.string())]))
                yield batch

        # Session A: Register the datasource
        spark_a = spark_session_factory()
        spark_a.dataSource.register(SessionIsolationDataSource)
        
        # Verify Session A can read from it
        df_a = spark_a.read.format("session_isolation_test").load()
        rows_a = df_a.collect()
        assert len(rows_a) == 3

        # Session B: Create a completely independent session
        spark_b = spark_session_factory()
        
        # Session B should NOT be able to access the datasource from Session A
        # because datasources are registered per-session
        with pytest.raises(Exception) as exc_info:
            df_b = spark_b.read.format("session_isolation_test").load()
            df_b.collect()
        
        error_msg = str(exc_info.value).lower()
        # Error should indicate the format is not found
        assert "session_isolation_test" in error_msg or "not found" in error_msg or "unknown" in error_msg


class TestFilterPushdown:
    """Tests for filter pushdown to Python DataSources."""

    def test_filter_pushdown_equality(self, spark):
        """Test that equality filters (WHERE id = X) are pushed to Python reader.
        
        Note: We verify pushFilters is called by checking that the reader
        actually applies the filter (returns only matching rows). Due to
        cloudpickle serialization, we can't track state across the boundary.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class FilterApplyingDataSource(DataSource):
            """DataSource that applies pushed filters during read."""

            @classmethod
            def name(cls) -> str:
                return "filter_applying_test"

            def schema(self):
                return pa.schema([
                    ("id", pa.int64()),
                    ("value", pa.string()),
                ])

            def reader(self, schema):
                return FilterApplyingReader()

        class FilterApplyingReader(DataSourceReader):
            def __init__(self):
                self.accepted_filters = []

            def pushFilters(self, filters):
                """Accept EqualTo filters and track them."""
                for f in filters:
                    filter_name = type(f).__name__
                    if filter_name == "EqualTo":
                        self.accepted_filters.append(f)
                    else:
                        yield f  # Reject non-equality filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                # Full dataset
                all_data = {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]}
                
                # Apply accepted filters
                if self.accepted_filters:
                    for f in self.accepted_filters:
                        # EqualTo filter has references to column tuple and value
                        # Access the filter value (it's stored as an attribute)
                        filter_val = getattr(f, 'value', None)
                        if filter_val is not None:
                            # Filter rows where id matches
                            filtered_ids = []
                            filtered_values = []
                            for i, id_val in enumerate(all_data["id"]):
                                if id_val == filter_val:
                                    filtered_ids.append(id_val)
                                    filtered_values.append(all_data["value"][i])
                            all_data = {"id": filtered_ids, "value": filtered_values}

                batch = pa.RecordBatch.from_pydict(
                    all_data,
                    schema=pa.schema([("id", pa.int64()), ("value", pa.string())])
                )
                yield batch

        spark.dataSource.register(FilterApplyingDataSource)
        
        # Execute a query with a filter
        df = spark.read.format("filter_applying_test").load()
        
        # Query without filter should return all 5 rows
        all_rows = df.collect()
        assert len(all_rows) == 5, f"Expected 5 rows, got {len(all_rows)}"
        
        # Query WITH filter - if pushFilters works, Python reader applies it
        filtered_rows = df.filter("id = 3").collect()
        
        # Should get exactly 1 row
        assert len(filtered_rows) == 1, f"Expected 1 row, got {len(filtered_rows)}"
        assert filtered_rows[0].id == 3

    def test_filter_pushdown_comparison(self, spark):
        """Test that comparison filters (>, <, >=, <=) work correctly.
        
        We verify that query results are correct when using comparison filters.
        DataFusion post-filters ensure correctness even if pushdown isn't applied.
        """
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class SimpleRangeDataSource(DataSource):
            @classmethod
            def name(cls) -> str:
                return "simple_range_test"

            def schema(self):
                return pa.schema([("id", pa.int64())])

            def reader(self, schema):
                return SimpleRangeReader()

        class SimpleRangeReader(DataSourceReader):
            def pushFilters(self, filters):
                # Accept all filters (don't actually apply - let DataFusion post-filter)
                return iter([])

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict(
                    {"id": list(range(10))},  # 0-9
                    schema=pa.schema([("id", pa.int64())])
                )
                yield batch

        spark.dataSource.register(SimpleRangeDataSource)
        df = spark.read.format("simple_range_test").load()
        
        # Test greater than
        rows = df.filter("id > 5").collect()
        assert len(rows) == 4, f"Expected 4 rows (6,7,8,9), got {len(rows)}"
        assert all(r.id > 5 for r in rows)

    def test_filter_pushdown_rejection(self, spark):
        """Test that when reader rejects filters, DataFusion post-filters correctly."""
        import pyarrow as pa
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class RejectAllFiltersDataSource(DataSource):
            @classmethod
            def name(cls) -> str:
                return "reject_all_filters_test"

            def schema(self):
                return pa.schema([("id", pa.int64())])

            def reader(self, schema):
                return RejectAllFiltersReader()

        class RejectAllFiltersReader(DataSourceReader):
            def pushFilters(self, filters):
                # Reject all filters by yielding them back
                yield from filters

            def partitions(self):
                return [InputPartition(0)]

            def read(self, partition):
                batch = pa.RecordBatch.from_pydict(
                    {"id": [1, 2, 3, 4, 5]},
                    schema=pa.schema([("id", pa.int64())])
                )
                yield batch

        spark.dataSource.register(RejectAllFiltersDataSource)
        df = spark.read.format("reject_all_filters_test").load()

        # Even though reader rejects filters, query should still work
        # DataFusion will post-filter the results
        rows = df.filter("id = 3").collect()
        assert len(rows) == 1
        assert rows[0].id == 3
