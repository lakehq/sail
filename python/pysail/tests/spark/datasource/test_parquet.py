from collections.abc import Mapping

import pandas as pd
from pandas.testing import assert_frame_equal

from pysail.testing.spark.utils.files import get_data_directory_size
from pysail.testing.spark.utils.sql import escape_sql_identifier


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


def test_parquet_read_write_basic(spark, sample_df, tmp_path):
    path = str(tmp_path / "parquet_basic")
    sample_df.write.parquet(path, mode="overwrite")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_parquet_read_write_compressed(spark, sample_df, sample_pandas_df, tmp_path):
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


def test_parquet_write_options(spark, sample_df, tmp_path):
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


def test_parquet_read_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "parquet_read_options")
    sample_df.write.parquet(path, mode="overwrite")
    read_df = spark.read.option("binaryAsString", "false").option("pruning", "true").parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_parquet_write_with_bloom_filter(spark, tmpdir):
    def size(p):
        return get_data_directory_size(p, extension=".parquet")

    # The bloom filter size is determined by a formula of FPP and NDV,
    # and then rounded up to the nearest power of two.
    # When the data is small, the bloom filter dominates the file size if enabled,
    # and one bloom filter is created for each column.
    # The file size in the assertions below are rough estimates.

    path = str(tmpdir / "default")
    spark.sql("SELECT 1").write.parquet(path)
    # The Parquet file without bloom filter is small (less than 1 kB).
    assert size(path) < 1024  # noqa: PLR2004

    path = str(tmpdir / "bloom_filter_off_explicit")
    (
        spark.sql("SELECT 1")
        .write.option("bloom_filter_on_write", "false")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert size(path) < 1024  # noqa: PLR2004

    path = str(tmpdir / "bloom_filter_off_implicit")
    (
        spark.sql("SELECT 1")
        # The default configuration does not enable bloom filters on write.
        .write.option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert size(path) < 1024  # noqa: PLR2004

    path = str(tmpdir / "bloom_filter_on")
    (
        spark.sql("SELECT 1")
        .write.option("bloom_filter_on_write", "true")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert 16384 < size(path) < 16384 + 1024  # noqa: PLR2004

    path = str(tmpdir / "bloom_filter_on_with_multiple_columns")
    (
        spark.sql("SELECT 1, 2")
        .write.option("bloom_filter_on_write", "true")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    assert 32768 < size(path) < 32768 + 1024  # noqa: PLR2004


def test_parquet_write_with_path_option(spark, tmpdir):
    """Test that df.write.format("parquet").option("path", path).save() works (issue #811)."""
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, schema="id INT, name STRING")

    path = str(tmpdir / "output")
    df.write.format("parquet").option("path", path).save()

    actual = spark.read.parquet(path).orderBy("id").toPandas()
    expected = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}).astype({"id": "int32"})
    assert_frame_equal(actual, expected)


def test_parquet_format_path(spark, sample_df, tmp_path):
    path = str(tmp_path / "data.parquet")
    sample_df.write.parquet(path, mode="overwrite")
    df = spark.sql(f"SELECT * FROM parquet.`{escape_sql_identifier(path)}`")  # noqa: S608
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)
