from pysail.tests.spark.utils import get_data_directory_size


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
