from pysail.testing.spark.utils.sql import escape_sql_identifier


def safe_sort_key(row):
    return tuple((v is not None, v) for v in row)


def test_json_read_write_basic(spark, sample_df, tmp_path):
    """Test basic JSON read/write."""
    path = str(tmp_path / "json_basic")
    sample_df.write.json(path, mode="overwrite")
    read_df = spark.read.json(path).select("col1", "col2")
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_json_write_compression(spark, sample_df, tmp_path):
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


def test_json_read_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "json_read_options")
    sample_df.write.json(path, mode="overwrite")
    read_df = spark.read.option("schemaInferMaxRecords", 1).json(path).select("col1", "col2")
    assert read_df.count() == sample_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_json_format_path(spark, tmp_path):
    json_file = tmp_path / "data.json"
    json_file.write_text('{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}\n')
    df = spark.sql(f"SELECT * FROM json.`{escape_sql_identifier(str(json_file))}`")  # noqa: S608
    assert df.count() == 2  # noqa: PLR2004
    assert "id" in df.columns
    assert "value" in df.columns
