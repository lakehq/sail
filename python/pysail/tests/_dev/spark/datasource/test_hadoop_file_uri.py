# SPDX-License-Identifier: Apache-2.0

# A Hadoop URI without an authority, such as `file:/tmp/x`, names an absolute path.

import os

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

ROWS = [(1, "a"), (2, "b"), (3, "c")]
SCHEMA = "a INT, b STRING"


def _assert_no_local_scheme_dir(*schemes):
    for scheme in schemes:
        assert not os.path.exists(os.path.join(os.getcwd(), f"{scheme}:"))


@pytest.fixture
def source(spark, tmp_path):
    path = str(tmp_path / "source")
    df = spark.createDataFrame(ROWS, SCHEMA)
    for data_format in ["parquet", "csv", "json"]:
        df.write.format(data_format).save(f"{path}/{data_format}")
    df.selectExpr("b").write.text(f"{path}/text")
    return path


@pytest.mark.parametrize("data_format", ["parquet", "csv", "json"])
def test_read(spark, source, data_format):
    path = f"{source}/{data_format}"
    for spelling in [f"file:{path}", f"FILE:{path}", f"file:{path}/"]:
        df = spark.read.format(data_format).schema(SCHEMA).load(spelling)
        assert sorted(df.collect()) == sorted(ROWS), spelling
    df = spark.sql(f"SELECT * FROM {data_format}.`file:{path}`")  # noqa: S608
    assert sorted(tuple(str(v) for v in row) for row in df.collect()) == sorted((str(a), b) for a, b in ROWS)


def test_read_text(spark, source):
    assert sorted(spark.read.text(f"file:{source}/text").collect()) == sorted((b,) for _, b in ROWS)


@pytest.mark.parametrize("data_format", ["parquet", "csv", "json"])
def test_read_with_base_path_and_recursive_lookup(spark, source, data_format):
    path = f"{source}/{data_format}"
    reader = spark.read.format(data_format).schema(SCHEMA)
    df = reader.option("basePath", f"file:{source}").load(f"file:{path}")
    assert sorted(df.collect()) == sorted(ROWS)
    df = reader.option("recursiveFileLookup", "true").load(f"file:{path}")
    assert sorted(df.collect()) == sorted(ROWS)


@pytest.mark.parametrize("data_format", ["parquet", "csv", "json"])
def test_read_glob_and_mixed_spellings(spark, source, data_format):
    path = f"{source}/{data_format}"
    reader = spark.read.format(data_format).schema(SCHEMA)
    assert sorted(reader.load(f"file:{path}/part-*").collect()) == sorted(ROWS)
    assert sorted(reader.load([f"file:{path}", path]).collect()) == sorted(ROWS * 2)


@pytest.mark.parametrize("data_format", ["parquet", "csv", "json"])
def test_write(spark, tmp_path, data_format):
    path = str(tmp_path / "target")
    spark.createDataFrame(ROWS, SCHEMA).write.format(data_format).save(f"file:{path}")
    assert sorted(spark.read.format(data_format).schema(SCHEMA).load(path).collect()) == sorted(ROWS)
    _assert_no_local_scheme_dir("file", "FILE")


def test_insert_overwrite_directory(spark, tmp_path):
    path = str(tmp_path / "target")
    location = escape_sql_string_literal(f"file:{path}")
    spark.sql(f"INSERT OVERWRITE DIRECTORY '{location}' USING parquet SELECT 1 AS a, 'a' AS b")
    assert spark.read.parquet(path).collect() == [(1, "a")]
    _assert_no_local_scheme_dir("file")


def test_table_location(spark, tmp_path):
    path = str(tmp_path / "table")
    location = escape_sql_string_literal(f"file:{path}")
    spark.sql(f"CREATE TABLE hadoop_file_uri_location ({SCHEMA}) USING csv LOCATION '{location}'")
    try:
        spark.sql("INSERT INTO hadoop_file_uri_location VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        assert sorted(spark.table("hadoop_file_uri_location").collect()) == sorted(ROWS)
        assert sorted(spark.read.schema(SCHEMA).csv(path).collect()) == sorted(ROWS)
    finally:
        spark.sql("DROP TABLE hadoop_file_uri_location")
    _assert_no_local_scheme_dir("file")


@pytest.mark.spark(skip_if_jvm=True, reason="Needs the Delta and Iceberg formats")
@pytest.mark.parametrize("data_format", ["delta", "iceberg"])
def test_lakehouse_path_table(spark, tmp_path, data_format):
    path = str(tmp_path / "table")
    spark.createDataFrame(ROWS, SCHEMA).write.format(data_format).save(f"file:{path}")
    spark.createDataFrame([(4, "d")], SCHEMA).write.format(data_format).mode("append").save(f"file:{path}")
    expected = sorted([*ROWS, (4, "d")])
    assert sorted(spark.read.format(data_format).load(path).collect()) == expected
    assert sorted(spark.read.format(data_format).load(f"file:{path}").collect()) == expected
    assert sorted(spark.sql(f"SELECT * FROM {data_format}.`file:{path}`").collect()) == expected  # noqa: S608
    _assert_no_local_scheme_dir("file")


@pytest.mark.parametrize("data_format", ["parquet", "csv"])
def test_uri_without_authority_for_other_schemes_is_not_local(spark, data_format):
    # The URI names a bucket-less S3 location, which fails like `s3a:///bucket/key`.
    path = "s3a:/bucket/key"
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.format(data_format).load(path).collect()
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.createDataFrame(ROWS, SCHEMA).write.format(data_format).save(path)
    _assert_no_local_scheme_dir("s3a")


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark reports a different error")
@pytest.mark.parametrize("path", ["http:/host/x", "http:///host/x", "file:", "s3a:"])
def test_uri_without_authority_or_path_is_rejected(spark, path):
    with pytest.raises(Exception, match="URL without authority"):
        spark.read.csv(path).collect()
    with pytest.raises(Exception, match="URL without authority"):
        spark.createDataFrame(ROWS, SCHEMA).write.csv(path)


def test_file_uri_with_other_host_is_rejected(spark, tmp_path):
    path = str(tmp_path / "p")
    spark.createDataFrame(ROWS, SCHEMA).write.parquet(path)
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.parquet(f"file://otherhost{path}").collect()
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.parquet(f"file://otherhost{path}").write.mode("overwrite").parquet(path)
    assert sorted(spark.read.parquet(path).collect()) == sorted(ROWS)


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark reports a different error")
def test_lakehouse_location_with_relative_file_uri_is_rejected(spark):
    with pytest.raises(Exception, match="must be an absolute path or URL: file:rel"):
        spark.sql("SELECT * FROM iceberg.`file:rel`").collect()
    try:
        with pytest.raises(Exception, match="must be an absolute path or URL: file:rel"):
            spark.sql("CREATE TABLE hadoop_file_uri_relative (a INT) USING delta LOCATION 'file:rel'")
    finally:
        spark.sql("DROP TABLE IF EXISTS hadoop_file_uri_relative")


@pytest.mark.spark(skip_if_jvm=True, reason="Needs the Delta format")
@pytest.mark.parametrize("location", ["s3a:/bucket/key", "s3a:///bucket/key"])
def test_lakehouse_location_without_authority_is_not_local(spark, location):
    try:
        with pytest.raises(Exception):  # noqa: B017, PT011
            spark.sql(f"CREATE TABLE hadoop_file_uri_s3a (a INT) USING delta LOCATION '{location}'")
    finally:
        spark.sql("DROP TABLE IF EXISTS hadoop_file_uri_s3a")
    _assert_no_local_scheme_dir("s3a")
