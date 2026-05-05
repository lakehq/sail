import gzip

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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


def test_json_read_uppercase_extension(spark, tmp_path):
    # Extensions are matched case-insensitively, so a `.JSON` file must
    # be discovered just like a `.json` file.
    data_path = tmp_path / "data.JSON"
    data_path.write_text('{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}\n')
    df = spark.read.format("json").load(str(data_path))
    assert df.count() == 2  # noqa: PLR2004


# -----------------------------------------------------------------------------
# Case-insensitive extension matching for JSON. Sail reads every non-hidden
# file regardless of extension case (matching Spark). The tests below cover
# the schema-provided path across single files, directories, mixed case, and
# compression.
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("ext", ["JSON", "Json", "jSon", "JsoN"])
def test_json_read_uppercase_extension_with_schema_struct_file(spark, tmp_path, ext):
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text('{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}\n')
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )
    df = spark.read.format("json").schema(schema).load(str(data_path))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [Row(id=1, value="a"), Row(id=2, value="b")]


@pytest.mark.parametrize("ext", ["JSON", "Json"])
def test_json_read_uppercase_extension_with_schema_ddl_file(spark, tmp_path, ext):
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text('{"id": 1, "value": "a"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(data_path))
    assert df.collect() == [Row(id=1, value="a")]


@pytest.mark.parametrize("ext", ["JSON", "Json"])
def test_json_read_uppercase_extension_with_schema_directory(spark, tmp_path, ext):
    path = tmp_path / "json_upper_schema_dir"
    path.mkdir()
    (path / f"part-0.{ext}").write_text('{"id": 1, "value": "a"}\n')
    (path / f"part-1.{ext}").write_text('{"id": 2, "value": "b"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(path))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [Row(id=1, value="a"), Row(id=2, value="b")]


def test_json_read_lowercase_extension_with_schema_regression(spark, tmp_path):
    # Regression: lowercase `.json` plus explicit schema must keep working.
    data_path = tmp_path / "data.json"
    data_path.write_text('{"id": 1, "value": "a"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(data_path))
    assert df.collect() == [Row(id=1, value="a")]


def test_json_read_uppercase_extension_compressed(spark, tmp_path):
    # `.JSON.GZ` must be discovered and decoded via case-insensitive
    # compressed-extension matching with no explicit `compression` option.
    file_path = tmp_path / "data.JSON.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b'{"id": 1, "value": "a"}\n{"id": 2, "value": "b"}\n')
    df = spark.read.format("json").load(str(file_path))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [Row(id=1, value="a"), Row(id=2, value="b")]


def test_json_read_uppercase_extension_with_schema_compressed(spark, tmp_path):
    # `.JSON.GZ` plus explicit schema.
    file_path = tmp_path / "data.JSON.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b'{"id": 1, "value": "a"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(file_path))
    assert df.collect() == [Row(id=1, value="a")]


def test_json_read_uppercase_extension_with_schema_subset_columns(spark, tmp_path):
    # JSON projection is by name: a schema that omits a field should drop it.
    data_path = tmp_path / "data.JSON"
    data_path.write_text('{"id": 1, "value": "a", "extra": "x"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(data_path))
    assert df.columns == ["id", "value"]
    assert df.collect() == [Row(id=1, value="a")]


def test_json_read_mixed_case_directory_with_schema(spark, tmp_path):
    # Spark parity: directory containing both `.json` and `.JSON` reads
    # every non-hidden file regardless of extension case.
    path = tmp_path / "json_mixed_dir"
    path.mkdir()
    (path / "lower.json").write_text('{"id": 1, "value": "a"}\n')
    (path / "upper.JSON").write_text('{"id": 2, "value": "b"}\n')
    df = spark.read.format("json").schema("id INT, value STRING").load(str(path))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [Row(id=1, value="a"), Row(id=2, value="b")]


def test_json_read_uppercase_extension_via_spark_json_helper(spark, tmp_path):
    # `spark.read.json(...)` helper plus uppercase extension plus schema.
    data_path = tmp_path / "data.JSON"
    data_path.write_text('{"id": 1, "value": "a"}\n')
    df = spark.read.schema("id INT, value STRING").json(str(data_path))
    assert df.collect() == [Row(id=1, value="a")]
