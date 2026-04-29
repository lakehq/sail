import glob
import gzip
from collections.abc import Mapping

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_identifier


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


def test_csv_read_write_basic(spark, sample_df, tmp_path):
    path = str(tmp_path / "csv_basic")
    sample_df.write.csv(path, header=True, mode="overwrite")
    read_df = (
        spark.read.option("header", True)
        # .option("inferSchema", True)
        .csv(path)
    )
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_csv_read_write_compressed(spark, sample_df, sample_pandas_df, tmp_path):
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


def test_csv_write_options(spark, sample_df, tmp_path):
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


def test_csv_read_options(spark, tmp_path):
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


def test_csv_read_truncated_rows(spark, tmp_path):
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


def test_csv_infer_schema_false(spark, tmp_path):
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


def test_csv_infer_schema_false_no_header(spark, tmp_path):
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


def test_csv_format_path(spark, tmp_path):
    csv_file = tmp_path / "data.csv"
    # No header row: CSV options cannot be specified with this syntax,
    # so column names cannot be asserted.
    csv_file.write_text("1,Alice\n2,Bob\n")
    df = spark.sql(f"SELECT * FROM csv.`{escape_sql_identifier(str(csv_file))}`")  # noqa: S608
    assert df.count() == 2  # noqa: PLR2004


@pytest.mark.parametrize("ext", ["CSV", "Csv", "cSv"])
def test_csv_read_uppercase_extension_file(spark, tmp_path, ext):
    # Spark matches file extensions case-insensitively; a file named
    # `data.CSV` should be readable just like `data.csv`.
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text("name,age\nAlice,30\n")
    df = spark.read.format("csv").option("header", "true").load(str(data_path))
    assert df.collect() == [Row(name="Alice", age=30)]


@pytest.mark.parametrize("ext", ["CSV", "Csv"])
def test_csv_read_uppercase_extension_directory(spark, tmp_path, ext):
    # Same case-insensitive matching when the path is a directory and we
    # rely on extension filtering to pick up the files inside it.
    path = tmp_path / "csv_upper_dir"
    path.mkdir()
    (path / f"part-0.{ext}").write_text("name,age\nAlice,30\n")
    (path / f"part-1.{ext}").write_text("name,age\nBob,40\n")
    df = spark.read.format("csv").option("header", "true").load(str(path))
    assert sorted(df.collect(), key=safe_sort_key) == [
        Row(name="Alice", age=30),
        Row(name="Bob", age=40),
    ]


def test_csv_read_uppercase_extension_compressed(spark, tmp_path):
    # Compressed-extension matching should also be case-insensitive: pointing
    # the reader directly at a `*.CSV.GZ` file (without an explicit
    # `compression` option) must still discover it as a gzipped CSV via
    # extension inference.
    file_path = tmp_path / "sample.CSV.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b"name,age\nAlice,30\nBob,40\n")
    read_df = spark.read.format("csv").option("header", "true").load(str(file_path))
    assert sorted(read_df.collect(), key=safe_sort_key) == [
        Row(name="Alice", age=30),
        Row(name="Bob", age=40),
    ]


# -----------------------------------------------------------------------------
# Case-insensitive extension matching when an explicit schema is supplied.
#
# When the user calls `.schema(...)` Sail skips schema inference (and thus the
# observation step that aligns `options.file_extension` with the actual
# on-disk case). DataFusion's scan-time listing then drops files whose
# extension does not match the lowercase canonical (e.g. `.csv`). The tests
# below cover that path across single files, directories, mixed case,
# compression, and schema-shape variations.
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("ext", ["CSV", "Csv", "cSv", "CsV", "csV"])
def test_csv_read_uppercase_extension_with_schema_struct_file(spark, tmp_path, ext):
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text("a,1\nb,2\n")
    schema = StructType(
        [
            StructField("k", StringType(), True),
            StructField("v", IntegerType(), True),
        ]
    )
    df = spark.read.format("csv").schema(schema).option("header", "false").load(str(data_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


@pytest.mark.parametrize("ext", ["CSV", "Csv"])
def test_csv_read_uppercase_extension_with_schema_ddl_file(spark, tmp_path, ext):
    # DDL-string schemas should behave the same as StructType.
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text("a,1\nb,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(data_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


@pytest.mark.parametrize("ext", ["CSV", "Csv"])
def test_csv_read_uppercase_extension_with_schema_directory(spark, tmp_path, ext):
    path = tmp_path / "csv_upper_schema_dir"
    path.mkdir()
    (path / f"part-0.{ext}").write_text("a,1\nb,2\n")
    (path / f"part-1.{ext}").write_text("c,3\nd,4\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(path))
    assert sorted(df.collect(), key=safe_sort_key) == [
        Row(k="a", v=1),
        Row(k="b", v=2),
        Row(k="c", v=3),
        Row(k="d", v=4),
    ]


def test_csv_read_lowercase_extension_with_schema_regression(spark, tmp_path):
    # Regression check: lowercase `.csv` with explicit schema must still work.
    data_path = tmp_path / "data.csv"
    data_path.write_text("a,1\nb,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(data_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_and_header(spark, tmp_path):
    # `header=true` with an explicit schema: header line is consumed and
    # column names from the schema are used.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("name,age\nAlice,30\nBob,40\n")
    df = (
        spark.read.format("csv")
        .schema("name STRING, age INT")
        .option("header", "true")
        .load(str(data_path))
    )
    assert sorted(df.collect(), key=safe_sort_key) == [
        Row(name="Alice", age=30),
        Row(name="Bob", age=40),
    ]


def test_csv_read_uppercase_extension_with_schema_all_strings(spark, tmp_path):
    # Schema where every field is StringType.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("a,1\nb,2\n")
    schema = StructType([StructField("k", StringType(), True), StructField("v", StringType(), True)])
    df = spark.read.format("csv").schema(schema).option("header", "false").load(str(data_path))
    assert sorted(df.collect(), key=safe_sort_key) == [
        Row(k="a", v="1"),
        Row(k="b", v="2"),
    ]


def test_csv_read_uppercase_extension_with_schema_compressed(spark, tmp_path):
    # Compressed file with uppercase `.CSV.GZ` plus explicit schema.
    file_path = tmp_path / "data.CSV.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b"a,1\nb,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(file_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_compressed_explicit_option(spark, tmp_path):
    # Same as above but with an explicit `compression` option.
    file_path = tmp_path / "data.CSV.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b"a,1\nb,2\n")
    df = (
        spark.read.format("csv")
        .schema("k STRING, v INT")
        .option("header", "false")
        .option("compression", "gzip")
        .load(str(file_path))
    )
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_compressed_directory(spark, tmp_path):
    # Directory of `*.CSV.GZ` files plus explicit schema.
    path = tmp_path / "csv_upper_schema_gz_dir"
    path.mkdir()
    with gzip.open(path / "part-0.CSV.GZ", "wb") as f:
        f.write(b"a,1\n")
    with gzip.open(path / "part-1.CSV.GZ", "wb") as f:
        f.write(b"b,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_delimiter(spark, tmp_path):
    # Custom delimiter combined with explicit schema and uppercase extension.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("a|1\nb|2\n")
    df = (
        spark.read.format("csv")
        .schema("k STRING, v INT")
        .option("header", "false")
        .option("delimiter", "|")
        .load(str(data_path))
    )
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_mixed_case_directory_with_schema(spark, tmp_path):
    # A directory containing both `.csv` and `.CSV` with an explicit schema.
    # Per the existing TODO in `resolve_listing_schema`, only one variant is
    # picked at scan time; the lowercase canonical is preferred when present.
    # This test pins that behavior so any future change is intentional.
    path = tmp_path / "csv_mixed_dir"
    path.mkdir()
    (path / "lower.csv").write_text("a,1\n")
    (path / "upper.CSV").write_text("b,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(path))
    rows = sorted(df.collect(), key=safe_sort_key)
    # Lowercase is preferred; uppercase is dropped at DataFusion's scan-time filter.
    assert rows == [Row(k="a", v=1)]


def test_csv_read_uppercase_extension_with_schema_subset_columns(spark, tmp_path):
    # CSV column projection is positional, so a 1-column schema reads only
    # the first column from each row.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("a,1\nb,2\n")
    df = spark.read.format("csv").schema("k STRING").option("header", "false").load(str(data_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a"), Row(k="b")]


def test_csv_read_uppercase_extension_with_schema_truncated_rows(spark, tmp_path):
    # `allowTruncatedRows=true` plus explicit schema plus uppercase extension —
    # the original repro from the bug report.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("a,1\nb\n")
    df = (
        spark.read.format("csv")
        .schema("k STRING, v INT")
        .option("header", "false")
        .option("allowTruncatedRows", "true")
        .load(str(data_path))
    )
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=None)]


def test_csv_read_uppercase_extension_via_spark_csv_helper(spark, tmp_path):
    # `spark.read.csv(...)` (no `.format("csv")`) must hit the same code path.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("a,1\n")
    df = spark.read.schema("k STRING, v INT").csv(str(data_path), header=False)
    assert df.collect() == [Row(k="a", v=1)]
