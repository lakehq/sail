import glob
import gzip
from collections.abc import Mapping

import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_identifier


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_write_basic(spark, sample_df, tmp_path, infer_schema):
    # Round-trip a typed DataFrame. With `inferSchema=True` types are
    # preserved; with the Spark default `inferSchema=False` every column
    # comes back as STRING. We pin both behaviors.
    path = str(tmp_path / f"csv_basic_{infer_schema}")
    sample_df.write.csv(path, header=True, mode="overwrite")
    read_df = spark.read.option("header", True).option("inferSchema", infer_schema).csv(path)
    assert sample_df.count() == read_df.count()
    if infer_schema:
        expected = [
            Row(col1="a", col2=1),
            Row(col1="b", col2=2),
            Row(col1="c", col2=3),
            Row(col1=None, col2=4),
        ]
    else:
        expected = [
            Row(col1="a", col2="1"),
            Row(col1="b", col2="2"),
            Row(col1="c", col2="3"),
            Row(col1=None, col2="4"),
        ]
    assert sorted(read_df.collect(), key=safe_sort_key) == sorted(expected, key=safe_sort_key)


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_write_compressed(spark, sample_df, sample_pandas_df, tmp_path, infer_schema):
    # Round-tripped values are typed under `inferSchema=True` and STRING-only
    # under the Spark default `inferSchema=False`. Both behaviors are pinned.
    if infer_schema:
        expected_rows = sorted(sample_df.collect(), key=safe_sort_key)
        expected_pandas = sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key)
    else:
        expected_rows = sorted(
            [
                Row(col1="a", col2="1"),
                Row(col1="b", col2="2"),
                Row(col1="c", col2="3"),
                Row(col1=None, col2="4"),
            ],
            key=safe_sort_key,
        )
        expected_pandas = sorted(
            [
                {"col1": "a", "col2": "1"},
                {"col1": "b", "col2": "2"},
                {"col1": "c", "col2": "3"},
                {"col1": None, "col2": "4"},
            ],
            key=safe_sort_key,
        )

    # Test reading a compressed CSV file written by Sail
    sail_dir = f"csv_compressed_gzip_{infer_schema}"
    path = str(tmp_path / sail_dir)
    sample_df.write.option("header", "true").option("compression", "gzip").csv(path, mode="overwrite")
    read_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("compression", "gzip")
        .option("inferSchema", infer_schema)
        .load(path)
    )
    assert sample_df.count() == read_df.count()
    assert sorted(read_df.collect(), key=safe_sort_key) == expected_rows
    assert len(list((tmp_path / sail_dir).glob("*.csv.gz"))) > 0

    # Compression type not explicitly set.
    read_df = spark.read.format("csv").option("header", "true").option("inferSchema", infer_schema).load(path)
    assert sample_df.count() == read_df.count()
    assert sorted(read_df.collect(), key=safe_sort_key) == expected_rows
    assert len(list((tmp_path / sail_dir).glob("*.csv.gz"))) > 0

    # Test reading a compressed CSV file written by Pandas.
    pandas_dir_1 = f"csv_compressed_gzip_pandas_1_{infer_schema}"
    path = tmp_path / pandas_dir_1
    path.mkdir()
    path = str(path)
    sample_pandas_df.to_csv(f"{path}/sample_pandas_df.csv.gz", index=False, compression="gzip")
    read_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("compression", "gzip")
        .option("inferSchema", infer_schema)
        .load(path)
    )
    assert len(sample_pandas_df) == read_df.count()
    assert sorted(read_df.toPandas().to_dict(orient="records"), key=safe_sort_key) == expected_pandas
    assert len(list((tmp_path / pandas_dir_1).glob("*.csv.gz"))) > 0

    # Test reading a compressed CSV file written by Pandas with `.gz` in the filename.
    pandas_dir_2 = f"csv_compressed_gzip_pandas_2_{infer_schema}"
    path = tmp_path / pandas_dir_2
    path.mkdir()
    path = str(path)
    sample_pandas_df.to_csv(f"{path}/sample_pandas_df.gz.csv", index=False, compression="gzip")
    read_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("compression", "gzip")
        .option("inferSchema", infer_schema)
        .load(path)
    )
    assert len(sample_pandas_df) == read_df.count()
    assert sorted(read_df.toPandas().to_dict(orient="records"), key=safe_sort_key) == expected_pandas
    assert len(list((tmp_path / pandas_dir_2).glob("*.gz.csv"))) > 0


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_write_options(spark, sample_df, tmp_path, infer_schema):
    sub_dir = f"csv_write_options_{infer_schema}"
    path = str(tmp_path / sub_dir)
    (
        sample_df.write.option("delimiter", ";")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", "\\")
        # .option("nullValue", "NULL")
        .csv(path, mode="overwrite")
    )
    # NOTICE: Should be None, but got NULL.
    csv_files = glob.glob(str(tmp_path / sub_dir / "*.csv"))
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
        .option("inferSchema", infer_schema)
        .csv(path)
    )
    assert sample_df.count() == read_df.count()
    if infer_schema:
        expected = sorted(sample_df.collect(), key=safe_sort_key)
    else:
        expected = sorted(
            [
                Row(col1="a", col2="1"),
                Row(col1="b", col2="2"),
                Row(col1="c", col2="3"),
                Row(col1=None, col2="4"),
            ],
            key=safe_sort_key,
        )
    assert sorted(read_df.collect(), key=safe_sort_key) == expected


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_options(spark, tmp_path, infer_schema):
    path = tmp_path / f"csv_read_options_{infer_schema}"
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
        .option("inferSchema", infer_schema)
        .csv(str(path))
    )
    expected_row_count = 2
    assert read_df.count() == expected_row_count
    assert read_df.columns == ["col1", "col2"]
    expected_col2_value = 10 if infer_schema else "10"
    assert read_df.collect()[0].col2 == expected_col2_value


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_truncated_rows(spark, tmp_path, infer_schema):
    path = tmp_path / f"csv_read_truncated_rows_{infer_schema}"
    path.mkdir()
    data_path = path / "data.csv"
    with open(data_path, "w") as f:
        f.write("col1,col2\n")
        f.write("x,10\n")
        f.write("y\n")

    df = spark.read.option("header", "true").option("inferSchema", infer_schema).csv(str(path))
    # Without `allowTruncatedRows`, a row that has fewer fields than the
    # header must error out. The exact error message depends on whether
    # type inference runs, so we only pin the exception type.
    with pytest.raises(SparkConnectGrpcException):
        df.collect()

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", infer_schema)
        .option("allowTruncatedRows", "true")
        .csv(str(path))
    )
    expected_x_col2 = 10 if infer_schema else "10"
    assert sorted(df.collect()) == [Row(col1="x", col2=expected_x_col2), Row(col1="y", col2=None)]


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
@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_uppercase_extension_file(spark, tmp_path, ext, infer_schema):
    # Spark matches file extensions case-insensitively; a file named
    # `data.CSV` should be readable just like `data.csv`. Pinned for both
    # `inferSchema` settings so the test exercises listing only.
    data_path = tmp_path / f"data.{ext}"
    data_path.write_text("name,age\nAlice,30\n")
    df = spark.read.format("csv").option("header", "true").option("inferSchema", infer_schema).load(str(data_path))
    expected_age = 30 if infer_schema else "30"
    assert df.collect() == [Row(name="Alice", age=expected_age)]


@pytest.mark.parametrize("ext", ["CSV", "Csv"])
@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_uppercase_extension_directory(spark, tmp_path, ext, infer_schema):
    # Same case-insensitive matching for a directory of files. The reader
    # admits every non-hidden file regardless of extension case.
    path = tmp_path / "csv_upper_dir"
    path.mkdir()
    (path / f"part-0.{ext}").write_text("name,age\nAlice,30\n")
    (path / f"part-1.{ext}").write_text("name,age\nBob,40\n")
    df = spark.read.format("csv").option("header", "true").option("inferSchema", infer_schema).load(str(path))
    if infer_schema:
        expected = [Row(name="Alice", age=30), Row(name="Bob", age=40)]
    else:
        expected = [Row(name="Alice", age="30"), Row(name="Bob", age="40")]
    assert sorted(df.collect(), key=safe_sort_key) == expected


@pytest.mark.parametrize("infer_schema", [True, False])
def test_csv_read_uppercase_extension_compressed(spark, tmp_path, infer_schema):
    # Compressed-extension matching should also be case-insensitive: pointing
    # the reader directly at a `*.CSV.GZ` file (without an explicit
    # `compression` option) must still discover it as a gzipped CSV via
    # extension inference.
    file_path = tmp_path / "sample.CSV.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b"name,age\nAlice,30\nBob,40\n")
    read_df = spark.read.format("csv").option("header", "true").option("inferSchema", infer_schema).load(str(file_path))
    if infer_schema:
        expected = [Row(name="Alice", age=30), Row(name="Bob", age=40)]
    else:
        expected = [Row(name="Alice", age="30"), Row(name="Bob", age="40")]
    assert sorted(read_df.collect(), key=safe_sort_key) == expected


# -----------------------------------------------------------------------------
# Case-insensitive extension matching when an explicit schema is supplied.
#
# Sail reads every non-hidden file in a directory regardless of extension
# case (matching Spark). The tests below cover the schema-provided path
# across single files, directories, mixed case, compression, and
# schema-shape variations.
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
    df = spark.read.format("csv").schema("name STRING, age INT").option("header", "true").load(str(data_path))
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
    # Compressed file with uppercase `.CSV.GZ` plus explicit schema —
    # compression is auto-detected from the extension on the schema-provided
    # path too, so no `compression` option is needed.
    file_path = tmp_path / "data.CSV.GZ"
    with gzip.open(file_path, "wb") as f:
        f.write(b"a,1\nb,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(file_path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_compressed_directory(spark, tmp_path):
    # Directory of `*.CSV.GZ` files plus explicit schema. Same compression
    # auto-detection applies to directories.
    path = tmp_path / "csv_upper_schema_gz_dir"
    path.mkdir()
    with gzip.open(path / "part-0.CSV.GZ", "wb") as f:
        f.write(b"a,1\n")
    with gzip.open(path / "part-1.CSV.GZ", "wb") as f:
        f.write(b"b,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(path))
    assert sorted(df.collect(), key=safe_sort_key) == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_compressed_directory_explicit(spark, tmp_path):
    # Same as `..._with_schema_compressed_directory` but the user passes the
    # `compression` option explicitly. Both paths must produce the same result.
    path = tmp_path / "csv_upper_schema_gz_dir_explicit"
    path.mkdir()
    with gzip.open(path / "part-0.CSV.GZ", "wb") as f:
        f.write(b"a,1\n")
    with gzip.open(path / "part-1.CSV.GZ", "wb") as f:
        f.write(b"b,2\n")
    df = (
        spark.read.format("csv")
        .schema("k STRING, v INT")
        .option("header", "false")
        .option("compression", "gzip")
        .load(str(path))
    )
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
    # Spark parity: a directory containing both `.csv` and `.CSV` reads
    # every non-hidden file regardless of extension case.
    path = tmp_path / "csv_mixed_dir"
    path.mkdir()
    (path / "lower.csv").write_text("a,1\n")
    (path / "upper.CSV").write_text("b,2\n")
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(path))
    rows = sorted(df.collect(), key=safe_sort_key)
    assert rows == [Row(k="a", v=1), Row(k="b", v=2)]


def test_csv_read_uppercase_extension_with_schema_renames_header(spark, tmp_path):
    # When both `header=true` and a schema with different column names are
    # supplied, the schema's names take precedence and the header line in
    # the file is consumed (not surfaced as data). Verifies this still
    # works through the case-insensitive extension path.
    data_path = tmp_path / "data.CSV"
    data_path.write_text("name,age\nAlice,30\n")
    df = spark.read.format("csv").schema("foo STRING, bar STRING").option("header", "true").load(str(data_path))
    assert df.columns == ["foo", "bar"]
    assert df.collect() == [Row(foo="Alice", bar="30")]


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


def test_csv_read_dataframe_reader_round_trip(spark, tmp_path):
    # Adapted from Spark 3.5's test for SPARK-42011 (Implement
    # `DataFrameReader.csv`):
    # https://github.com/apache/spark/blob/branch-3.5/python/pyspark/sql/tests/connect/test_connect_basic.py#L328-L336
    # Round-trips a DataFrame through `.write.format("csv").save(...)` and
    # `spark.read.csv(...)` to make sure the Spark Connect entrypoint reaches
    # the CSV reader.
    path = str(tmp_path / "csv_dataframe_reader_round_trip")
    spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}]).write.mode("overwrite").format("csv").save(path)
    actual = sorted(spark.read.csv(path).toPandas().to_dict(orient="records"), key=lambda r: r["_c0"])
    assert actual == [{"_c0": "Alice"}, {"_c0": "Bob"}]
