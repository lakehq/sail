import glob
from collections.abc import Mapping

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StringType

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
