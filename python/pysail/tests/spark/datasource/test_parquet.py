import re
from collections.abc import Mapping

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row

from pysail.testing.spark.utils.files import get_data_directory_size
from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


def test_parquet_read_write_basic(spark, sample_df, tmp_path):
    output_path = tmp_path / "parquet_basic"
    path = str(output_path)
    sample_df.write.parquet(path, mode="overwrite")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    files = list((tmp_path / "parquet_basic").glob("*.parquet"))
    assert files
    assert all(
        re.fullmatch(
            r"part-\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
            r"-c\d{3}\.snappy\.parquet",
            file.name,
        )
        for file in files
    )
    assert (output_path / "_SUCCESS").read_bytes() == b""
    assert not (output_path / "_temporary").exists()


def test_parquet_hive_partition_paths_match_spark(spark, tmp_path):
    path = tmp_path / "parquet_hive_partition_paths"
    df = spark.createDataFrame(
        [
            (1, "a/b"),
            (2, "x=y"),
            (3, ""),
            (4, None),
            (5, "雪"),
        ],
        "id INT, `part=name` STRING",
    )

    df.write.partitionBy("part=name").parquet(str(path))

    files = list(path.rglob("*.parquet"))
    partition_directories = {str(file.parent.relative_to(path)) for file in files}
    assert partition_directories == {
        "part%3Dname=a%2Fb",
        "part%3Dname=x%3Dy",
        "part%3Dname=__HIVE_DEFAULT_PARTITION__",
        "part%3Dname=雪",
    }
    assert all(
        re.fullmatch(
            r"part-\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
            r"\.c\d{3}\.snappy\.parquet",
            file.name,
        )
        for file in files
    )
    assert (path / "_SUCCESS").read_bytes() == b""
    assert not (path / "_temporary").exists()

    read_df = spark.read.parquet(str(path))
    assert sorted(read_df.collect(), key=lambda row: row.id) == [
        Row(id=1, **{"part=name": "a/b"}),
        Row(id=2, **{"part=name": "x=y"}),
        Row(id=3, **{"part=name": None}),
        Row(id=4, **{"part=name": None}),
        Row(id=5, **{"part=name": "雪"}),
    ]
    assert read_df.where("`part=name` = 'a/b'").collect() == [Row(id=1, **{"part=name": "a/b"})]
    assert read_df.selectExpr("count(`part=name`) AS count").collect() == [Row(count=3)]


def test_parquet_binary_hive_partition_paths_and_round_trip(spark, tmp_path):
    path = tmp_path / "parquet_binary_hive_partition_paths"
    df = spark.createDataFrame(
        [
            (1, bytearray(b"ab")),
            (2, bytearray()),
            (3, None),
            (4, bytearray(b"\xff")),
            (5, bytearray(b"\xc3(")),
        ],
        "id INT, binary_part BINARY",
    )

    df.write.partitionBy("binary_part").parquet(str(path))

    files = list(path.rglob("*.parquet"))
    partition_directories = {str(file.parent.relative_to(path)) for file in files}
    assert partition_directories == {
        "binary_part=ab",
        "binary_part=�",
        "binary_part=�(",
        "binary_part=__HIVE_DEFAULT_PARTITION__",
    }

    rows = spark.read.schema("id INT, binary_part BINARY").parquet(str(path)).orderBy("id").collect()
    assert [(row.id, None if row.binary_part is None else bytes(row.binary_part)) for row in rows] == [
        (1, b"ab"),
        (2, None),
        (3, None),
        (4, "�".encode()),
        (5, "�(".encode()),
    ]


def test_parquet_hive_partition_value_formatting(spark, tmp_path):
    path = tmp_path / "parquet_hive_partition_value_formatting"
    df = spark.sql(
        """
        SELECT
          1 AS id,
          DATE '2024-01-02' AS date_part,
          CAST(123.40 AS DECIMAL(10, 2)) AS decimal_part,
          TIMESTAMP '2024-01-02 03:04:05.123456' AS timestamp_part
        """
    )

    df.write.partitionBy("date_part", "decimal_part", "timestamp_part").parquet(str(path))

    files = list(path.rglob("*.parquet"))
    assert len(files) == 1
    assert str(files[0].parent.relative_to(path)) == (
        "date_part=2024-01-02/decimal_part=123.40/timestamp_part=2024-01-02 03%3A04%3A05.123456"
    )

    read_df = spark.read.schema(
        "id INT, date_part DATE, decimal_part DECIMAL(10, 2), timestamp_part TIMESTAMP"
    ).parquet(str(path))
    assert read_df.selectExpr(
        "id",
        "CAST(date_part AS STRING) AS date_part",
        "CAST(decimal_part AS STRING) AS decimal_part",
        "CAST(timestamp_part AS STRING) AS timestamp_part",
    ).collect() == [
        Row(
            id=1,
            date_part="2024-01-02",
            decimal_part="123.40",
            timestamp_part="2024-01-02 03:04:05.123456",
        )
    ]


def test_parquet_max_records_per_file(spark, tmp_path):
    path = tmp_path / "parquet_max_records_per_file"
    row_count = 5
    spark.range(row_count).coalesce(1).write.option("maxRecordsPerFile", 2).parquet(str(path))

    files = sorted(path.glob("*.parquet"))
    assert [re.search(r"-c(\d{3})\.", file.name).group(1) for file in files] == [
        "000",
        "001",
        "002",
    ]
    assert [spark.read.parquet(str(file)).count() for file in files] == [2, 2, 1]

    unlimited_path = tmp_path / "parquet_max_records_per_file_unlimited"
    spark.range(5).coalesce(1).write.option("maxRecordsPerFile", -1).parquet(str(unlimited_path))
    unlimited_files = list(unlimited_path.glob("*.parquet"))
    assert len(unlimited_files) == 1
    assert spark.read.parquet(str(unlimited_files[0])).count() == row_count


def test_parquet_max_records_per_file_with_hive_partitions(spark, tmp_path):
    path = tmp_path / "parquet_partitioned_max_records_per_file"
    df = spark.createDataFrame(
        [(1, "a"), (2, "a"), (3, "a"), (4, "a"), (5, "a"), (6, "b"), (7, "b"), (8, "b")],
        "id INT, bucket STRING",
    ).coalesce(1)

    df.write.option("maxRecordsPerFile", 2).partitionBy("bucket").parquet(str(path))

    a_files = sorted((path / "bucket=a").glob("*.parquet"))
    b_files = sorted((path / "bucket=b").glob("*.parquet"))
    assert [spark.read.parquet(str(file)).count() for file in a_files] == [2, 2, 1]
    assert [spark.read.parquet(str(file)).count() for file in b_files] == [2, 1]


def test_parquet_max_records_per_file_session_fallback_and_override(spark, tmp_path):
    key = "spark.sql.files.maxRecordsPerFile"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "2")
        session_path = tmp_path / "parquet_max_records_session"
        spark.range(5).coalesce(1).write.parquet(str(session_path))
        session_files = sorted(session_path.glob("*.parquet"))
        assert [spark.read.parquet(str(file)).count() for file in session_files] == [2, 2, 1]

        override_path = tmp_path / "parquet_max_records_override"
        spark.range(5).coalesce(1).write.option("maxRecordsPerFile", 3).parquet(str(override_path))
        override_files = sorted(override_path.glob("*.parquet"))
        assert [spark.read.parquet(str(file)).count() for file in override_files] == [3, 2]
    finally:
        spark.conf.set(key, previous)


def test_parquet_empty_write_has_one_readable_file(spark, tmp_path):
    path = tmp_path / "parquet_empty_write"
    empty = spark.createDataFrame([], "id INT, value STRING").repartition(2)

    empty.write.parquet(str(path))

    files = list(path.glob("*.parquet"))
    assert len(files) == 1
    read_df = spark.read.parquet(str(path))
    assert read_df.schema == empty.schema
    assert read_df.count() == 0
    assert (path / "_SUCCESS").read_bytes() == b""
    assert not (path / "_temporary").exists()


def test_parquet_path_glob_filter(spark, tmp_path):
    keep_source = tmp_path / "parquet_keep_source"
    drop_source = tmp_path / "parquet_drop_source"
    spark.createDataFrame([(1,)], "id INT").coalesce(1).write.parquet(str(keep_source))
    spark.createDataFrame([(2,)], "id INT").coalesce(1).write.parquet(str(drop_source))

    path = tmp_path / "parquet_path_glob_filter"
    path.mkdir()
    next(keep_source.glob("*.parquet")).rename(path / "keep.parquet")
    next(drop_source.glob("*.parquet")).rename(path / "drop.parquet")

    df = spark.read.option("pathGlobFilter", "keep.*").parquet(str(path))

    assert df.collect() == [Row(id=1)]


def test_parquet_write_modes(spark, tmp_path):
    output_path = tmp_path / "parquet_write_modes"
    path = str(output_path)

    spark.createDataFrame([(1, "old")], schema="id INT, value STRING").write.parquet(path)

    with pytest.raises(Exception, match="already exists"):
        spark.createDataFrame([(2, "error")], schema="id INT, value STRING").write.parquet(path)

    spark.createDataFrame([(3, "ignored")], schema="id INT, value STRING").write.mode("ignore").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [Row(id=1, value="old")]

    spark.createDataFrame([(4, "appended")], schema="id INT, value STRING").write.mode("append").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [
        Row(id=1, value="old"),
        Row(id=4, value="appended"),
    ]

    spark.createDataFrame([(5, "new")], schema="id INT, value STRING").write.mode("overwrite").parquet(path)
    assert spark.read.parquet(path).orderBy("id").collect() == [Row(id=5, value="new")]
    assert (output_path / "_SUCCESS").read_bytes() == b""
    assert not (output_path / "_temporary").exists()


def test_parquet_write_modes_with_empty_existing_path(spark, tmp_path):
    path = tmp_path / "parquet_write_modes_empty_existing_path"
    path.mkdir()

    with pytest.raises(Exception, match="already exists"):
        spark.createDataFrame([(1, "error")], schema="id INT, value STRING").write.parquet(str(path))
    assert list(path.iterdir()) == []

    spark.createDataFrame([(2, "ignored")], schema="id INT, value STRING").write.mode("ignore").parquet(str(path))
    assert list(path.iterdir()) == []


def test_parquet_read_write_compressed(spark, sample_df, sample_pandas_df, tmp_path):
    # Test reading a compressed Parquet file written by Sail
    path = str(tmp_path / "parquet_compressed_zstd")
    sample_df.write.parquet(path, mode="overwrite", compression="zstd(4)")
    read_df = spark.read.parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    assert len(list((tmp_path / "parquet_compressed_zstd").glob("*.zstd.parquet"))) > 0

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

    path = str(tmp_path / "parquet_write_options_uncompressed")
    sample_df.write.parquet(path, mode="overwrite", compression="none")
    read_df = spark.read.parquet(path)
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)
    files = list((tmp_path / "parquet_write_options_uncompressed").glob("*.parquet"))
    assert files
    assert all(re.search(r"-c\d{3}\.parquet$", file.name) for file in files)
    assert all("..parquet" not in file.name for file in files)


def test_parquet_read_options(spark, sample_df, tmp_path):
    path = str(tmp_path / "parquet_read_options")
    sample_df.write.parquet(path, mode="overwrite")
    read_df = spark.read.option("binaryAsString", "false").option("pruning", "true").parquet(path)
    assert sample_df.count() == read_df.count()
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_parquet_write_with_bloom_filter(spark, tmpdir):
    def size(p):
        return get_data_directory_size(p, extension=".parquet")

    path = str(tmpdir / "default")
    spark.sql("SELECT 1").write.parquet(path)
    # The Parquet file without bloom filter is small (less than 1 kB).
    base_size = size(path)
    assert base_size < 1024  # noqa: PLR2004

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
    # Bloom filter adds overhead; file must be larger than the base (no bloom filter) size.
    assert size(path) > base_size

    path = str(tmpdir / "bloom_filter_on_with_multiple_columns")
    (
        spark.sql("SELECT 1, 2")
        .write.option("bloom_filter_on_write", "true")
        .option("bloom_filter_fpp", "0.05")
        .option("bloom_filter_ndv", "10000")
        .parquet(path)
    )
    # Two columns produce a larger bloom filter than one column.
    assert size(path) > size(str(tmpdir / "bloom_filter_on"))


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


def test_parquet_read_with_custom_extension(spark, sample_pandas_df, tmp_path):
    """`pathGlobFilter` selects Parquet files with a custom suffix."""
    directory = tmp_path / "parquet_custom_extension"
    directory.mkdir()
    file_path = directory / "data.hive"
    sample_pandas_df.to_parquet(str(file_path))
    sample_pandas_df.to_parquet(str(directory / "ignored.parquet"))

    expected_count = len(sample_pandas_df)
    expected_rows = sorted(sample_pandas_df.to_dict(orient="records"), key=safe_sort_key)

    def actual_rows(df):
        return sorted(df.toPandas().to_dict(orient="records"), key=safe_sort_key)

    read_df = spark.read.option("pathGlobFilter", "*.hive").parquet(str(directory))
    assert read_df.count() == expected_count
    assert actual_rows(read_df) == expected_rows

    # SQL CREATE TABLE with OPTIONS (pathGlobFilter '*.hive').
    table_name = "parquet_custom_extension_table"
    try:
        spark.sql(
            f"CREATE TABLE {table_name} USING parquet "
            f"OPTIONS (pathGlobFilter '*.hive') "
            f"LOCATION '{escape_sql_string_literal(str(directory))}'"
        )
        read_df = spark.sql(f"SELECT * FROM {table_name}")  # noqa: S608
        assert read_df.count() == expected_count
        assert actual_rows(read_df) == expected_rows
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_parquet_read_uppercase_extension(spark, sample_df, tmp_path):
    # Extensions are matched case-insensitively, so renaming the parquet
    # file's extension to `.PARQUET` must still allow it to be read.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    dst.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        f.rename(dst / f"part-{i}.PARQUET")
    df = spark.read.parquet(str(dst))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


# -----------------------------------------------------------------------------
# Case-insensitive extension matching for Parquet, plus partition-aware reads.
# Sail reads every non-hidden file in a directory regardless of extension case
# (matching Spark). The tests below cover that across single files,
# directories, partitioned trees, mixed case, schema-shape variations, and
# user-supplied globs.
# -----------------------------------------------------------------------------


def _rename_part_files(src, dst, ext):
    """Move every `*.parquet` file from `src` into `dst` with the given extension."""
    dst.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        f.rename(dst / f"part-{i}.{ext}")


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet", "ParQuet", "parqueT"])
def test_parquet_read_uppercase_extension_file(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    files = list(dst.glob(f"*.{ext}"))
    assert files, "expected renamed parquet files"
    df = spark.read.parquet(str(files[0]))
    assert df.count() > 0


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet"])
def test_parquet_read_uppercase_extension_with_schema_struct_file(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    files = list(dst.glob(f"*.{ext}"))
    # `sample_df` is tiny (4 rows) so a single output file is expected.
    assert len(files) == 1
    df = spark.read.schema(sample_df.schema).parquet(str(files[0]))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


@pytest.mark.parametrize("ext", ["PARQUET", "Parquet"])
def test_parquet_read_uppercase_extension_with_schema_directory(spark, sample_df, tmp_path, ext):
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, ext)
    df = spark.read.schema(sample_df.schema).parquet(str(dst))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


def test_parquet_read_lowercase_extension_with_schema_regression(spark, sample_df, tmp_path):
    # Regression: lowercase `.parquet` with schema must keep working.
    path = str(tmp_path / "lower")
    sample_df.write.parquet(path, mode="overwrite")
    df = spark.read.schema(sample_df.schema).parquet(path)
    assert df.count() == sample_df.count()


def test_parquet_read_uppercase_extension_with_schema_subset_columns(spark, sample_df, tmp_path):
    # Parquet column projection is by name: schema with one field should
    # only return that column.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    dst = tmp_path / "dst"
    _rename_part_files(src, dst, "PARQUET")
    df = spark.read.schema("col1 STRING").parquet(str(dst))
    assert df.columns == ["col1"]
    assert df.count() == sample_df.count()


def test_parquet_read_mixed_case_directory_with_schema(spark, sample_df, tmp_path):
    # Spark parity: directory with both `.parquet` and `.PARQUET` reads
    # every non-hidden file regardless of extension case.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    mixed = tmp_path / "mixed"
    mixed.mkdir()
    for i, f in enumerate(src.glob("*.parquet")):
        if i % 2 == 0:
            f.rename(mixed / f"part-{i}.parquet")
        else:
            f.rename(mixed / f"part-{i}.PARQUET")
    df = spark.read.schema(sample_df.schema).parquet(str(mixed))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


def test_parquet_read_uppercase_extension_partitioned_directory(spark, tmp_path):
    # Partitioned write produces a partitioned tree under a directory.
    # Renaming every leaf `.parquet` to `.PARQUET` must still let the table
    # be read. Relies on partition discovery (no `.schema()`) since
    # `part` lives only in the directory name, not in the file.
    df_in = spark.createDataFrame(
        [(1, "a", "x"), (2, "b", "x"), (3, "c", "y")],
        "id INT, val STRING, part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("part").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", part="x"),
        Row(id=2, val="b", part="x"),
        Row(id=3, val="c", part="y"),
    ]


def test_parquet_read_uppercase_extension_partitioned_directory_with_schema(spark, tmp_path):
    # Same as the previous test but with an explicit schema that includes
    # the partition column. Spark recognizes `part` as a partition column
    # from the directory structure even when a schema is supplied; Sail
    # should match that so users migrating from Spark don't see a regression.
    df_in = spark.createDataFrame(
        [(1, "a", "x"), (2, "b", "x"), (3, "c", "y")],
        "id INT, val STRING, part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("part").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    df = spark.read.schema("id INT, val STRING, part STRING").parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", part="x"),
        Row(id=2, val="b", part="x"),
        Row(id=3, val="c", part="y"),
    ]


def test_parquet_hidden_files_are_excluded(spark, sample_df, tmp_path):
    # `_SUCCESS`, `_committed_*`, and `.crc` files commonly land alongside
    # parquet output. The default URL glob (`[!._]*`) must skip them so the
    # parquet reader doesn't try to parse them as parquet.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    # Drop in a few hidden marker files of the kinds Spark / Hadoop tools
    # write next to data files.
    (src / "_SUCCESS").write_text("")
    (src / "_committed_xyz").write_text("garbage")
    (src / ".crc").write_text("garbage")
    df = spark.read.parquet(str(src))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)


@pytest.mark.skip(
    reason=(
        "FIXME: Sail's partition-column type inference returns every "
        "discovered column as STRING; Spark infers Int/Double/String from "
        "the observed `key=value` strings (see FIXME above "
        "`rewrite_listing_partitions` in `crates/sail-data-source/src/listing.rs`)."
    )
)
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_partitioned_directory_type_inference(spark, tmp_path, provide_schema):
    # Verify partition-column type inference against Spark behavior across
    # int / double / string. The original DataFrame columns are
    # `int_part INT, float_part DOUBLE, string_part STRING`.
    #
    # No-schema (Spark `partitionColumnTypeInference.enabled=true`, default):
    #   - int_part      → integer (all values parse as int)
    #   - float_part    → double  (all values parse as float)
    #   - string_part   → string  (alpha/beta don't parse numerically)
    #
    # With-schema: declared types are honored.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 1.5, "alpha"),
            (2, "b", 2024, 2.5, "beta"),
            (3, "c", 2025, 3.0, "alpha"),
        ],
        "id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("int_part", "float_part", "string_part").parquet(str(src), mode="overwrite")

    if provide_schema:
        df = (
            spark.read.schema("id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING")
            .parquet(str(src))
            .orderBy("id")
        )
        type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert type_by_name["int_part"] == "int"
        assert type_by_name["float_part"] == "double"
        assert type_by_name["string_part"] == "string"
        rows = df.collect()
        assert rows == [
            Row(
                id=1,
                val="a",
                int_part=2024,
                float_part=1.5,
                string_part="alpha",
            ),
            Row(
                id=2,
                val="b",
                int_part=2024,
                float_part=2.5,
                string_part="beta",
            ),
            Row(
                id=3,
                val="c",
                int_part=2025,
                float_part=3.0,
                string_part="alpha",
            ),
        ]
    else:
        df = spark.read.parquet(str(src)).orderBy("id")
        type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert type_by_name["int_part"] == "int"
        assert type_by_name["float_part"] == "double"
        assert type_by_name["string_part"] == "string"
        rows = df.collect()
        assert rows == [
            Row(id=1, val="a", int_part=2024, float_part=1.5, string_part="alpha"),
            Row(id=2, val="b", int_part=2024, float_part=2.5, string_part="beta"),
            Row(id=3, val="c", int_part=2025, float_part=3.0, string_part="alpha"),
        ]


# TODO: remove this test once the FIXME above `rewrite_listing_partitions`
# in `crates/sail-data-source/src/listing.rs` is addressed and the
# Spark-parity test above starts passing.
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_partitioned_directory_type_inference_string_only(spark, tmp_path, provide_schema):
    # Pins Sail's current behavior: every partition column comes back as
    # STRING regardless of the underlying values. Once partition type
    # inference matches Spark, this test should be deleted in favor of the
    # `test_parquet_read_partitioned_directory_type_inference` above.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 1.5, "alpha"),
            (2, "b", 2024, 2.5, "beta"),
            (3, "c", 2025, 3.0, "alpha"),
        ],
        "id INT, val STRING, int_part INT, float_part DOUBLE, string_part STRING",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("int_part", "float_part", "string_part").parquet(str(src), mode="overwrite")

    if provide_schema:
        df = (
            spark.read.schema("id INT, val STRING, int_part STRING, float_part STRING, string_part STRING")
            .parquet(str(src))
            .orderBy("id")
        )
    else:
        df = spark.read.parquet(str(src)).orderBy("id")

    type_by_name = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert type_by_name["int_part"] == "string"
    assert type_by_name["float_part"] == "string"
    assert type_by_name["string_part"] == "string"
    rows = df.collect()
    # Partition values use Spark-compatible formatting even though Sail still
    # discovers all partition column types as STRING.
    assert rows == [
        Row(id=1, val="a", int_part="2024", float_part="1.5", string_part="alpha"),
        Row(id=2, val="b", int_part="2024", float_part="2.5", string_part="beta"),
        Row(id=3, val="c", int_part="2025", float_part="3.0", string_part="alpha"),
    ]


@pytest.mark.skip(
    reason=(
        "FIXME: Sail's partition-column type inference returns every "
        "discovered column as STRING; Spark infers Int/Double/String from "
        "the observed `key=value` strings (see FIXME above "
        "`rewrite_listing_partitions` in `crates/sail-data-source/src/listing.rs`)."
    )
)
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_multi_level_partitioned_directory(spark, tmp_path, provide_schema):
    # Two-level partition tree: `year=2024/month=11/...`. Partition discovery
    # must walk both segments and surface both columns. Tested both with
    # auto-discovery (no schema) and with an explicit schema that includes
    # the partition columns.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 10),
            (2, "b", 2024, 11),
            (3, "c", 2025, 1),
        ],
        "id INT, val STRING, year INT, month INT",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("year", "month").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    if provide_schema:
        df = spark.read.schema("id INT, val STRING, year INT, month INT").parquet(str(src))
    else:
        df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", year=2024, month=10),
        Row(id=2, val="b", year=2024, month=11),
        Row(id=3, val="c", year=2025, month=1),
    ]


# TODO: remove this test once the FIXME above `rewrite_listing_partitions`
# in `crates/sail-data-source/src/listing.rs` is addressed and the
# Spark-parity test above starts passing.
@pytest.mark.parametrize("provide_schema", [True, False])
def test_parquet_read_multi_level_partitioned_directory_string_only(spark, tmp_path, provide_schema):
    # Pins Sail's current behavior: every partition column comes back as
    # STRING regardless of the underlying values. Once partition type
    # inference matches Spark, this test should be deleted in favor of the
    # `test_parquet_read_multi_level_partitioned_directory` above.
    df_in = spark.createDataFrame(
        [
            (1, "a", 2024, 10),
            (2, "b", 2024, 11),
            (3, "c", 2025, 1),
        ],
        "id INT, val STRING, year INT, month INT",
    )
    src = tmp_path / "src"
    df_in.write.partitionBy("year", "month").parquet(str(src), mode="overwrite")
    for f in src.rglob("*.parquet"):
        f.rename(f.with_suffix(".PARQUET"))
    if provide_schema:
        df = spark.read.schema("id INT, val STRING, year STRING, month STRING").parquet(str(src))
    else:
        df = spark.read.parquet(str(src))
    rows = sorted(df.collect(), key=lambda r: r.id)
    assert rows == [
        Row(id=1, val="a", year="2024", month="10"),
        Row(id=2, val="b", year="2024", month="11"),
        Row(id=3, val="c", year="2025", month="1"),
    ]


@pytest.mark.parametrize("ext", ["parquet", "PARQUET"])
def test_parquet_read_with_schema_column_projection(spark, sample_df, tmp_path, ext):
    # Parquet natively supports column projection. Supplying a schema with a
    # subset of the file's columns must return only those columns (without
    # erroring like CSV does). Verified for both the lowercase and uppercase
    # extension paths.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    if ext != "parquet":
        for f in src.glob("*.parquet"):
            f.rename(f.with_suffix(f".{ext}"))
    df = spark.read.schema("col1 STRING").parquet(str(src))
    assert df.columns == ["col1"]
    assert df.count() == sample_df.count()
    actual = sorted([r.col1 for r in df.collect()], key=lambda v: (v is not None, v))
    expected = sorted([r.col1 for r in sample_df.collect()], key=lambda v: (v is not None, v))
    assert actual == expected


def test_parquet_read_with_user_supplied_glob(spark, sample_df, tmp_path):
    # When the caller passes their own glob in the URL (e.g. `*.PARQUET`),
    # our default hidden-file glob must not interfere. The user's pattern
    # takes precedence, and the read should match exactly the files the
    # user asked for — including dropping a co-located `.parquet` file.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    # Half the files become uppercase, half stay lowercase.
    files = sorted(src.glob("*.parquet"))
    for i, f in enumerate(files):
        if i % 2 == 0:
            f.rename(f.with_suffix(".PARQUET"))
    upper_glob = str(src / "*.PARQUET")
    df = spark.read.parquet(upper_glob)
    # Expected = rows from only the renamed (uppercase) files.
    expected_count = sum(spark.read.parquet(str(f)).count() for f in src.glob("*.PARQUET"))
    assert df.count() == expected_count


def test_parquet_read_uppercase_single_file_with_schema(spark, sample_df, tmp_path):
    # A single uppercase-extension parquet file plus an explicit schema.
    # Pinned in addition to the directory variant to make sure single-file
    # URL handling (which goes through `head` instead of `list`) behaves
    # the same way.
    src = tmp_path / "src"
    sample_df.write.parquet(str(src), mode="overwrite")
    files = list(src.glob("*.parquet"))
    assert len(files) == 1
    upper = files[0].with_suffix(".PARQUET")
    files[0].rename(upper)
    df = spark.read.schema(sample_df.schema).parquet(str(upper))
    assert df.count() == sample_df.count()
    assert sorted(df.collect(), key=safe_sort_key) == sorted(sample_df.collect(), key=safe_sort_key)
