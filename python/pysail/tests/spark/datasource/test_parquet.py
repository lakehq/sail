from collections.abc import Mapping
from datetime import UTC, date, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors import AnalysisException
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.files import get_data_directory_size
from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal


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
    path = str(tmp_path / "parquet_write_modes")

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


@pytest.mark.parametrize(
    ("physical_schema", "requested_schema", "value"),
    [
        ("value DOUBLE", "value INT", 1.5),
        ("value INT", "value BOOLEAN", 1),
        ("value STRUCT<nested: DOUBLE>", "value STRUCT<nested: INT>", Row(nested=1.5)),
        ("value ARRAY<DOUBLE>", "value ARRAY<INT>", [1.5]),
        ("value MAP<STRING, DOUBLE>", "value MAP<STRING, INT>", {"key": 1.5}),
    ],
)
def test_parquet_explicit_schema_rejects_incompatible_types(spark, tmp_path, physical_schema, requested_schema, value):
    path = str(tmp_path / "incompatible_explicit_schema")
    spark.createDataFrame([(value,)], physical_schema).write.parquet(path)

    with pytest.raises(Exception, match="PARQUET_COLUMN_DATA_TYPE_MISMATCH"):
        spark.read.schema(requested_schema).parquet(path).collect()


def test_parquet_explicit_schema_allows_supported_widening(spark, tmp_path):
    path = str(tmp_path / "supported_explicit_schema_widening")
    spark.createDataFrame([(1, 1.5), (None, None)], "id INT, value FLOAT").write.parquet(path)

    rows = spark.read.schema("id BIGINT, value DOUBLE").parquet(path).orderBy("id").collect()

    assert rows == [Row(id=None, value=None), Row(id=1, value=1.5)]


def test_parquet_explicit_schema_allows_missing_fields(spark, tmp_path):
    path = str(tmp_path / "missing_explicit_schema_field")
    spark.createDataFrame([(1,)], "id INT").write.parquet(path)

    rows = spark.read.schema("id INT, missing STRING").parquet(path).collect()

    assert rows == [Row(id=1, missing=None)]


@pytest.mark.parametrize("requested_type", ["TIMESTAMP", "TIMESTAMP_NTZ"])
def test_parquet_explicit_schema_determines_int96_timestamp_family(spark, tmp_path, requested_type):
    path = tmp_path / "int96_explicit_timestamp_schema.parquet"
    table = pa.table(
        {
            "value": pa.array(
                [datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)],
                type=pa.timestamp("us"),
            )
        }
    )
    pq.write_table(table, path, use_deprecated_int96_timestamps=True)

    rows = (
        spark.read.schema(f"value {requested_type}")
        .parquet(str(path))
        .selectExpr("CAST(value AS STRING) AS value")
        .collect()
    )

    assert rows == [Row(value="2024-01-02 03:04:05")]


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
    # Note: Sail's writer drops trailing zeros when stringifying a DOUBLE
    # partition value (`3.0` → `"3"`). Spark would write `"3.0"`. Pinned
    # against Sail's current behavior.
    assert rows == [
        Row(id=1, val="a", int_part="2024", float_part="1.5", string_part="alpha"),
        Row(id=2, val="b", int_part="2024", float_part="2.5", string_part="beta"),
        Row(id=3, val="c", int_part="2025", float_part="3", string_part="alpha"),
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


def test_parquet_arithmetic_operand_rejection(spark, tmp_path):
    # Fixed-size binary and unsigned integers have no SQL literal spelling, so
    # a file is the only way they reach the arithmetic operand guards.
    path = str(tmp_path / "arithmetic_operands.parquet")
    pq.write_table(
        pa.table(
            {
                "fsb": pa.array([b"abcd"], pa.binary(4)),
                "u8": pa.array([1], pa.uint8()),
                "u16": pa.array([1], pa.uint16()),
                "u32": pa.array([1], pa.uint32()),
                "u64": pa.array([1], pa.uint64()),
            }
        ),
        path,
    )
    spark.read.parquet(path).createOrReplaceTempView("arithmetic_operands")

    # BINARY is not one of the input types any operator accepts.
    for op in ["+", "-", "*", "/", "%"]:
        with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
            spark.sql(f"SELECT fsb {op} 1 FROM arithmetic_operands").collect()  # noqa: S608

    # Spark reads UINT_32 as BIGINT and UINT_64 as DECIMAL(20,0), and `DateAdd` takes only a
    # BYTE, SHORT or INT offset (`datetimeExpressions.scala:331-332`, and it is
    # `ExpectsInputTypes`), so it rejects both. Sail used to accept `u32` because its date offset
    # rule was widened for functions it typed BIGINT; they carry Spark's INT now, the rule is
    # `DateAdd`'s own, and the engines agree on every width.
    for column in ["u8", "u16"]:
        query = f"SELECT DATE'2024-01-01' + {column} AS r FROM arithmetic_operands"  # noqa: S608
        assert spark.sql(query).collect() == [Row(r=date(2024, 1, 2))]
    for column in ["u32", "u64"]:
        query = f"SELECT DATE'2024-01-01' + {column} AS r FROM arithmetic_operands"  # noqa: S608
        with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
            spark.sql(query).collect()

    # The same unsigned columns stay usable in ordinary numeric arithmetic.
    assert spark.sql("SELECT u32 * 2 AS r FROM arithmetic_operands").collect() == [Row(r=2)]


@pytest.mark.parametrize(
    ("column", "spark_type"),
    [("u8", "SMALLINT"), ("u16", "INT"), ("u32", "BIGINT"), ("u64", "DECIMAL(20,0)")],
)
def test_parquet_unsigned_operand_is_named_by_its_spark_type(spark, tmp_path, column, spark_type):
    # Spark has no unsigned types: its Parquet reader widens each unsigned width one step
    # so every value stays representable (`ParquetSchemaConverter.scala:290,311`), and the
    # arithmetic error names that widened type.
    path = str(tmp_path / "unsigned_operand.parquet")
    pq.write_table(
        pa.table(
            {
                "u8": pa.array([1], pa.uint8()),
                "u16": pa.array([1], pa.uint16()),
                "u32": pa.array([1], pa.uint32()),
                "u64": pa.array([1], pa.uint64()),
                "flag": pa.array([True], pa.bool_()),
            }
        ),
        path,
    )
    spark.read.parquet(path).createOrReplaceTempView("unsigned_operand")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"SELECT flag + {column} FROM unsigned_operand").collect()  # noqa: S608
    assert f"BOOLEAN and {spark_type}" in str(excinfo.value).replace('"', "")


@pytest.mark.parametrize("op", ["+", "-"])
def test_parquet_uint32_date_offset_is_named_bigint(spark, tmp_path, op):
    # Spark reads a UINT_32 column as BIGINT, which `DateAdd` refuses, and names it BIGINT in the
    # message.
    path = str(tmp_path / "uint32_offset.parquet")
    pq.write_table(pa.table({"u32": pa.array([1], pa.uint32())}), path)
    spark.read.parquet(path).createOrReplaceTempView("uint32_offset")
    with pytest.raises(AnalysisException, match="BIGINT"):
        spark.sql(f"SELECT DATE'2024-01-01' {op} u32 FROM uint32_offset").collect()  # noqa: S608


# TODO: Sail reads a BINARY `overlay` input as a STRING until its string functions take a BINARY
#   (see `binary_substring.feature`); a BINARY result broke them downstream.
@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="a BINARY overlay is read as a STRING")
def test_parquet_binary_overlay_stays_a_binary_cut_by_bytes(spark, tmp_path):
    # A Parquet scan reads BINARY as an Arrow `BinaryView`. `Overlay` over a BINARY is a BINARY cut by
    # bytes (`stringExpressions.scala:1000-1010`), bytes that are not valid UTF-8 included, and a
    # BINARY is not an arithmetic operand.
    path = str(tmp_path / "binary_overlay.parquet")
    pq.write_table(pa.table({"b": pa.array([b"\xff\x00Spark"], pa.binary()), "r": pa.array([b"_"], pa.binary())}), path)
    spark.read.parquet(path).createOrReplaceTempView("binary_overlay")
    row = spark.sql(
        "SELECT typeof(overlay(b PLACING r FROM 2)) AS t, hex(overlay(b PLACING r FROM 2)) AS h FROM binary_overlay"
    ).collect()
    assert row == [Row(t="binary", h="FF5F537061726B")]
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql("SELECT 2 / overlay(b PLACING r FROM 2) AS r FROM binary_overlay").collect()


def test_parquet_date_minus_a_zoned_timestamp_uses_the_session_time_zone(spark, tmp_path):
    # A DATE subtracted with a TIMESTAMP is read as midnight in the SESSION time zone
    # (`SubtractTimestamps`), whatever zone the Parquet column was written with. The column instant is
    # 06:00 UTC, which is 22:00 the day before in Los Angeles, so the date is two hours after it.
    path = str(tmp_path / "zoned_timestamp.parquet")
    instant = pa.array([datetime(2024, 1, 15, 6, 0, tzinfo=UTC)], pa.timestamp("us", tz="America/New_York"))
    pq.write_table(pa.table({"d": pa.array([date(2024, 1, 15)], pa.date32()), "ts": instant}), path)
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        spark.read.parquet(path).createOrReplaceTempView("zoned_timestamp")
        row = spark.sql(
            "SELECT CAST(d - ts AS STRING) AS a, CAST(ts - d AS STRING) AS b FROM zoned_timestamp"
        ).collect()
        assert row == [Row(a="INTERVAL '0 02:00:00' DAY TO SECOND", b="INTERVAL '-0 02:00:00' DAY TO SECOND")]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


def test_parquet_uint64_plus_a_string_is_a_double_with_ansi_on(spark, tmp_path):
    # Spark reads UINT_64 as DECIMAL(20,0), and a string beside a DECIMAL is promoted to DOUBLE
    # (`AnsiStringPromotionTypeCoercion.findWiderTypeForString`), so a value past BIGINT still answers.
    path = str(tmp_path / "uint64_string.parquet")
    pq.write_table(pa.table({"u": pa.array([18446744073709551000], pa.uint64())}), path)
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        spark.read.parquet(path).createOrReplaceTempView("uint64_string")
        row = spark.sql("SELECT typeof(u + '1') AS t, u + '1' AS v FROM uint64_string").collect()
        assert row == [Row(t="double", v=1.8446744073709552e19)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def test_parquet_binary_substring_feeds_string_functions(spark, tmp_path):
    # `substr`/`left`/`overlay` of a BINARY read from Parquet feed `hex`, `trim`, `replace` and
    # `initcap` in Spark (`stringExpressions.scala:2301-2313`, `mathExpressions.scala:1195-1196`).
    path = str(tmp_path / "parquet_binary_substring")
    spark.sql("SELECT X'2061626364' AS b").write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("parquet_binary_substring")
    try:
        row = spark.sql(
            "SELECT hex(substr(b, 2)) AS h, trim(substr(b, 1, 3)) AS t, replace(left(b, 3), 'a', 'z') AS r, "
            "initcap(overlay(b PLACING X'78' FROM 1)) AS i FROM parquet_binary_substring"
        ).collect()
        assert row == [Row(h="61626364", t="ab", r=" zb", i="Xabcd")]
    finally:
        spark.catalog.dropTempView("parquet_binary_substring")
