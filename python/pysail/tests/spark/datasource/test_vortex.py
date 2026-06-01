"""Tests for the Vortex Python DataSource.

Requires the ``vortex-data`` package to be installed.
All tests are skipped if ``vortex-data`` is not available.
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

try:
    from pyspark.sql.datasource import EqualTo as _EqualTo  # noqa: F401
except ImportError:
    pytest.skip(
        "Python DataSource API with filter pushdown not available (requires PySpark 4.1+)", allow_module_level=True
    )

try:
    import vortex
except ImportError:
    pytest.skip("vortex-data not installed", allow_module_level=True)


def safe_sort_key(row):
    if isinstance(row, Mapping):
        return tuple((v is not None, v) for _, v in sorted(row.items()))
    return tuple((v is not None, v) for v in row)


@pytest.fixture
def vtx_file(tmp_path):
    """Create a temporary Vortex file with sample data."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["alice", "bob", "carol", "dave", "eve"], type=pa.string()),
            "score": pa.array([90.5, 85.0, 92.3, 78.1, 88.7], type=pa.float64()),
        }
    )
    path = str(tmp_path / "test.vortex")
    vortex.io.write(table, path)
    return path


@pytest.fixture
def nested_vtx_file(tmp_path):
    """Create a temporary Vortex file inside nested subdirectories."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["alice", "bob", "carol", "dave", "eve"], type=pa.string()),
            "score": pa.array([90.5, 85.0, 92.3, 78.1, 88.7], type=pa.float64()),
        }
    )
    parent_dir = tmp_path / "sub_dir_1" / "sub_dir_2"
    parent_dir.mkdir(parents=True, exist_ok=True)
    path = str(parent_dir / "test.vortex")
    vortex.io.write(table, path)
    return tmp_path, path


@pytest.fixture
def vtx_file_with_nulls(tmp_path):
    """Create a Vortex file with null values."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, None, 5], type=pa.int64()),
            "name": pa.array(["alice", None, "carol", "dave", None], type=pa.string()),
        }
    )
    path = str(tmp_path / "nulls.vortex")
    vortex.io.write(table, path)
    return path


@pytest.fixture
def other_df(spark):
    """A different DataFrame from sample_df with the same schema."""
    table = pa.table(
        {
            "col1": pa.array(["alice", "carol"], type=pa.string()),
            "col2": pa.array([1, 2], type=pa.int64()),
        }
    )
    return spark.createDataFrame(table)


@pytest.fixture(autouse=True)
def _register_vortex(spark):
    """Register VortexDataSource for all tests."""
    from pysail.spark.datasource.vortex import VortexDataSource

    spark.dataSource.register(VortexDataSource)


def test_vortex_read_all_rows(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    assert df.count() == 5  # noqa: PLR2004


def test_vortex_read_all_rows_from_nested_directory(spark, nested_vtx_file):
    vtx_directory = nested_vtx_file[0]
    df = spark.read.format("vortex").option("path", vtx_directory).load()
    assert df.count() == 5  # noqa: PLR2004


def test_vortex_read_all_rows_using_glob_pattern(spark, nested_vtx_file):
    vtx_pattern = f"{nested_vtx_file[0]}/**/*.vortex"
    df = spark.read.format("vortex").option("path", vtx_pattern).load()
    assert df.count() == 5  # noqa: PLR2004


def test_vortex_write_basic(spark, sample_df, tmp_path):
    path = str(tmp_path / "vortex_write_basic")
    sample_df.write.format("vortex").option("path", path).save()
    read_df = spark.read.format("vortex").option("path", path).load()
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_vortex_write_error_mode(spark, sample_df, other_df, tmp_path):
    path = str(tmp_path / "vortex_write_error_mode")
    sample_df.write.format("vortex").option("path", path).save()
    with pytest.raises(
        Exception, match=f"Path '{path}' already exists. Use mode='overwrite' to replace or mode='append' to add to it."
    ):
        other_df.write.format("vortex").option("path", path).save()
    read_df = spark.read.format("vortex").option("path", path).load()
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_vortex_write_ignore_mode(spark, sample_df, other_df, tmp_path):
    path = str(tmp_path / "vortex_write_ignore_mode")
    sample_df.write.format("vortex").option("path", path).save()
    other_df.write.mode("ignore").format("vortex").option("path", path).save()
    read_df = spark.read.format("vortex").option("path", path).load()
    assert sample_df.count() == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_vortex_write_overwrite_mode(spark, sample_df, other_df, tmp_path):
    path = str(tmp_path / "vortex_write_overwrite_mode")
    sample_df.write.format("vortex").option("path", path).save()
    other_df.write.mode("overwrite").format("vortex").option("path", path).save()
    read_df = spark.read.format("vortex").option("path", path).load()
    assert other_df.count() == read_df.count()
    assert other_df.schema == read_df.schema
    assert sorted(other_df.collect(), key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_vortex_write_append_mode(spark, sample_df, tmp_path):
    path = str(tmp_path / "vortex_write_append_mode")
    sample_df.write.format("vortex").option("path", path).save()
    sample_df.write.mode("append").format("vortex").option("path", path).save()
    read_df = spark.read.format("vortex").option("path", path).load()
    assert sample_df.count() * 2 == read_df.count()
    assert sample_df.schema == read_df.schema
    assert sorted(sample_df.collect() * 2, key=safe_sort_key) == sorted(read_df.collect(), key=safe_sort_key)


def test_vortex_read_schema(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert fields["id"] == "bigint"
    assert fields["name"] == "string"
    assert fields["score"] == "double"


def test_vortex_read_values(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    rows = df.orderBy("id").collect()
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "alice"
    assert rows[4]["name"] == "eve"


def test_vortex_select_columns(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    names = [row["name"] for row in df.select("name").orderBy("name").collect()]
    assert names == ["alice", "bob", "carol", "dave", "eve"]


def test_vortex_equality_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.id == 3).collect()  # noqa: PLR2004
    assert len(result) == 1
    assert result[0]["name"] == "carol"


def test_vortex_greater_than_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.id > 3).orderBy("id").collect()  # noqa: PLR2004
    assert len(result) == 2  # noqa: PLR2004
    assert [r["id"] for r in result] == [4, 5]


def test_vortex_less_than_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.id < 3).orderBy("id").collect()  # noqa: PLR2004
    assert len(result) == 2  # noqa: PLR2004
    assert [r["id"] for r in result] == [1, 2]


def test_vortex_combined_filters(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter((df.id >= 2) & (df.id <= 4)).orderBy("id").collect()  # noqa: PLR2004
    assert len(result) == 3  # noqa: PLR2004
    assert [r["id"] for r in result] == [2, 3, 4]


def test_vortex_string_equality_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.name == "bob").collect()
    assert len(result) == 1
    assert result[0]["id"] == 2  # noqa: PLR2004


def test_vortex_in_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.id.isin(1, 3, 5)).orderBy("id").collect()
    assert len(result) == 3  # noqa: PLR2004
    assert [r["id"] for r in result] == [1, 3, 5]


def test_vortex_not_equal_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(df.id != 3).orderBy("id").collect()  # noqa: PLR2004
    assert len(result) == 4  # noqa: PLR2004
    assert [r["id"] for r in result] == [1, 2, 4, 5]


def test_vortex_not_in_filter(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.filter(~df.id.isin(2, 4)).orderBy("id").collect()
    assert len(result) == 3  # noqa: PLR2004
    assert [r["id"] for r in result] == [1, 3, 5]


def test_vortex_read_with_nulls(spark, vtx_file_with_nulls):
    df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
    assert df.count() == 5  # noqa: PLR2004


def test_vortex_null_filter(spark, vtx_file_with_nulls):
    df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
    result = df.filter(df.name.isNull()).collect()
    assert len(result) == 2  # noqa: PLR2004


def test_vortex_not_null_filter(spark, vtx_file_with_nulls):
    df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
    result = df.filter(df.name.isNotNull()).collect()
    assert len(result) == 3  # noqa: PLR2004


def test_vortex_missing_path_option(spark):
    with pytest.raises(Exception, match="path"):
        spark.read.format("vortex").load().collect()


def test_vortex_nonexistent_file(spark, tmp_path):
    path = str(tmp_path / "nonexistent.vortex")
    with pytest.raises(Exception, match=f"No Vortex files found in the specified path: '{path}'"):
        spark.read.format("vortex").option("path", path).load().collect()


def test_vortex_sql_query(spark, vtx_file):
    spark.read.format("vortex").option("path", vtx_file).load().createOrReplaceTempView("vtx_table")
    result = spark.sql("SELECT * FROM vtx_table WHERE id > 3 ORDER BY id").collect()
    assert len(result) == 2  # noqa: PLR2004
    assert [r["id"] for r in result] == [4, 5]


def test_vortex_aggregation(spark, vtx_file):
    df = spark.read.format("vortex").option("path", vtx_file).load()
    result = df.agg({"score": "avg"}).collect()
    avg_score = result[0][0]
    assert abs(avg_score - 86.92) < 0.01  # noqa: PLR2004
