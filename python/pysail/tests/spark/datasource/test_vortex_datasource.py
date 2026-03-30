"""Tests for the Vortex Python DataSource.

Requires the ``vortex-data`` package to be installed.
All tests are skipped if ``vortex-data`` is not available.
"""

from __future__ import annotations

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


@pytest.fixture(autouse=True)
def _register_vortex(spark):
    """Register VortexDataSource for all tests."""
    from pysail.spark.datasource.vortex import VortexDataSource

    spark.dataSource.register(VortexDataSource)


class TestVortexBasicRead:
    def test_read_all_rows(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        assert df.count() == 5  # noqa: PLR2004

    def test_read_schema(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert fields["id"] == "bigint"
        assert fields["name"] == "string"
        assert fields["score"] == "double"

    def test_read_values(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        rows = df.orderBy("id").collect()
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "alice"
        assert rows[4]["name"] == "eve"

    def test_select_columns(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        names = [row["name"] for row in df.select("name").orderBy("name").collect()]
        assert names == ["alice", "bob", "carol", "dave", "eve"]


class TestVortexFilterPushdown:
    def test_equality_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.id == 3).collect()  # noqa: PLR2004
        assert len(result) == 1
        assert result[0]["name"] == "carol"

    def test_greater_than_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.id > 3).orderBy("id").collect()  # noqa: PLR2004
        assert len(result) == 2  # noqa: PLR2004
        assert [r["id"] for r in result] == [4, 5]

    def test_less_than_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.id < 3).orderBy("id").collect()  # noqa: PLR2004
        assert len(result) == 2  # noqa: PLR2004
        assert [r["id"] for r in result] == [1, 2]

    def test_combined_filters(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter((df.id >= 2) & (df.id <= 4)).orderBy("id").collect()  # noqa: PLR2004
        assert len(result) == 3  # noqa: PLR2004
        assert [r["id"] for r in result] == [2, 3, 4]

    def test_string_equality_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.name == "bob").collect()
        assert len(result) == 1
        assert result[0]["id"] == 2  # noqa: PLR2004

    def test_in_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.id.isin(1, 3, 5)).orderBy("id").collect()
        assert len(result) == 3  # noqa: PLR2004
        assert [r["id"] for r in result] == [1, 3, 5]

    def test_not_equal_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(df.id != 3).orderBy("id").collect()  # noqa: PLR2004
        assert len(result) == 4  # noqa: PLR2004
        assert [r["id"] for r in result] == [1, 2, 4, 5]

    def test_not_in_filter(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.filter(~df.id.isin(2, 4)).orderBy("id").collect()
        assert len(result) == 3  # noqa: PLR2004
        assert [r["id"] for r in result] == [1, 3, 5]


class TestVortexNullHandling:
    def test_read_with_nulls(self, spark, vtx_file_with_nulls):
        df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
        assert df.count() == 5  # noqa: PLR2004

    def test_null_filter(self, spark, vtx_file_with_nulls):
        df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
        result = df.filter(df.name.isNull()).collect()
        assert len(result) == 2  # noqa: PLR2004

    def test_not_null_filter(self, spark, vtx_file_with_nulls):
        df = spark.read.format("vortex").option("path", vtx_file_with_nulls).load()
        result = df.filter(df.name.isNotNull()).collect()
        assert len(result) == 3  # noqa: PLR2004


class TestVortexEdgeCases:
    def test_missing_path_option(self, spark):
        with pytest.raises(Exception, match="path"):
            spark.read.format("vortex").load().collect()

    def test_nonexistent_file(self, spark, tmp_path):
        path = str(tmp_path / "nonexistent.vortex")
        with pytest.raises(Exception, match="Failed to open Vortex file"):
            spark.read.format("vortex").option("path", path).load().collect()

    def test_sql_query(self, spark, vtx_file):
        spark.read.format("vortex").option("path", vtx_file).load().createOrReplaceTempView("vtx_table")
        result = spark.sql("SELECT * FROM vtx_table WHERE id > 3 ORDER BY id").collect()
        assert len(result) == 2  # noqa: PLR2004
        assert [r["id"] for r in result] == [4, 5]

    def test_aggregation(self, spark, vtx_file):
        df = spark.read.format("vortex").option("path", vtx_file).load()
        result = df.agg({"score": "avg"}).collect()
        avg_score = result[0][0]
        assert abs(avg_score - 86.92) < 0.01  # noqa: PLR2004
