"""Tests for Spark Catalog API field names.

PySpark maps tableName from the server to name in the client Table namedtuple.
The camelCase fields (tableType, isTemporary) are passed through directly.
"""

from pathlib import Path

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


class TestListTables:
    """Test spark.catalog.listTables() returns correct field names and values."""

    @pytest.fixture(autouse=True)
    def setup_view(self, spark):
        """Create a temporary view for testing."""
        spark.sql("SELECT 1 AS col").createOrReplaceTempView("test_view")
        yield
        spark.catalog.dropTempView("test_view")

    def test_list_tables_returns_name(self, spark):
        """listTables returns table with 'name' field (mapped from tableName)."""
        tables = spark.catalog.listTables()
        table_names = [t.name for t in tables]
        assert "test_view" in table_names

    def test_list_tables_returns_table_type(self, spark):
        """listTables returns tableType in camelCase."""
        tables = spark.catalog.listTables()
        test_table = next(t for t in tables if t.name == "test_view")
        assert test_table.tableType == "TEMPORARY"

    def test_list_tables_returns_is_temporary(self, spark):
        """listTables returns isTemporary in camelCase."""
        tables = spark.catalog.listTables()
        test_table = next(t for t in tables if t.name == "test_view")
        assert test_table.isTemporary is True

    def test_show_tables_returns_spark_sql_shape(self, spark):
        """SHOW TABLES returns the Spark SQL 3-column output shape."""
        tables = spark.sql("SHOW TABLES")
        assert tables.columns == ["database", "tableName", "isTemporary"]

        test_table = next(row for row in tables.collect() if row.tableName == "test_view")
        assert test_table.isTemporary is True

    def test_show_table_extended_returns_spark_sql_shape(self, spark):
        """SHOW TABLE EXTENDED returns the Spark SQL 4-column output shape."""
        tables = spark.sql("SHOW TABLE EXTENDED LIKE 'test_view'")
        assert tables.columns == ["database", "tableName", "isTemporary", "information"]

        test_table = next(row for row in tables.collect() if row.tableName == "test_view")
        assert test_table.isTemporary is True
        assert "Type: TEMPORARY" in test_table.information
        assert "Schema: root" in test_table.information
        assert " |-- col: int (nullable = " in test_table.information

    @pytest.mark.parametrize(
        "sql",
        [
            "DESCRIBE TABLE EXTENDED test_view",
            "DESCRIBE EXTENDED test_view",
        ],
    )
    def test_describe_extended_accepts_long_and_short_forms(self, spark, sql):
        """DESCRIBE EXTENDED accepts both Spark table forms."""
        describe = spark.sql(sql)
        assert describe.columns == ["col_name", "data_type", "comment"]

        rows = describe.collect()
        column_row = next(row for row in rows if row.col_name == "col")
        assert column_row.data_type == "int"

        metadata_marker = next(row for row in rows if row.col_name == "# Detailed Table Information")
        assert metadata_marker.data_type == ""

    @pytest.mark.catalog_integration
    def test_persistent_table_defaults_to_managed(self, spark):
        """Persistent tables without an explicit location are managed."""
        table_name = "test_external_default"
        try:
            spark.sql(f"CREATE TABLE {table_name} (id INT) USING PARQUET")

            table = spark.catalog.getTable(table_name)
            assert table.tableType == "MANAGED"

            show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
            show_row = next(row for row in show_rows if row.tableName == table_name)
            assert "Type: MANAGED" in show_row.information

            describe_rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            type_row = next(row for row in describe_rows if row.col_name == "Type")
            assert type_row.data_type == "MANAGED"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    @pytest.mark.catalog_integration
    def test_persistent_table_with_location_is_external(self, spark, tmp_path):
        """Persistent table created with LOCATION surfaces EXTERNAL type."""
        table_name = "test_external_with_location"
        location = str(tmp_path / table_name)
        Path(location).mkdir(parents=True, exist_ok=True)
        try:
            spark.sql(
                f"CREATE TABLE {table_name} (id INT) USING PARQUET LOCATION '{escape_sql_string_literal(location)}'"
            )

            table = spark.catalog.getTable(table_name)
            assert table.tableType == "EXTERNAL"

            show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
            show_row = next(row for row in show_rows if row.tableName == table_name)
            assert "Type: EXTERNAL" in show_row.information

            describe_rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            type_row = next(row for row in describe_rows if row.col_name == "Type")
            assert type_row.data_type == "EXTERNAL"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_show_tblproperties_for_temp_view_returns_empty(self, spark):
        """SHOW TBLPROPERTIES on a temporary view should return zero rows."""
        rows = spark.sql("SHOW TBLPROPERTIES test_view").collect()
        assert rows == []

    def test_show_tblproperties_reserved_key_lookup_behaves_like_missing_property(self, spark):
        """Reserved-key lookup should return the Spark missing-property message."""
        table_name = "test_tblproperties_reserved_lookup"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT) USING PARQUET "
            "TBLPROPERTIES ('custom.b' = '2', 'custom.a' = '1')"
        )
        try:
            rows = spark.sql(f"SHOW TBLPROPERTIES {table_name} ('provider')").collect()
            assert len(rows) == 1
            assert rows[0]["key"] == "provider"
            value = rows[0]["value"]
            assert value.endswith(
                f".default.{table_name} does not have property: provider"
            )
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")


class TestListDatabases:
    """Test spark.catalog.listDatabases() returns correct field names."""

    def test_list_databases_returns_name(self, spark):
        """listDatabases returns database with 'name' field."""
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "default" in db_names


class TestDeltaMetadataHydration:
    """Test that catalog commands show storage-level metadata for Delta tables.

    These tests exercise hydrate_table_status → load_storage_table_metadata
    through the spark.sql() path against real Delta storage.
    """

    def test_describe_extended_delta_table_shows_columns(self, spark, tmp_path):
        """DESCRIBE EXTENDED on a Delta table shows storage-level columns."""
        table_name = "test_describe_delta_cols"
        delta_path = tmp_path / "delta_cols"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, name STRING) "
            f"USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            col_names = [r.col_name for r in rows if r.col_name and not r.col_name.startswith("#")]
            assert "id" in col_names
            assert "name" in col_names
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_describe_extended_delta_table_shows_provider(self, spark, tmp_path):
        """DESCRIBE EXTENDED on a Delta table shows Provider: delta."""
        table_name = "test_describe_delta_provider"
        delta_path = tmp_path / "delta_provider"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT) "
            f"USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            metadata = {r.col_name: r.data_type for r in rows}
            assert metadata.get("Provider") == "delta"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_show_tblproperties_delta_table(self, spark, tmp_path):
        """SHOW TBLPROPERTIES on a Delta table shows storage-level properties."""
        table_name = "test_show_tblprops_delta"
        delta_path = tmp_path / "delta_props"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT) "
            f"USING DELTA "
            f"TBLPROPERTIES ('custom.key' = 'hello') "
            f"LOCATION '{delta_path}'"
        )
        try:
            # Ensure Delta metadata files exist in environments that initialize
            # _delta_log lazily on first write.
            spark.sql(f"INSERT INTO {table_name} VALUES (1)")
            rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            # SHOW TBLPROPERTIES returns (key, value) columns
            assert len(rows) >= 1
            props = {r["key"]: r["value"] for r in rows}
            assert props.get("custom.key") == "hello"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_describe_extended_partitioned_delta_table(self, spark, tmp_path):
        """DESCRIBE EXTENDED on a partitioned Delta table shows partition info."""
        table_name = "test_describe_delta_partitioned"
        delta_path = tmp_path / "delta_partitioned"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, region STRING) "
            f"USING DELTA PARTITIONED BY (region) "
            f"LOCATION '{delta_path}'"
        )
        try:
            # Verify partition column is flagged in column list
            rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            col_rows = {r.col_name: r.data_type for r in rows if r.col_name and not r.col_name.startswith("#")}
            assert "region" in col_rows
            assert "id" in col_rows
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_show_table_extended_delta_table(self, spark, tmp_path):
        """SHOW TABLE EXTENDED on a Delta table shows storage-level information."""
        table_name = "test_show_extended_delta"
        delta_path = tmp_path / "delta_extended"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, value DOUBLE) "
            f"USING DELTA COMMENT 'a test table' "
            f"LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
            assert len(rows) >= 1
            info = rows[0].information
            assert "Provider: delta" in info
            assert "Schema: root" in info
            assert "id:" in info
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_catalog_list_columns_delta_table(self, spark, tmp_path):
        """spark.catalog.listColumns() on a Delta table returns storage-level columns."""
        table_name = "test_list_columns_delta"
        delta_path = tmp_path / "delta_list_cols"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, name STRING, score DOUBLE) "
            f"USING DELTA LOCATION '{delta_path}'"
        )
        try:
            columns = spark.catalog.listColumns(table_name)
            col_names = [c.name for c in columns]
            assert "id" in col_names
            assert "name" in col_names
            assert "score" in col_names
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_catalog_list_columns_partitioned_delta_table(self, spark, tmp_path):
        """spark.catalog.listColumns() flags partition columns on a Delta table."""
        table_name = "test_list_columns_delta_part"
        delta_path = tmp_path / "delta_list_cols_part"
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, region STRING) "
            f"USING DELTA PARTITIONED BY (region) "
            f"LOCATION '{delta_path}'"
        )
        try:
            columns = spark.catalog.listColumns(table_name)
            col_by_name = {c.name: c for c in columns}
            assert "region" in col_by_name
            assert "id" in col_by_name
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
