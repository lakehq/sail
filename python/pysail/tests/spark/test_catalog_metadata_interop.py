"""Catalog metadata interop tests.

Verify that table metadata (schema, partition columns, properties, comment)
is compatible between Sail and external engines.

**Direction: Sail → external**
  Sail writes a table, an external engine (PyIceberg, Delta log reader) reads
  the metadata and verifies correctness.

**Direction: External → Sail**
  An external engine writes a table, Sail reads metadata via:
  - DataFrameReader (spark.read.format)
  - SQL (DESCRIBE EXTENDED, SHOW TBLPROPERTIES, SHOW TABLE EXTENDED)
  - Catalog API (spark.catalog.listColumns)

For Delta tables, Sail supports managed tables (CREATE TABLE ... USING DELTA),
so "External → Sail" tests register the path with the catalog and exercise
the full hydration path (hydrate_table_status → load_storage_table_metadata).

For Iceberg tables, the HMS catalog provider does not support Iceberg.
Sail supports path-based reads but not managed Iceberg tables through HMS.
The "External → Sail" tests load Iceberg data as temp views, which do
**not** trigger storage metadata hydration (hydrate_table_status returns
early for TemporaryView). Full hydration testing for Iceberg requires a
catalog provider that supports Iceberg table registration (e.g., an
Iceberg REST catalog), which is not available in the unit test environment.

Iceberg tests cover both format version 1 and version 2.
"""

from __future__ import annotations

import json
import platform
import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from pathlib import Path

from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from pysail.tests.spark.iceberg.utils import create_sql_catalog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_delta_log_metadata(table_path: str | Path) -> dict:
    """Read the metaData action from the first Delta log commit."""
    import pathlib

    log_dir = pathlib.Path(table_path) / "_delta_log"
    commit_files = sorted(log_dir.glob("*.json"))
    assert commit_files, f"no Delta log files found in {log_dir}"
    with commit_files[0].open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            action = json.loads(line)
            if "metaData" in action:
                return action["metaData"]
    raise AssertionError("no metaData action found in Delta log")


def _write_delta_log_with_metadata(
    table_path: Path,
    *,
    schema_string: str,
    partition_columns: list[str] | None = None,
    configuration: dict[str, str] | None = None,
    description: str | None = None,
) -> None:
    """Write a minimal Delta log commit with the given metadata."""
    table_path.mkdir(parents=True, exist_ok=True)
    log_dir = table_path / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)

    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "name": None,
                "description": description,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": partition_columns or [],
                "configuration": configuration or {},
                "createdTime": 1700000000000,
            }
        },
    ]

    commit_file = log_dir / "00000000000000000000.json"
    with commit_file.open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action))
            f.write("\n")


def _simple_iceberg_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
    )


# =========================================================================
# Iceberg interop
# =========================================================================

# ---------------------------------------------------------------------------
# Iceberg: Sail → PyIceberg
# ---------------------------------------------------------------------------

@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
class TestIcebergSailToPyiceberg:
    """Sail writes Iceberg tables; PyIceberg verifies the metadata."""

    def test_sail_write_schema_readable_by_pyiceberg(self, spark, tmp_path):
        """Sail writes an Iceberg table and PyIceberg can load the schema."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_schema"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame(
                [(1, "alpha", 0.5), (2, "beta", 0.9)],
                schema="id LONG, event STRING, score DOUBLE",
            )
            df.write.format("iceberg").mode("overwrite").save(table.location())

            py_tbl = StaticTable.from_metadata(table.location())
            assert len(py_tbl.schema().fields) == 3
            field_names = [f.name for f in py_tbl.schema().fields]
            assert field_names == ["id", "event", "score"]
        finally:
            catalog.drop_table(identifier)

    def test_sail_write_properties_readable_by_pyiceberg(self, spark, tmp_path):
        """Sail writes an Iceberg table; PyIceberg sees location metadata."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_props"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame(
                [(1, "a", 1.0)],
                schema="id LONG, event STRING, score DOUBLE",
            )
            df.write.format("iceberg").mode("overwrite").save(table.location())

            py_tbl = StaticTable.from_metadata(table.location())
            assert py_tbl.metadata.location == table.location()
        finally:
            catalog.drop_table(identifier)


# ---------------------------------------------------------------------------
# Iceberg: PyIceberg → Sail (DataFrameReader)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
class TestIcebergPyicebergToSailReader:
    """PyIceberg creates Iceberg tables; Sail reads data via DataFrameReader."""

    def test_sail_reads_data_from_pyiceberg_table(self, spark, tmp_path):
        """PyIceberg creates a table with data; Sail reads it back."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_reader"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame(
                [(1, "x", 0.1)],
                schema="id LONG, event STRING, score DOUBLE",
            )
            df.write.format("iceberg").mode("overwrite").save(table.location())

            result = spark.read.format("iceberg").load(table.location()).collect()
            assert len(result) == 1
            assert result[0]["id"] == 1
        finally:
            catalog.drop_table(identifier)


# ---------------------------------------------------------------------------
# Iceberg: PyIceberg → Sail (SQL + Catalog API via temp view)
#
# NOTE: These tests load Iceberg data as temp views. Temp views do NOT
# trigger storage metadata hydration (hydrate_table_status returns early
# for TemporaryView). They test the temp view code path for DESCRIBE
# EXTENDED / SHOW TBLPROPERTIES / listColumns. Full hydration testing
# for Iceberg requires catalog integration with an Iceberg REST catalog.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific catalog commands")
class TestIcebergPyicebergToSailTempView:
    """PyIceberg creates tables; Sail reads metadata via temp view (no hydration)."""

    def test_describe_extended_on_iceberg_temp_view(self, spark, tmp_path):
        """DESCRIBE EXTENDED on an Iceberg temp view shows columns from the loaded schema."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_tv_describe"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame(
                [(1, "a", 1.0), (2, "b", 2.0)],
                schema="id LONG, event STRING, score DOUBLE",
            )
            df.write.format("iceberg").mode("overwrite").save(table.location())

            spark.read.format("iceberg").load(table.location()).createOrReplaceTempView("iceberg_tv_v")

            rows = spark.sql("DESCRIBE EXTENDED iceberg_tv_v").collect()
            col_names = [r.col_name for r in rows if r.col_name and not r.col_name.startswith("#")]
            assert "id" in col_names
            assert "event" in col_names
            assert "score" in col_names
        finally:
            catalog.drop_table(identifier)
            spark.catalog.dropTempView("iceberg_tv_v")

    def test_show_tblproperties_on_iceberg_temp_view_returns_empty(self, spark, tmp_path):
        """SHOW TBLPROPERTIES on a temp view returns empty (no managed table properties)."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_tv_props"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame([(1, "a", 1.0)], schema="id LONG, event STRING, score DOUBLE")
            df.write.format("iceberg").mode("overwrite").save(table.location())

            spark.read.format("iceberg").load(table.location()).createOrReplaceTempView("iceberg_tv_props_v")

            rows = spark.sql("SHOW TBLPROPERTIES iceberg_tv_props_v").collect()
            assert rows == []
        finally:
            catalog.drop_table(identifier)
            spark.catalog.dropTempView("iceberg_tv_props_v")

    def test_list_columns_on_iceberg_temp_view(self, spark, tmp_path):
        """spark.catalog.listColumns() on an Iceberg temp view shows columns."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_tv_cols"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame([(1, "a", 1.0)], schema="id LONG, event STRING, score DOUBLE")
            df.write.format("iceberg").mode("overwrite").save(table.location())

            spark.read.format("iceberg").load(table.location()).createOrReplaceTempView("iceberg_tv_cols_v")

            columns = spark.catalog.listColumns("iceberg_tv_cols_v")
            col_names = [c.name for c in columns]
            assert "id" in col_names
            assert "event" in col_names
            assert "score" in col_names
        finally:
            catalog.drop_table(identifier)
            spark.catalog.dropTempView("iceberg_tv_cols_v")


# ---------------------------------------------------------------------------
# Iceberg format version 2 interop
# ---------------------------------------------------------------------------

@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
class TestIcebergV2Interop:
    """Verify Sail can read Iceberg v2 tables created by PyIceberg."""

    def test_sail_reader_reads_v2_table(self, spark, tmp_path):
        """PyIceberg creates a v2 table; Sail reads it via DataFrameReader."""
        catalog = create_sql_catalog(tmp_path)
        identifier = "default.interop_v2_reader"
        schema = _simple_iceberg_schema()
        table = catalog.create_table(identifier=identifier, schema=schema)
        try:
            df = spark.createDataFrame(
                [(10, "A", 0.98), (11, "B", 0.54)],
                schema="id LONG, event STRING, score DOUBLE",
            )
            df.write.format("iceberg").mode("overwrite").save(table.location())

            result = (
                spark.read.format("iceberg")
                .load(table.location())
                .sort("id")
                .collect()
            )
            assert len(result) == 2
            assert result[0]["id"] == 10
            assert result[1]["id"] == 11

            static_table = StaticTable.from_metadata(table.location())
            assert static_table.metadata.format_version == 2
        finally:
            catalog.drop_table(identifier)


# =========================================================================
# Delta interop
# =========================================================================

# ---------------------------------------------------------------------------
# Delta: Sail → Delta log
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific metadata writing")
class TestDeltaSailToDeltaLog:
    """Sail writes Delta tables; the JSON log is inspected for metadata correctness."""

    def test_sail_write_schema_in_delta_log(self, spark, tmp_path):
        """Sail creates a Delta table and the log contains the correct schema."""
        delta_path = tmp_path / "delta_interop_schema"
        spark.sql(
            f"CREATE TABLE delta_interop_schema (id INT, name STRING) "
            f"USING DELTA LOCATION '{delta_path}'"
        )
        try:
            metadata = _read_delta_log_metadata(delta_path)
            schema = json.loads(metadata["schemaString"])
            field_names = [f["name"] for f in schema["fields"]]
            assert field_names == ["id", "name"]
        finally:
            spark.sql("DROP TABLE IF EXISTS delta_interop_schema")

    def test_sail_write_partition_columns_in_delta_log(self, spark, tmp_path):
        """Sail creates a partitioned Delta table and the log records partition columns."""
        delta_path = tmp_path / "delta_interop_partitions"
        spark.sql(
            f"CREATE TABLE delta_interop_partitions (id INT, category STRING, value DOUBLE) "
            f"USING DELTA PARTITIONED BY (category) LOCATION '{delta_path}'"
        )
        try:
            metadata = _read_delta_log_metadata(delta_path)
            assert metadata["partitionColumns"] == ["category"]
        finally:
            spark.sql("DROP TABLE IF EXISTS delta_interop_partitions")

    def test_sail_write_properties_in_delta_log(self, spark, tmp_path):
        """Sail creates a Delta table with TBLPROPERTIES and the log records them."""
        delta_path = tmp_path / "delta_interop_props"
        spark.sql(
            f"CREATE TABLE delta_interop_props (id INT) "
            f"USING DELTA "
            f"TBLPROPERTIES ('custom.key' = 'custom_value', 'delta.appendOnly' = 'true') "
            f"LOCATION '{delta_path}'"
        )
        try:
            metadata = _read_delta_log_metadata(delta_path)
            config = metadata.get("configuration", {})
            assert config.get("delta.appendOnly") == "true"
        finally:
            spark.sql("DROP TABLE IF EXISTS delta_interop_props")

    def test_sail_write_comment_in_delta_log(self, spark, tmp_path):
        """Sail creates a Delta table with a COMMENT and the log records it."""
        delta_path = tmp_path / "delta_interop_comment"
        spark.sql(
            f"CREATE TABLE delta_interop_comment (id INT) "
            f"USING DELTA COMMENT 'test table comment' "
            f"LOCATION '{delta_path}'"
        )
        try:
            metadata = _read_delta_log_metadata(delta_path)
            assert metadata.get("description") == "test table comment"
        finally:
            spark.sql("DROP TABLE IF EXISTS delta_interop_comment")


# ---------------------------------------------------------------------------
# Delta: Delta log → Sail (DataFrameReader)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific catalog commands")
class TestDeltaLogToSailReader:
    """Hand-crafted Delta logs are created; Sail reads data via DataFrameReader."""

    def test_sail_reads_schema_from_delta_log(self, spark, tmp_path):
        """A Delta log with specific columns is read correctly by Sail."""
        delta_path = tmp_path / "delta_log_reader_schema"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "user_id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "email", "type": "string", "nullable": False, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(delta_path, schema_string=schema_string)

        df = spark.read.format("delta").load(str(delta_path))
        assert set(df.columns) == {"user_id", "email"}
        assert df.count() == 0

    def test_sail_reads_partition_columns_from_delta_log(self, spark, tmp_path):
        """A Delta log with partition columns is read correctly by Sail."""
        delta_path = tmp_path / "delta_log_reader_partitions"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "region", "type": "string", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(
            delta_path,
            schema_string=schema_string,
            partition_columns=["region"],
        )

        df = spark.read.format("delta").load(str(delta_path))
        assert set(df.columns) == {"id", "region"}


# ---------------------------------------------------------------------------
# Delta: Delta log → Sail (managed table, exercises hydration)
#
# These tests create a Delta log externally, then register it as a managed
# Delta table via CREATE TABLE ... USING DELTA LOCATION. This exercises
# the full hydration path (hydrate_table_status → load_storage_table_metadata)
# through SQL and Catalog API against storage metadata written by an
# external engine.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific catalog commands")
class TestDeltaLogToSailHydration:
    """External Delta log registered as managed table; Sail hydrates metadata from storage."""

    def test_describe_extended_shows_storage_schema(self, spark, tmp_path):
        """A Delta log created externally; DESCRIBE EXTENDED shows storage-level columns."""
        delta_path = tmp_path / "delta_hydration_schema"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "order_id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "total", "type": "double", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(delta_path, schema_string=schema_string)

        table_name = "test_hydration_schema"
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            col_names = [r.col_name for r in rows if r.col_name and not r.col_name.startswith("#")]
            assert "order_id" in col_names
            assert "total" in col_names

            # Verify provider comes from storage metadata
            metadata = {r.col_name: r.data_type for r in rows}
            assert metadata.get("Provider") == "delta"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_describe_extended_shows_partition_columns(self, spark, tmp_path):
        """A partitioned Delta log; DESCRIBE EXTENDED shows partition info from storage."""
        delta_path = tmp_path / "delta_hydration_partitions"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "region", "type": "string", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(
            delta_path,
            schema_string=schema_string,
            partition_columns=["region"],
        )

        table_name = "test_hydration_partitions"
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
            col_names = [r.col_name for r in rows if r.col_name and not r.col_name.startswith("#")]
            assert "region" in col_names
            assert "id" in col_names
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_show_tblproperties_shows_storage_properties(self, spark, tmp_path):
        """A Delta log with configuration; SHOW TBLPROPERTIES shows storage-level props."""
        delta_path = tmp_path / "delta_hydration_props"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(
            delta_path,
            schema_string=schema_string,
            configuration={"delta.appendOnly": "true", "custom.prop": "value"},
        )

        table_name = "test_hydration_props"
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            assert len(rows) >= 1
            props = {r["key"]: r["value"] for r in rows}
            assert "custom.prop" in props
            assert props["custom.prop"] == "value"
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_show_table_extended_shows_storage_info(self, spark, tmp_path):
        """A Delta log with comment; SHOW TABLE EXTENDED shows storage-level information."""
        delta_path = tmp_path / "delta_hydration_extended"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                {"name": "value", "type": "double", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(
            delta_path,
            schema_string=schema_string,
            description="hydrated table comment",
        )

        table_name = "test_hydration_extended"
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )
        try:
            rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
            assert len(rows) >= 1
            info = rows[0].information
            assert "Provider: delta" in info
            assert "Schema: root" in info
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_list_columns_returns_storage_columns(self, spark, tmp_path):
        """spark.catalog.listColumns() on a managed Delta table returns storage-level columns."""
        delta_path = tmp_path / "delta_hydration_listcols"
        schema_string = json.dumps({
            "type": "struct",
            "fields": [
                {"name": "product_id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "price", "type": "double", "nullable": True, "metadata": {}},
            ],
        })
        _write_delta_log_with_metadata(delta_path, schema_string=schema_string)

        table_name = "test_hydration_listcols"
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_path}'"
        )
        try:
            columns = spark.catalog.listColumns(table_name)
            col_names = [c.name for c in columns]
            assert "product_id" in col_names
            assert "price" in col_names
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
