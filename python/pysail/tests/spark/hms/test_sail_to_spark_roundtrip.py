"""Sail -> Spark roundtrip test through a shared HMS metastore."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def _reference_catalog_table(reference_spark: SparkSession, database: str, table: str):
    """Return Spark's restored CatalogTable from the reference external catalog."""
    return (
        reference_spark._jsparkSession.sessionState()  # noqa: SLF001
        .catalog()
        .externalCatalog()
        .getTable(database, table)
    )


def _scala_option_to_string(option) -> str | None:
    if option.isDefined():
        value = option.get()
        return value.toString() if hasattr(value, "toString") else str(value)
    return None


def _assert_reference_spark_table(
    reference_spark: SparkSession,
    database: str,
    table: str,
    *,
    table_type: str,
) -> None:
    """Assert Spark restores Sail-written HMS metadata as a data-source table."""
    spark_table = _reference_catalog_table(reference_spark, database, table)

    assert spark_table.tableType().name() == table_type
    assert _scala_option_to_string(spark_table.provider()) == "parquet"
    assert _scala_option_to_string(spark_table.storage().locationUri()) is not None


def test_sail_creates_spark_reads_parquet(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail creates a managed Parquet table; reference Spark reads it back."""
    table = "roundtrip_parquet"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="MANAGED",
    )
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()

    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"
    assert ref_rows[0].id == 1 and ref_rows[0].name == "alice"
    assert ref_rows[1].id == 2 and ref_rows[1].name == "bob"


def test_sail_creates_spark_reads_parquet_with_explicit_location(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Sail creates an external Parquet table with LOCATION; Spark reads it back."""
    table = "roundtrip_location_parquet"
    table_fqn = f"{hms_database}.{table}"
    location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/roundtrip_location_parquet"

    hms_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'"
    )
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="EXTERNAL",
    )
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"
    assert ref_rows[0].id == 1 and ref_rows[0].name == "alice"
    assert ref_rows[1].id == 2 and ref_rows[1].name == "bob"


def test_sail_creates_spark_reads_parquet_with_relative_location(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail resolves relative LOCATION against the database path like Spark."""
    table = "roundtrip_relative_location_parquet"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET "
        "LOCATION 'relative/roundtrip_location_parquet'"
    )
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="EXTERNAL",
    )
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"
    assert ref_rows[0].id == 1 and ref_rows[0].name == "alice"
    assert ref_rows[1].id == 2 and ref_rows[1].name == "bob"
