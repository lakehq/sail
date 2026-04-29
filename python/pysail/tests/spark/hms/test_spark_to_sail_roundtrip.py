"""Spark → Sail roundtrip test: reference Spark creates a managed Parquet
table through the shared HMS metastore, then Sail reads it back."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def _describe_extended_properties(spark: SparkSession, table_fqn: str) -> dict[str, str]:
    rows = spark.sql(f"DESCRIBE EXTENDED {table_fqn}").collect()
    return {row.col_name: row.data_type for row in rows if row.col_name}


def _assert_sail_describes_spark_table(
    hms_spark: SparkSession,
    table_fqn: str,
    *,
    table_type: str,
) -> None:
    properties = _describe_extended_properties(hms_spark, table_fqn)

    assert properties.get("Type") == table_type
    assert properties.get("Provider", "").lower() == "parquet"
    assert properties.get("Location")


def test_spark_creates_sail_reads_parquet(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark creates a managed Parquet table; Sail reads it back.

    Steps:
    1. Reference Spark (JVM, Hive support) creates a managed table and
       inserts two rows.
    2. Sail (via Spark Connect to the Sail server + HMS catalog) reads
       the same table and verifies row count, schema, and content.
    """
    table_fqn = f"{hms_database}.roundtrip_parquet"

    # ── Phase 1: Reference Spark writes ──────────────────────────────────
    reference_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET"
    )
    reference_spark.sql(
        f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')"
    )

    # Quick sanity check on reference side
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    # ── Phase 2: Sail reads back ─────────────────────────────────────────
    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="MANAGED")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()

    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    # Verify content
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_creates_sail_reads_parquet_with_explicit_location(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Reference Spark creates an external Parquet table with LOCATION; Sail reads it back."""
    table_fqn = f"{hms_database}.roundtrip_location_parquet"
    location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/roundtrip_location_parquet"

    reference_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'"
    )
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="EXTERNAL")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_creates_sail_reads_parquet_with_relative_location(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark resolves relative LOCATION against the database path; Sail reads it back."""
    table_fqn = f"{hms_database}.roundtrip_relative_location_parquet"

    reference_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET "
        "LOCATION 'relative/roundtrip_location_parquet'"
    )
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="EXTERNAL")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"
