# ruff: noqa: PLR2004, PT018, S608, TC002, TC003
"""Spark → Sail roundtrip test: reference Spark creates a managed Parquet
table through the shared HMS metastore, then Sail reads it back."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.hms_interop


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


def _assert_schema_partition_column(
    spark: SparkSession,
    table_fqn: str,
    partition_column: str,
) -> None:
    schema = spark.table(table_fqn).schema
    assert schema[-1].name == partition_column


def _assert_schema_matrix_shape(spark: SparkSession, table_fqn: str) -> None:
    schema = spark.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}

    assert fields["amount"].dataType.simpleString() == "decimal(10,2)"
    assert fields["payload"].dataType.simpleString() == "struct<flag:boolean,score:int>"
    assert fields["tags"].dataType.simpleString() == "array<string>"
    assert fields["events"].dataType.simpleString() == "array<struct<kind:string,score:int>>"
    assert (
        fields["nested_combo"].dataType.simpleString()
        == "struct<items:array<struct<label:string,weight:decimal(5,2)>>,attrs:map<string,array<int>>>"
    )
    assert fields["attrs"].dataType.simpleString() == "map<string,int>"
    assert fields["nullable_note"].nullable


def _assert_schema_matrix_rows(rows) -> None:
    assert len(rows) == 2
    assert rows[0].id == 1
    assert rows[0].amount == Decimal("12.34")
    assert rows[0].payload.flag is True
    assert rows[0].payload.score == 7
    assert rows[0].tags == ["red", "blue"]
    assert [(event.kind, event.score) for event in rows[0].events] == [
        ("click", 3),
        ("view", 5),
    ]
    assert [(item.label, item.weight) for item in rows[0].nested_combo.items] == [
        ("first", Decimal("1.25")),
        ("second", Decimal("2.50")),
    ]
    assert rows[0].nested_combo.attrs == {"empty": [], "nums": [1, 2]}
    assert rows[0].attrs == {"x": 1, "y": 2}
    assert rows[0].nullable_note is None
    assert rows[1].id == 2
    assert rows[1].amount == Decimal("0.10")
    assert rows[1].payload.flag is False
    assert rows[1].tags == []
    assert rows[1].events == []
    assert rows[1].nested_combo.items == []
    assert rows[1].nested_combo.attrs == {}
    assert rows[1].attrs == {}
    assert rows[1].nullable_note == "present"


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
    reference_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

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


def test_spark_creates_sail_reads_schema_matrix_parquet(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark writes tricky supported Parquet types; Sail restores schema and values."""
    table_fqn = f"{hms_database}.roundtrip_schema_matrix"

    reference_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          amount DECIMAL(10, 2),
          payload STRUCT<flag: BOOLEAN, score: INT>,
          tags ARRAY<STRING>,
          events ARRAY<STRUCT<kind: STRING, score: INT>>,
          nested_combo STRUCT<
            items: ARRAY<STRUCT<label: STRING, weight: DECIMAL(5, 2)>>,
            attrs: MAP<STRING, ARRAY<INT>>
          >,
          attrs MAP<STRING, INT>,
          nullable_note STRING
        )
        USING PARQUET
        """
    )
    reference_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (
            1,
            CAST(12.34 AS DECIMAL(10, 2)),
            named_struct('flag', true, 'score', 7),
            array('red', 'blue'),
            array(
              named_struct('kind', 'click', 'score', 3),
              named_struct('kind', 'view', 'score', 5)
            ),
            named_struct(
              'items',
              array(
                named_struct('label', 'first', 'weight', CAST(1.25 AS DECIMAL(5, 2))),
                named_struct('label', 'second', 'weight', CAST(2.50 AS DECIMAL(5, 2)))
              ),
              'attrs',
              map('nums', array(1, 2), 'empty', array())
            ),
            map('x', 1, 'y', 2),
            CAST(NULL AS STRING)
          ),
          (
            2,
            CAST(0.10 AS DECIMAL(10, 2)),
            named_struct('flag', false, 'score', 0),
            CAST(array() AS ARRAY<STRING>),
            CAST(array() AS ARRAY<STRUCT<kind: STRING, score: INT>>),
            named_struct(
              'items',
              CAST(array() AS ARRAY<STRUCT<label: STRING, weight: DECIMAL(5, 2)>>),
              'attrs',
              CAST(map() AS MAP<STRING, ARRAY<INT>>)
            ),
            CAST(map() AS MAP<STRING, INT>),
            'present'
          )
        """
    )

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="MANAGED")
    _assert_schema_matrix_shape(hms_spark, table_fqn)
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    _assert_schema_matrix_rows(sail_rows)


def test_spark_creates_sail_reads_timestamp_types_parquet(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark writes timestamp LTZ/NTZ columns; Sail restores schema and values."""
    table_fqn = f"{hms_database}.roundtrip_timestamp_types"

    reference_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          ts_ltz TIMESTAMP,
          ts_ntz TIMESTAMP_NTZ
        )
        USING PARQUET
        """
    )
    reference_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (
            1,
            TIMESTAMP '2024-01-02 03:04:05',
            TIMESTAMP_NTZ '2024-01-02 03:04:05'
          )
        """
    )

    schema = hms_spark.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}
    assert fields["ts_ltz"].dataType.simpleString() == "timestamp"
    assert fields["ts_ntz"].dataType.simpleString() == "timestamp_ntz"
    rows = hms_spark.sql(
        f"SELECT id, CAST(ts_ltz AS STRING) AS ts_ltz, CAST(ts_ntz AS STRING) AS ts_ntz FROM {table_fqn}"
    ).collect()
    assert [(r.id, r.ts_ltz, r.ts_ntz) for r in rows] == [(1, "2024-01-02 03:04:05", "2024-01-02 03:04:05")]


def test_spark_creates_sail_reads_parquet_with_explicit_location(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Reference Spark creates an external Parquet table with LOCATION; Sail reads it back."""
    table_fqn = f"{hms_database}.roundtrip_location_parquet"
    location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/roundtrip_location_parquet"

    reference_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'")
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="EXTERNAL")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_creates_sail_reads_partitioned_parquet(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark creates partitioned Parquet; Sail restores partition metadata."""
    table_fqn = f"{hms_database}.roundtrip_partitioned_parquet"

    reference_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING, region STRING) USING PARQUET PARTITIONED BY (region)"
    )
    reference_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'alice', 'north'),
          (2, 'bob', 'with space'),
          (3, 'carol', 'a/b')
        """
    )

    ref_partitions = {row.partition for row in reference_spark.sql(f"SHOW PARTITIONS {table_fqn}").collect()}
    assert ref_partitions == {
        "region=a%2Fb",
        "region=north",
        "region=with space",
    }

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="MANAGED")
    _assert_schema_partition_column(hms_spark, table_fqn, "region")
    sail_rows = hms_spark.sql(f"SELECT id, name, region FROM {table_fqn} ORDER BY id").collect()

    assert [(r.id, r.name, r.region) for r in sail_rows] == [
        (1, "alice", "north"),
        (2, "bob", "with space"),
        (3, "carol", "a/b"),
    ]

    filtered_rows = hms_spark.sql(f"SELECT id FROM {table_fqn} WHERE region = 'a/b'").collect()
    assert [r.id for r in filtered_rows] == [3]


def test_spark_creates_sail_reads_parquet_with_relative_location(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Reference Spark resolves relative LOCATION against the database path; Sail reads it back."""
    table_fqn = f"{hms_database}.roundtrip_relative_location_parquet"

    reference_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION 'relative/roundtrip_location_parquet'"
    )
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="EXTERNAL")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_alters_datasource_table_sail_still_reads_path_metadata(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
) -> None:
    """Spark alter-table preserves datasource path metadata; Sail still restores it."""
    table_fqn = f"{hms_database}.roundtrip_altered_parquet"

    reference_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")
    reference_spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ('interop_note' = 'spark_altered')")

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="MANAGED")
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


def test_spark_alters_datasource_table_location_sail_reads_new_path(
    reference_spark: SparkSession,
    hms_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Spark ALTER TABLE SET LOCATION updates datasource path metadata for Sail."""
    table_fqn = f"{hms_database}.roundtrip_altered_location_parquet"
    old_location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/alter_location_old"
    new_location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/alter_location_new"

    reference_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{old_location}'")
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old')")
    reference_spark.sql(f"ALTER TABLE {table_fqn} SET LOCATION '{new_location}'")
    reference_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'new')")

    _assert_sail_describes_spark_table(hms_spark, table_fqn, table_type="EXTERNAL")
    properties = _describe_extended_properties(hms_spark, table_fqn)
    assert properties.get("Location") == new_location
    sail_rows = hms_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(2, "new")]
