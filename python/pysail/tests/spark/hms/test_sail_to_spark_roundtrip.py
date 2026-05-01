# ruff: noqa: PLR2004, PT018, S608, TC002, TC003
"""Sail -> Spark roundtrip test through a shared HMS metastore."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.hms_interop


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


def _normalize_file_uri(uri: str | None) -> str | None:
    if uri is not None and uri.startswith("file:/") and not uri.startswith("file://"):
        return f"file:///{uri.removeprefix('file:/')}"
    return uri


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


def test_sail_creates_spark_reads_schema_matrix_parquet(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail writes tricky supported Parquet types; Spark restores schema and values."""
    table = "roundtrip_schema_matrix"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(
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
    hms_spark.sql(
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

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="MANAGED",
    )
    _assert_schema_matrix_shape(reference_spark, table_fqn)
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    _assert_schema_matrix_rows(ref_rows)


def test_sail_creates_spark_reads_timestamp_types_parquet(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail writes timestamp LTZ/NTZ columns; Spark restores schema and values."""
    table = "roundtrip_timestamp_types"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          ts_ltz TIMESTAMP,
          ts_ntz TIMESTAMP_NTZ
        )
        USING PARQUET
        """
    )
    hms_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (
            1,
            TIMESTAMP '2024-01-02 03:04:05',
            TIMESTAMP_NTZ '2024-01-02 03:04:05'
          )
        """
    )

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="MANAGED",
    )
    schema = reference_spark.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}
    assert fields["ts_ltz"].dataType.simpleString() == "timestamp"
    assert fields["ts_ntz"].dataType.simpleString() == "timestamp_ntz"
    rows = reference_spark.sql(
        f"SELECT id, CAST(ts_ltz AS STRING) AS ts_ltz, CAST(ts_ntz AS STRING) AS ts_ntz FROM {table_fqn}"
    ).collect()
    assert [(r.id, r.ts_ltz, r.ts_ntz) for r in rows] == [(1, "2024-01-02 03:04:05", "2024-01-02 03:04:05")]


def test_sail_creates_spark_reads_partitioned_parquet(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail creates partitioned Parquet; reference Spark restores partition metadata."""
    table = "roundtrip_partitioned_parquet"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING, region STRING) USING PARQUET PARTITIONED BY (region)"
    )
    hms_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'alice', 'north'),
          (2, 'bob', 'with space'),
          (3, 'carol', 'a/b')
        """
    )

    sail_rows = hms_spark.sql(f"SELECT id, name, region FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name, r.region) for r in sail_rows] == [
        (1, "alice", "north"),
        (2, "bob", "with space"),
        (3, "carol", "a/b"),
    ]
    sail_filtered_rows = hms_spark.sql(f"SELECT id FROM {table_fqn} WHERE region = 'a/b'").collect()
    assert [r.id for r in sail_filtered_rows] == [3]

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="MANAGED",
    )
    _assert_schema_partition_column(reference_spark, table_fqn, "region")
    ref_partitions = {row.partition for row in reference_spark.sql(f"SHOW PARTITIONS {table_fqn}").collect()}
    assert ref_partitions == {
        "region=a%2Fb",
        "region=north",
        "region=with space",
    }
    ref_rows = reference_spark.sql(f"SELECT id, name, region FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name, r.region) for r in ref_rows] == [
        (1, "alice", "north"),
        (2, "bob", "with space"),
        (3, "carol", "a/b"),
    ]

    filtered_rows = reference_spark.sql(f"SELECT id FROM {table_fqn} WHERE region = 'a/b'").collect()
    assert [r.id for r in filtered_rows] == [3]


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

    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'")
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


def test_sail_creates_spark_reads_partitioned_parquet_with_file_uri_authority(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Sail recovers partitions for local file URIs that include an authority."""
    table = "roundtrip_partitioned_file_uri_authority"
    table_fqn = f"{hms_database}.{table}"
    location_path = hms_warehouse_dir / hms_database / table
    location = f"file://localhost{location_path}"

    hms_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, region STRING) USING PARQUET PARTITIONED BY (region) LOCATION '{location}'"
    )
    hms_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'north'),
          (2, 'a/b')
        """
    )

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="EXTERNAL",
    )
    ref_partitions = {row.partition for row in reference_spark.sql(f"SHOW PARTITIONS {table_fqn}").collect()}
    assert ref_partitions == {
        "region=a%2Fb",
        "region=north",
    }
    ref_rows = reference_spark.sql(f"SELECT id, region FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.region) for r in ref_rows] == [(1, "north"), (2, "a/b")]


def test_sail_creates_spark_reads_parquet_with_relative_location(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail resolves relative LOCATION against the database path like Spark."""
    table = "roundtrip_relative_location_parquet"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION 'relative/roundtrip_location_parquet'"
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


def test_sail_alters_datasource_table_spark_still_reads_path_metadata(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
) -> None:
    """Sail alter-table preserves Spark datasource path metadata for reference Spark."""
    table = "roundtrip_altered_parquet"
    table_fqn = f"{hms_database}.{table}"

    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")
    hms_spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ('interop_note' = 'sail_altered')")

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="MANAGED",
    )
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(1, "alice"), (2, "bob")]


def test_sail_alters_datasource_table_location_spark_reads_new_path(
    hms_spark: SparkSession,
    reference_spark: SparkSession,
    hms_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Sail ALTER TABLE SET LOCATION updates datasource path metadata for Spark."""
    table = "roundtrip_altered_location_parquet"
    table_fqn = f"{hms_database}.{table}"
    old_location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/alter_location_old"
    new_location = f"{hms_warehouse_dir.as_uri().rstrip('/')}/{hms_database}/alter_location_new"

    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{old_location}'")
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old')")
    hms_spark.sql(f"ALTER TABLE {table_fqn} SET LOCATION '{new_location}'")
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'new')")

    _assert_reference_spark_table(
        reference_spark,
        hms_database,
        table,
        table_type="EXTERNAL",
    )
    spark_table = _reference_catalog_table(reference_spark, hms_database, table)
    assert _normalize_file_uri(_scala_option_to_string(spark_table.storage().locationUri())) == new_location
    ref_rows = reference_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(2, "new")]
