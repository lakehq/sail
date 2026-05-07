# ruff: noqa: PLR2004, PT018, S608, TC002
"""Sail -> Spark roundtrip tests through a shared HMS metastore.

All tests use the MinIO-backed S3 warehouse to avoid Docker bind-mount
permission issues on CI.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

pytestmark = pytest.mark.catalog_integration

_S3_WAREHOUSE_PREFIX = "s3://hms-warehouse"


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


def _describe_extended_properties(spark: SparkSession, table_fqn: str) -> dict[str, str]:
    rows = spark.sql(f"DESCRIBE EXTENDED {table_fqn}").collect()
    return {row.col_name: row.data_type for row in rows if row.col_name}


def _describe_column_comments(spark: SparkSession, table_fqn: str) -> dict[str, str | None]:
    rows = spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()
    return {row.col_name: row.comment for row in rows if row.col_name and not row.col_name.startswith("#")}


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


# ---------------------------------------------------------------------------
# S3 / MinIO-backed tests
# ---------------------------------------------------------------------------


def test_sail_creates_spark_reads_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail creates an external Parquet table; reference Spark reads it back."""
    table = "roundtrip_parquet"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT COMMENT 'identifier',
          name STRING
        )
        USING PARQUET
        TBLPROPERTIES ('interop.owner' = 'sail')
        """
    )
    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = hms_s3_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    ref_rows = reference_spark_s3.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()

    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"
    assert ref_rows[0].id == 1 and ref_rows[0].name == "alice"
    assert ref_rows[1].id == 2 and ref_rows[1].name == "bob"
    ref_properties = _describe_extended_properties(reference_spark_s3, table_fqn)
    assert ref_properties.get("Type") == "EXTERNAL"
    assert ref_properties.get("Provider", "").lower() == "parquet"
    assert "interop.owner=sail" in ref_properties.get("Table Properties", "")
    assert "spark.sql." not in ref_properties.get("Table Properties", "")
    assert _describe_column_comments(reference_spark_s3, table_fqn)["id"] == "identifier"


def test_sail_creates_spark_reads_schema_matrix_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail writes tricky supported Parquet types; Spark restores schema and values."""
    table = "roundtrip_schema_matrix"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
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
    hms_s3_spark.sql(
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
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    _assert_schema_matrix_shape(reference_spark_s3, table_fqn)
    ref_rows = reference_spark_s3.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    _assert_schema_matrix_rows(ref_rows)


def test_sail_creates_spark_reads_timestamp_types_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail writes timestamp LTZ/NTZ columns; Spark restores schema and values."""
    table = "roundtrip_timestamp_types"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          ts_ltz TIMESTAMP,
          ts_ntz TIMESTAMP_NTZ
        )
        USING PARQUET
        """
    )
    hms_s3_spark.sql(
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
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    schema = reference_spark_s3.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}
    assert fields["ts_ltz"].dataType.simpleString() == "timestamp"
    assert fields["ts_ntz"].dataType.simpleString() == "timestamp_ntz"
    rows = reference_spark_s3.sql(
        f"SELECT id, CAST(ts_ltz AS STRING) AS ts_ltz, CAST(ts_ntz AS STRING) AS ts_ntz FROM {table_fqn}"
    ).collect()
    assert [(r.id, r.ts_ltz, r.ts_ntz) for r in rows] == [(1, "2024-01-02 03:04:05", "2024-01-02 03:04:05")]


def test_sail_creates_spark_reads_parquet_with_explicit_location(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail creates an external Parquet table with S3 LOCATION; Spark reads it back."""
    table = "roundtrip_location_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location = f"s3://hms-warehouse/{hms_s3_database}/roundtrip_location_parquet"

    hms_s3_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'")
    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = hms_s3_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    ref_rows = reference_spark_s3.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"
    assert ref_rows[0].id == 1 and ref_rows[0].name == "alice"
    assert ref_rows[1].id == 2 and ref_rows[1].name == "bob"


def test_sail_alters_datasource_table_location_spark_reads_new_path(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail ALTER TABLE SET LOCATION updates datasource path metadata for Spark."""
    table = "roundtrip_altered_location_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    old_location = f"s3://hms-warehouse/{hms_s3_database}/alter_location_old"
    new_location = f"s3://hms-warehouse/{hms_s3_database}/alter_location_new"

    hms_s3_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{old_location}'")
    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old')")
    hms_s3_spark.sql(f"ALTER TABLE {table_fqn} SET LOCATION '{new_location}'")
    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'new')")

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    ref_rows = reference_spark_s3.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(2, "new")]


def test_sail_unsets_table_properties_spark_observes_metadata_change(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail ALTER TABLE UNSET TBLPROPERTIES updates HMS metadata for Spark."""
    table = "roundtrip_unset_properties"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT, name STRING)
        USING PARQUET
        TBLPROPERTIES ('interop.keep' = 'yes', 'interop.drop' = 'remove')
        """
    )
    hms_s3_spark.sql(f"ALTER TABLE {table_fqn} UNSET TBLPROPERTIES ('interop.drop')")

    ref_properties = _describe_extended_properties(reference_spark_s3, table_fqn)
    table_properties = ref_properties.get("Table Properties", "")
    assert "interop.keep=yes" in table_properties
    assert "interop.drop" not in table_properties
    assert "spark.sql." not in table_properties


def test_sail_creates_external_table_via_catalog_api(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail creates an external table via spark.catalog API; Spark reads it back as EXTERNAL."""
    table = "roundtrip_catalog_api_external"
    table_fqn = f"{hms_s3_database}.{table}"
    location = f"s3://hms-warehouse/{hms_s3_database}/{table}"

    hms_s3_spark.catalog.createTable(
        table_fqn,
        path=location,
        source="parquet",
        schema=StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        ),
    )

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )


def test_sail_dataframe_writer_creates_spark_readable_external_table(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail DataFrame writer creates HMS metadata that Spark can read."""
    table = "roundtrip_dataframe_writer"
    table_fqn = f"{hms_s3_database}.{table}"
    location = f"s3://hms-warehouse/{hms_s3_database}/{table}"

    hms_s3_spark.createDataFrame([(1, "alice"), (2, "bob")], schema="id INT, name STRING").write.saveAsTable(
        table_fqn,
        path=location,
    )

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    rows = reference_spark_s3.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "alice"), (2, "bob")]
