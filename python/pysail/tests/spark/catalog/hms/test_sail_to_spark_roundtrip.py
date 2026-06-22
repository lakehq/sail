# ruff: noqa: PLR2004, PT018, S608, TC002
"""Sail -> Spark roundtrip tests through a shared HMS metastore.

All tests use the MinIO-backed S3 warehouse to avoid Docker bind-mount
permission issues on CI.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.tests.spark.catalog.hms.conftest import (
    _assert_schema_matrix_rows,
    _assert_schema_matrix_shape,
    _describe_column_comments,
    _describe_extended_properties,
    _reference_catalog_table,
    _scala_option_to_string,
)

_S3_WAREHOUSE_PREFIX = "s3://hms-warehouse"


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
    ref_properties = _describe_extended_properties(reference_spark_s3, table_fqn)
    assert ref_properties.get("Type") == "EXTERNAL"
    assert ref_properties.get("Provider", "").lower() == "parquet"


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
    ref_properties = _describe_extended_properties(reference_spark_s3, table_fqn)
    assert ref_properties.get("Type") == "EXTERNAL"
    assert ref_properties.get("Provider", "").lower() == "parquet"
    rows = reference_spark_s3.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "alice"), (2, "bob")]


def test_sail_creates_spark_reads_mixed_complex_partitioned_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail writes mixed complex partitioned schema; Spark reads nested values and partitions."""
    table = "roundtrip_mixed_complex_partitioned"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          payload STRUCT<
            items: ARRAY<STRUCT<label: STRING, attrs: MAP<STRING, ARRAY<INT>>>>,
            active: BOOLEAN
          >,
          metrics MAP<STRING, STRUCT<count: INT, weights: ARRAY<DOUBLE>>>,
          category STRING,
          event_date DATE
        )
        USING PARQUET
        PARTITIONED BY (category, event_date)
        """
    )
    hms_s3_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (
            1,
            named_struct(
              'items',
              array(
                named_struct(
                  'label', 'l1',
                  'attrs', map('nums', array(1, 2), 'empty', array())
                )
              ),
              'active',
              true
            ),
            map(
              'm1', named_struct('count', 3, 'weights', array(1.5D, 2.5D))
            ),
            'retail',
            DATE '2024-01-02'
          ),
          (
            2,
            named_struct(
              'items',
              array(
                named_struct(
                  'label', 'l2',
                  'attrs', map('nums', array(7, 8), 'empty', array())
                )
              ),
              'active',
              false
            ),
            map(
              'm1', named_struct('count', 5, 'weights', array(3.5D, 4.5D))
            ),
            'wholesale',
            DATE '2024-01-03'
          )
        """
    )

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    props = _describe_extended_properties(reference_spark_s3, table_fqn)
    assert props.get("Type") == "EXTERNAL"
    assert props.get("Provider", "").lower() == "parquet"
    assert props.get("Location")
    retail = reference_spark_s3.sql(
        f"""
        SELECT
          id,
          payload.active AS active,
          payload.items[0].label AS label,
          payload.items[0].attrs['nums'][0] AS first_num,
          metrics['m1'].count AS metric_count,
          metrics['m1'].weights[1] AS second_weight,
          category,
          CAST(event_date AS STRING) AS event_date
        FROM {table_fqn}
        WHERE category = 'retail' AND event_date = DATE '2024-01-02'
        ORDER BY id
        """
    ).collect()
    assert [
        (r.id, r.active, r.label, r.first_num, r.metric_count, r.second_weight, r.category, r.event_date)
        for r in retail
    ] == [(1, True, "l1", 1, 3, 2.5, "retail", "2024-01-02")]


def test_sail_creates_spark_reads_date_and_binary_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail writes DATE and BINARY values; Spark restores exact values."""
    table = "roundtrip_date_binary"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(f"CREATE TABLE {table_fqn} (id INT, day DATE, payload BINARY) USING PARQUET")
    hms_s3_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, DATE '2024-01-02', unhex('00FF10')),
          (2, DATE '2024-01-03', unhex('ABCD'))
        """
    )

    _assert_reference_spark_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        table_type="EXTERNAL",
    )
    rows = reference_spark_s3.sql(
        f"SELECT id, CAST(day AS STRING) AS day, hex(payload) AS payload_hex FROM {table_fqn} ORDER BY id"
    ).collect()
    assert [(r.id, r.day, r.payload_hex) for r in rows] == [
        (1, "2024-01-02", "00FF10"),
        (2, "2024-01-03", "ABCD"),
    ]
