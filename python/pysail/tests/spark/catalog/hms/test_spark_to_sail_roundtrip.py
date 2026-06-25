# ruff: noqa: PLR2004, PT018, S608, TC002
"""Spark -> Sail roundtrip tests through a shared HMS metastore.

All tests use the MinIO-backed S3 warehouse to avoid Docker bind-mount
permission issues on CI.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.tests.spark.catalog.hms.conftest import (
    _assert_schema_matrix_rows,
    _assert_schema_matrix_shape,
    _describe_column_comments,
    _describe_extended_properties,
)


def _assert_sail_describes_spark_table(
    spark: SparkSession,
    table_fqn: str,
    *,
    table_type: str,
) -> None:
    properties = _describe_extended_properties(spark, table_fqn)

    assert properties.get("Type") == table_type
    assert properties.get("Provider", "").lower() == "parquet"
    assert properties.get("Location")


def test_spark_creates_sail_reads_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark creates a Parquet table in an S3-backed database; Sail reads it back."""
    table_fqn = f"{hms_s3_database}.roundtrip_parquet"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT COMMENT 'identifier',
          name STRING
        )
        USING PARQUET
        TBLPROPERTIES ('interop.owner' = 'spark')
        """
    )
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = jvm_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()

    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"
    sail_properties = _describe_extended_properties(spark, table_fqn)
    assert "interop.owner=spark" in sail_properties.get("Table Properties", "")
    assert "spark.sql." not in sail_properties.get("Table Properties", "")
    assert _describe_column_comments(spark, table_fqn)["id"] == "identifier"


@pytest.mark.xfail(reason="not yet working in Hive 4", strict=True)
def test_spark_creates_sail_reads_schema_matrix_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark writes tricky supported Parquet types; Sail restores schema and values."""
    table_fqn = f"{hms_s3_database}.roundtrip_schema_matrix"

    jvm_spark.sql(
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
    jvm_spark.sql(
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

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    _assert_schema_matrix_shape(spark, table_fqn)
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    _assert_schema_matrix_rows(sail_rows)


def test_spark_creates_sail_reads_timestamp_types_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark writes timestamp LTZ/NTZ columns; Sail restores schema and values."""
    table_fqn = f"{hms_s3_database}.roundtrip_timestamp_types"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          ts_ltz TIMESTAMP,
          ts_ntz TIMESTAMP_NTZ
        )
        USING PARQUET
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (
            1,
            TIMESTAMP '2024-01-02 03:04:05',
            TIMESTAMP_NTZ '2024-01-02 03:04:05'
          )
        """
    )

    schema = spark.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}
    assert fields["ts_ltz"].dataType.simpleString() == "timestamp"
    assert fields["ts_ntz"].dataType.simpleString() == "timestamp_ntz"
    rows = spark.sql(
        f"SELECT id, CAST(ts_ltz AS STRING) AS ts_ltz, CAST(ts_ntz AS STRING) AS ts_ntz FROM {table_fqn}"
    ).collect()
    assert [(r.id, r.ts_ltz, r.ts_ntz) for r in rows] == [(1, "2024-01-02 03:04:05", "2024-01-02 03:04:05")]


def test_spark_creates_sail_reads_parquet_with_explicit_location(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark creates an external Parquet table with S3 LOCATION; Sail reads it back."""
    table_fqn = f"{hms_s3_database}.roundtrip_location_parquet"
    location = f"s3://hms-warehouse/{hms_s3_database}/roundtrip_location_parquet"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{location}'")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = jvm_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_creates_sail_reads_parquet_with_relative_location(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark resolves relative LOCATION against the database path; Sail reads it back."""
    table_fqn = f"{hms_s3_database}.roundtrip_relative_location_parquet"

    jvm_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION 'relative/roundtrip_location_parquet'"
    )
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = jvm_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"
    assert sail_rows[0].id == 1 and sail_rows[0].name == "alice"
    assert sail_rows[1].id == 2 and sail_rows[1].name == "bob"


def test_spark_alters_datasource_table_sail_still_reads_path_metadata(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark alter-table preserves datasource path metadata; Sail still restores it."""
    table_fqn = f"{hms_s3_database}.roundtrip_altered_parquet"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")
    jvm_spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ('interop_note' = 'spark_altered')")

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    properties = _describe_extended_properties(spark, table_fqn)
    assert "interop_note=spark_altered" in properties.get("Table Properties", "")
    assert "spark.sql." not in properties.get("Table Properties", "")
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


def test_spark_alters_datasource_table_location_sail_reads_new_path(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark ALTER TABLE SET LOCATION updates datasource path metadata for Sail."""
    table_fqn = f"{hms_s3_database}.roundtrip_altered_location_parquet"
    old_location = f"s3://hms-warehouse/{hms_s3_database}/alter_location_old"
    new_location = f"s3://hms-warehouse/{hms_s3_database}/alter_location_new"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET LOCATION '{old_location}'")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old')")
    jvm_spark.sql(f"ALTER TABLE {table_fqn} SET LOCATION '{new_location}'")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'new')")

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    properties = _describe_extended_properties(spark, table_fqn)
    assert properties.get("Location") == new_location
    sail_rows = spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(2, "new")]


def test_spark_catalog_api_creates_table_sail_reads_external_table(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark catalog API createTable metadata is readable from Sail."""
    table = "roundtrip_catalog_api_external"
    table_fqn = f"{hms_s3_database}.{table}"
    location = f"s3://hms-warehouse/{hms_s3_database}/{table}"

    jvm_spark.catalog.createTable(
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
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


def test_spark_dataframe_writer_creates_table_sail_reads_external_table(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark DataFrameWriter saveAsTable metadata is readable from Sail."""
    table = "roundtrip_dataframe_writer"
    table_fqn = f"{hms_s3_database}.{table}"
    location = f"s3://hms-warehouse/{hms_s3_database}/{table}"

    jvm_spark.createDataFrame([(1, "alice"), (2, "bob")], schema="id INT, name STRING").write.saveAsTable(
        table_fqn,
        path=location,
    )

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


@pytest.mark.xfail(reason="not yet working in Hive 4", strict=True)
def test_spark_creates_sail_reads_mixed_complex_partitioned_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark writes mixed complex partitioned schema; Sail reads nested values and partitions."""
    table_fqn = f"{hms_s3_database}.roundtrip_mixed_complex_partitioned"
    location = f"s3://hms-warehouse/{hms_s3_database}/roundtrip_mixed_complex_partitioned"

    jvm_spark.sql(
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
        LOCATION '{location}'
        """
    )
    jvm_spark.sql(
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

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    props = _describe_extended_properties(spark, table_fqn)
    assert props.get("Type") == "EXTERNAL"
    assert props.get("Provider", "").lower() == "parquet"
    assert props.get("Location") == location
    retail = spark.sql(
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


def test_spark_creates_sail_reads_date_and_binary_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark writes DATE and BINARY values; Sail restores exact values."""
    table_fqn = f"{hms_s3_database}.roundtrip_date_binary"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, day DATE, payload BINARY) USING PARQUET")
    jvm_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, DATE '2024-01-02', unhex('00FF10')),
          (2, DATE '2024-01-03', unhex('ABCD'))
        """
    )

    _assert_sail_describes_spark_table(spark, table_fqn, table_type="EXTERNAL")
    rows = spark.sql(
        f"SELECT id, CAST(day AS STRING) AS day, hex(payload) AS payload_hex FROM {table_fqn} ORDER BY id"
    ).collect()
    assert [(r.id, r.day, r.payload_hex) for r in rows] == [
        (1, "2024-01-02", "00FF10"),
        (2, "2024-01-03", "ABCD"),
    ]
