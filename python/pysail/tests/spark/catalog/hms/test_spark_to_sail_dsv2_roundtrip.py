# ruff: noqa: PLR2004, S608
"""Spark -> Sail roundtrip tests for Spark datasource v2 (DSV2) formats and Iceberg.

The JVM reference Spark creates a table in HMS using a datasource format; Sail
reads it back through its HMS catalog. This exercises the SerDe ``path`` handling
in :func:`sail_catalog_hms::convert::table_location` for the full Spark
datasource provider set that Sail supports in HMS: parquet, csv, json, avro, and
delta. Iceberg and Hive-native textfile tables have separate contract tests
because their HMS metadata contracts are intentionally different.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.catalog.hms.conftest import (
    _describe_column_comments,
    _describe_extended_properties,
    _reference_catalog_table,
    _scala_option_to_string,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_DSV2_FORMATS = ["parquet", "csv", "json", "avro", "delta"]
# Sail reads every DSV2 format above, but cannot *write* Avro: its writer raises
# "Writer not implemented for this format". The reverse (Sail-write) parametrization
# keeps Avro in the matrix as xfail with that real error, so a future Avro writer
# landing turns the xfail green without touching the read-side coverage.
_SAIL_WRITE_FORMATS = [
    "parquet",
    "csv",
    "json",
    pytest.param(
        "avro",
        marks=pytest.mark.xfail(
            reason="Sail has no Avro writer ('Writer not implemented for this format')",
            strict=True,
        ),
    ),
    "delta",
]
# Formats whose authoritative table metadata (schema, column comments, user
# TBLPROPERTIES) lives in HMS table parameters, so assertions reading that metadata
# back through Sail are meaningful. Delta is deliberately excluded: the DeltaCatalog
# keeps schema/column comments/user properties in the Delta transaction log, not in
# HMS, and rejects ``ALTER TABLE ... SET LOCATION`` on a Delta table outright
# (``DELTA_MISSING_DELTA_TABLE``). Those behaviors are Spark/Delta semantics, not
# Sail gaps, so parametrizing Delta through the HMS-metadata assertions would assert
# a contract Delta does not provide.
_HMS_METADATA_FORMATS = [fmt for fmt in _DSV2_FORMATS if fmt != "delta"]


def _assert_sail_describes_datasource_table(
    spark: SparkSession,
    table_fqn: str,
    *,
    provider: str,
) -> None:
    properties = _describe_extended_properties(spark, table_fqn)
    assert properties.get("Type") == "EXTERNAL", properties
    assert properties.get("Provider", "").lower() == provider, properties
    assert properties.get("Location"), f"no location resolved for {table_fqn}"


@pytest.mark.parametrize("fmt", _DSV2_FORMATS)
def test_spark_creates_sail_reads_dsv2(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Reference Spark creates a DSV2 datasource table; Sail reads it back.

    ``CREATE TABLE ... USING <fmt>`` registers the table in HMS as a Spark datasource
    table carrying ``spark.sql.sources.provider=<fmt>`` and the location mirrored into
    the SerDe ``path`` parameter. Sail must recognise the provider, resolve the
    location via the SerDe path, and return the rows.
    """
    table_fqn = f"{hms_s3_database}.roundtrip_{fmt}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING {fmt.upper()}
        """
    )
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = jvm_spark.sql(f"SELECT * FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    _assert_sail_describes_datasource_table(spark, table_fqn, provider=fmt)
    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


def _assert_jvm_spark_sees_datasource_provider(
    jvm_spark: SparkSession,
    database: str,
    table: str,
    provider: str,
) -> None:
    """Assert that JVM Spark sees the expected provider in HMS metadata.

    This is an independent cross-engine check: JVM Spark reads Sail's HMS metadata
    directly via ``externalCatalog().getTable()``, not via DESCRIBE EXTENDED.
    The comparison is case-insensitive because Spark normalizes the provider
    string differently across versions (e.g. ``USING ORC`` is recorded as
    ``ORC``), and the contract we care about is the *provider identity*, not its
    casing.
    """
    spark_table = _reference_catalog_table(jvm_spark, database, table)
    actual = _scala_option_to_string(spark_table.provider())
    assert actual is not None, f"provider is None for {database}.{table}"
    assert actual.lower() == provider.lower(), f"expected provider={provider!r}, got {actual!r}"


def _reference_table_properties(jvm_spark: SparkSession, database: str, table: str) -> dict[str, str]:
    scala_properties = _reference_catalog_table(jvm_spark, database, table).properties()
    iterator = scala_properties.iterator()
    properties: dict[str, str] = {}
    while iterator.hasNext():
        entry = iterator.next()
        properties[str(entry.productElement(0))] = str(entry.productElement(1))
    return properties


def _set_hms_table_parameter(
    reference_spark: SparkSession,
    database: str,
    table: str,
    key: str,
    value: str,
) -> None:
    """Update one table parameter directly so storage metadata stays unchanged."""
    jvm = reference_spark._jvm  # noqa: SLF001
    hadoop_conf = reference_spark._jsc.hadoopConfiguration()  # noqa: SLF001
    hive_conf = jvm.org.apache.hadoop.hive.conf.HiveConf()
    hive_conf.set("hive.metastore.uris", hadoop_conf.get("hive.metastore.uris"))
    client = jvm.org.apache.hadoop.hive.metastore.HiveMetaStoreClient(hive_conf)
    try:
        hms_table = client.getTable(database, table)
        hms_table.getParameters().put(key, value)
        client.alter_table(database, table, hms_table)
        actual = client.getTable(database, table).getParameters().get(key)
        assert actual == value, f"expected raw HMS parameter {key}={value!r}, got {actual!r}"
    finally:
        client.close()


@pytest.mark.parametrize("fmt", _SAIL_WRITE_FORMATS)
def test_sail_creates_spark_reads_dsv2(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Sail creates a DSV2 datasource table; reference Spark reads it back.

    The reverse direction: Sail's ``build_generic_table`` writes the SerDe ``path``
    parameter (the change in this PR), and reference Spark must resolve the table
    location and read the rows.
    """
    table_fqn = f"{hms_s3_database}.reverse_{fmt}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING {fmt.upper()}
        """
    )
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'alice'), (2, 'bob')")

    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert len(sail_rows) == 2, f"Sail expected 2 rows, got {len(sail_rows)}"

    # Verify Sail's own read of the metadata it wrote: this is the write-side
    # assertion that directly exercises build_generic_table's provider + SerDe path
    # handling (the change in this PR), not just row parity.
    _assert_sail_describes_datasource_table(spark, table_fqn, provider=fmt)

    # Independent cross-engine validation: JVM Spark reads HMS metadata directly.
    _assert_jvm_spark_sees_datasource_provider(jvm_spark, hms_s3_database, f"reverse_{fmt}", provider=fmt)

    ref_rows = jvm_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(1, "alice"), (2, "bob")]


def test_spark_creates_sail_reads_iceberg(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Reference Spark creates an Iceberg table in HMS; Sail reads it back.

    Spark's ``iceberg`` catalog (type=hive) registers the table in HMS with
    ``table_type=ICEBERG`` and a ``metadata-location`` property. Sail detects the
    Iceberg marker, resolves the format, loads the metadata file from S3 via
    :class:`IcebergTableProvider`, and returns the rows.
    """
    table = "roundtrip_iceberg"
    # Spark writes through its named ``iceberg`` catalog; Sail reads the same HMS
    # table via the ``sail`` catalog (plain db.table, no catalog prefix).
    spark_iceberg_fqn = f"iceberg.{hms_s3_database}.{table}"
    sail_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {spark_iceberg_fqn} (
          id INT,
          name STRING
        )
        USING ICEBERG
        """
    )
    jvm_spark.sql(f"INSERT INTO {spark_iceberg_fqn} VALUES (1, 'alice'), (2, 'bob')")

    ref_rows = jvm_spark.sql(f"SELECT id, name FROM {spark_iceberg_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 2, f"Reference Spark expected 2 rows, got {len(ref_rows)}"

    properties = _describe_extended_properties(spark, sail_fqn)
    assert properties.get("Provider", "").lower() == "iceberg", properties
    assert properties.get("Location"), f"Sail did not resolve a location for {sail_fqn}"

    sail_rows = spark.sql(f"SELECT id, name FROM {sail_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]


def test_sail_creates_spark_reads_iceberg(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail creates an Iceberg table in HMS; reference Spark reads it back.

    The reverse direction of ``test_spark_creates_sail_reads_iceberg``: Sail's
    CatalogCoordinated create commits ``table_type=ICEBERG`` and a ``metadata-location``
    to HMS. Reference Spark reads the table through its named ``iceberg`` catalog
    (type=hive), which detects the Iceberg marker and loads the metadata file.
    """
    table = "reverse_iceberg"
    sail_fqn = f"{hms_s3_database}.{table}"
    spark_iceberg_fqn = f"iceberg.{hms_s3_database}.{table}"

    spark.sql(
        f"""
        CREATE TABLE {sail_fqn} (
          id INT,
          name STRING
        )
        USING ICEBERG
        """
    )
    spark.sql(f"INSERT INTO {sail_fqn} VALUES (1, 'alice'), (2, 'bob')")

    # Sail reads its own metadata back: Provider=iceberg, location resolved.
    properties = _describe_extended_properties(spark, sail_fqn)
    assert properties.get("Provider", "").lower() == "iceberg", properties
    assert properties.get("Location"), f"Sail did not resolve a location for {sail_fqn}"

    sail_rows = spark.sql(f"SELECT id, name FROM {sail_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob")]

    # Reference Spark reads the same HMS table through its iceberg catalog.
    ref_rows = jvm_spark.sql(f"SELECT id, name FROM {spark_iceberg_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(1, "alice"), (2, "bob")]


def test_iceberg_append_visibility_both_ways(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark and Sail alternately append to one HMS Iceberg table and see snapshots."""
    table = "append_iceberg"
    spark_iceberg_fqn = f"iceberg.{hms_s3_database}.{table}"
    sail_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {spark_iceberg_fqn} (
          id INT,
          name STRING
        )
        USING ICEBERG
        """
    )
    jvm_spark.sql(f"INSERT INTO {spark_iceberg_fqn} VALUES (1, 'spark')")
    assert [(r.id, r.name) for r in spark.sql(f"SELECT id, name FROM {sail_fqn}").collect()] == [(1, "spark")]

    spark.sql(f"INSERT INTO {sail_fqn} VALUES (2, 'sail')")
    # JVM Spark's Iceberg ``SparkCatalog`` caches table metadata in-process for the
    # session, so an external commit that advances the HMS ``metadata-location``
    # pointer is not picked up automatically. An explicit ``REFRESH TABLE`` forces a
    # re-read of the catalog entry. This is Spark catalog semantics, not a Sail gap:
    # pyiceberg (``StaticTable.from_metadata``), which has no catalog cache, sees
    # Sail's appended row without any refresh. Each engine that reads after another
    # engine's external commit must refresh its own catalog view first.
    jvm_spark.catalog.refreshTable(spark_iceberg_fqn)
    ref_rows = jvm_spark.sql(f"SELECT id, name FROM {spark_iceberg_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in ref_rows] == [(1, "spark"), (2, "sail")]

    jvm_spark.sql(f"INSERT INTO {spark_iceberg_fqn} VALUES (3, 'spark-again')")
    # Sail re-reads HMS table metadata on each access (it does not hold an in-process
    # catalog cache like the JVM Iceberg SparkCatalog), so no refresh is needed here.
    sail_rows = spark.sql(f"SELECT id, name FROM {sail_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "spark"), (2, "sail"), (3, "spark-again")]


def test_spark_creates_sail_reads_iceberg_type_properties_and_comments(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Iceberg HMS tables preserve basic schema fidelity, properties, and comments."""
    table = "iceberg_types_props"
    spark_iceberg_fqn = f"iceberg.{hms_s3_database}.{table}"
    sail_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {spark_iceberg_fqn} (
          id INT COMMENT 'identifier',
          amount DECIMAL(10, 2),
          ts TIMESTAMP
        )
        USING ICEBERG
        TBLPROPERTIES ('interop.owner' = 'spark')
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {spark_iceberg_fqn} VALUES
          (1, CAST(12.34 AS DECIMAL(10, 2)), TIMESTAMP '2024-01-02 03:04:05'),
          (2, CAST(0.10 AS DECIMAL(10, 2)), TIMESTAMP '2024-01-02 03:04:06')
        """
    )

    hms_properties = _reference_table_properties(jvm_spark, hms_s3_database, table)
    assert hms_properties.get("table_type", "").lower() == "iceberg", hms_properties
    assert hms_properties.get("metadata-location") or hms_properties.get("metadata_location"), hms_properties

    sail_properties = _describe_extended_properties(spark, sail_fqn)
    assert sail_properties.get("Provider", "").lower() == "iceberg", sail_properties
    assert "interop.owner=spark" in sail_properties.get("Table Properties", ""), sail_properties
    assert _describe_column_comments(spark, sail_fqn)["id"] == "identifier"

    fields = {field.name: field for field in spark.table(sail_fqn).schema.fields}
    assert fields["amount"].dataType.simpleString() == "decimal(10,2)", fields
    assert fields["ts"].dataType.simpleString() == "timestamp", fields
    rows = spark.sql(
        f"""
        SELECT id, CAST(amount AS STRING) AS amount, CAST(ts AS STRING) AS ts
        FROM {sail_fqn}
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.amount, r.ts) for r in rows] == [
        (1, "12.34", "2024-01-02 03:04:05"),
        (2, "0.10", "2024-01-02 03:04:06"),
    ]


def test_spark_creates_sail_reads_partitioned_iceberg(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark writes an identity-partitioned HMS Iceberg table; Sail reads and prunes."""
    table = "iceberg_part"
    spark_iceberg_fqn = f"iceberg.{hms_s3_database}.{table}"
    sail_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {spark_iceberg_fqn} (
          id INT,
          name STRING,
          event_date DATE
        )
        USING ICEBERG
        PARTITIONED BY (event_date)
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {spark_iceberg_fqn} VALUES
          (1, 'alice', DATE '2024-01-02'),
          (2, 'bob', DATE '2024-01-03'),
          (3, 'cara', DATE '2024-01-02')
        """
    )

    rows = spark.sql(
        f"""
        SELECT id, name, CAST(event_date AS STRING) AS event_date
        FROM {sail_fqn}
        WHERE event_date = DATE '2024-01-02'
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.name, r.event_date) for r in rows] == [
        (1, "alice", "2024-01-02"),
        (3, "cara", "2024-01-02"),
    ]


@pytest.mark.xfail(
    reason=(
        "Sail detects the Hive textfile format at the HMS metadata layer but has no "
        "textfile file reader, so reading raises 'No table format found for: textfile'."
    ),
    strict=True,
)
def test_spark_creates_sail_reads_hive_textfile_table(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Hive SerDe textfile has no datasource provider and uses SD location."""
    table = "native_textfile"
    table_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (value STRING) STORED AS TEXTFILE")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES ('alpha'), ('beta')")

    # A Hive-native textfile table carries no ``spark.sql.sources.provider`` key in
    # HMS parameters; JVM ``CatalogTable.provider()`` still reports ``hive``, but that
    # is the Hive catalog handle, not a Spark datasource provider. The real contract
    # is the *absence* of the datasource provider key, which is what forces Sail to
    # resolve the table via SerDe/storage-descriptor detection instead of the path.
    hms_props = _reference_table_properties(jvm_spark, hms_s3_database, table)
    assert "spark.sql.sources.provider" not in {k.lower() for k in hms_props}, hms_props
    assert _scala_option_to_string(_reference_catalog_table(jvm_spark, hms_s3_database, table).storage().locationUri())

    properties = _describe_extended_properties(spark, table_fqn)
    assert properties.get("Provider", "").lower() == "textfile", properties
    assert properties.get("Location"), properties
    rows = spark.sql(f"SELECT value FROM {table_fqn} ORDER BY value").collect()
    assert [row.value for row in rows] == ["alpha", "beta"]


def test_unsupported_datasource_provider_does_not_poison_database_listing(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """An unsupported Spark datasource provider must not break database listing.

    The JVM reference Spark creates ORC and Parquet datasource tables. The test
    then changes a second Parquet table's provider marker to a custom provider
    without changing its Parquet SerDe metadata. Sail must enumerate all three
    and preserve both unsupported provider identities when describing them.

    Listing/describe must stay usable for any provider: ``list_tables`` maps
    every HMS table through ``table_to_status``, so an unsupported provider on
    one table must not fail the whole listing. A datasource provider marker is
    authoritative: Sail must not relabel ``com.acme.CustomSource`` as Parquet
    merely because the provider uses Parquet storage metadata.

    Reading the ORC table is intentionally not asserted: Sail may still fail
    at the scan/read layer, which is acceptable as long as listing and describe
    succeed.
    """
    orc_table = "orc_t"
    parquet_table = "parquet_t"
    custom_table = "custom_provider_t"
    orc_fqn = f"{hms_s3_database}.{orc_table}"
    parquet_fqn = f"{hms_s3_database}.{parquet_table}"
    custom_fqn = f"{hms_s3_database}.{custom_table}"
    custom_provider = "com.acme.CustomSource"

    jvm_spark.sql(f"CREATE TABLE {orc_fqn} (id INT, name STRING) USING ORC")
    jvm_spark.sql(f"CREATE TABLE {parquet_fqn} (id INT, name STRING) USING PARQUET")
    jvm_spark.sql(f"CREATE TABLE {custom_fqn} (id INT, name STRING) USING PARQUET")
    _set_hms_table_parameter(
        jvm_spark,
        hms_s3_database,
        custom_table,
        "spark.sql.sources.provider",
        custom_provider,
    )

    # JVM cross-engine sanity: the ORC table really is a datasource table with
    # provider=orc in HMS metadata (i.e. this test really exercises the
    # unsupported-provider path, not a Hive-native SerDe-only table).
    _assert_jvm_spark_sees_datasource_provider(jvm_spark, hms_s3_database, orc_table, provider="orc")

    # Listing contract: both tables are visible through Sail's HMS catalog.
    # An unsupported provider must not fail ``list_tables``.
    shown = {row.tableName for row in spark.sql(f"SHOW TABLES IN {hms_s3_database}").collect()}
    assert orc_table in shown, f"orc table missing from SHOW TABLES: {shown}"
    assert parquet_table in shown, f"parquet table missing from SHOW TABLES: {shown}"
    assert custom_table in shown, f"custom-provider table missing from SHOW TABLES: {shown}"

    # The ORC table's metadata is visible and preserves its provider (Sail has
    # no ORC reader, but metadata-only access must still work).
    orc_properties = _describe_extended_properties(spark, orc_fqn)
    assert orc_properties.get("Provider", "").lower() == "orc", orc_properties
    assert orc_properties.get("Location"), orc_properties

    custom_properties = _describe_extended_properties(spark, custom_fqn)
    assert custom_properties.get("Provider", "").lower() == custom_provider.lower(), custom_properties
    assert custom_properties.get("Location"), custom_properties


# ---------------------------------------------------------------------------
# Metadata and type fidelity for DSV2 formats (spark -> sail read path).
#
# These catch format-specific interop breaks that the basic (INT, STRING)
# round-trip cannot surface: decimal scale/precision, timestamp serialisation,
# SerDe-path updates on ALTER TABLE, foreign property visibility, and column
# comments. All are spark->sail because that is the direction where Sail's reader
# must interpret Spark's written files/metadata, and it is the direction that
# works for every DSV2 format including avro.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", _DSV2_FORMATS)
def test_spark_creates_sail_reads_dsv2_type_fidelity(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Reference Spark writes DECIMAL and TIMESTAMP; Sail restores exact types and values."""
    table_fqn = f"{hms_s3_database}.types_{fmt}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          amount DECIMAL(10, 2),
          ts TIMESTAMP
        )
        USING {fmt.upper()}
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, CAST(12.34 AS DECIMAL(10, 2)), TIMESTAMP '2024-01-02 03:04:05'),
          (2, CAST(0.10 AS DECIMAL(10, 2)), TIMESTAMP '2024-01-02 03:04:06')
        """
    )

    # Schema is preserved: decimal precision/scale and timestamp type survive.
    schema = spark.table(table_fqn).schema
    fields = {field.name: field for field in schema.fields}
    assert fields["amount"].dataType.simpleString() == "decimal(10,2)", fields
    assert fields["ts"].dataType.simpleString() == "timestamp", fields

    # Values are restored exactly. Cast to string to avoid decimal/timezone
    # comparison noise (mirrors the existing parquet timestamp test).
    rows = spark.sql(
        f"""
        SELECT
          id,
          CAST(amount AS STRING) AS amount,
          CAST(ts AS STRING) AS ts
        FROM {table_fqn}
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.amount, r.ts) for r in rows] == [
        (1, "12.34", "2024-01-02 03:04:05"),
        (2, "0.10", "2024-01-02 03:04:06"),
    ]


@pytest.mark.parametrize("fmt", _HMS_METADATA_FORMATS)
def test_spark_alters_dsv2_table_location_sail_reads_new_path(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Spark ALTER TABLE SET LOCATION updates the datasource SerDe path; Sail reads it.

    Distinct from create: the SerDe ``path`` *update* path. After SET LOCATION the
    old location is abandoned; Sail must resolve and read only the new location.
    Mirrors the existing parquet ``test_spark_alters_datasource_table_location_*``.
    """
    table_fqn = f"{hms_s3_database}.alter_loc_{fmt}"
    old_location = f"s3://hms-warehouse/{hms_s3_database}/alter_loc_{fmt}_old"
    new_location = f"s3://hms-warehouse/{hms_s3_database}/alter_loc_{fmt}_new"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING {fmt.upper()} LOCATION '{old_location}'")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old')")
    jvm_spark.sql(f"ALTER TABLE {table_fqn} SET LOCATION '{new_location}'")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'new')")

    _assert_sail_describes_datasource_table(spark, table_fqn, provider=fmt)
    properties = _describe_extended_properties(spark, table_fqn)
    assert properties.get("Location") == new_location, properties
    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(2, "new")]


@pytest.mark.parametrize("fmt", _HMS_METADATA_FORMATS)
def test_spark_dsv2_table_properties_round_trip_spark_sql_hidden(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Foreign TBLPROPERTIES survive; spark.sql.* internal keys are hidden by Sail.

    Datasource tables auto-carry ``spark.sql.sources.provider`` and
    ``spark.sql.sources.schema.*`` properties. Sail must expose user properties
    (e.g. ``interop.owner``) while hiding these ``spark.sql.`` internals.
    """
    table_fqn = f"{hms_s3_database}.props_{fmt}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT, name STRING)
        USING {fmt.upper()}
        TBLPROPERTIES ('interop.owner' = 'spark')
        """
    )

    sail_properties = _describe_extended_properties(spark, table_fqn)
    assert "interop.owner=spark" in sail_properties.get("Table Properties", ""), sail_properties
    assert "spark.sql." not in sail_properties.get("Table Properties", ""), sail_properties


@pytest.mark.parametrize("fmt", _HMS_METADATA_FORMATS)
def test_spark_dsv2_column_comments_preserved(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Column comments set by Spark are preserved when Sail reads the table."""
    table_fqn = f"{hms_s3_database}.comments_{fmt}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT COMMENT 'identifier',
          name STRING
        )
        USING {fmt.upper()}
        """
    )

    assert _describe_column_comments(spark, table_fqn)["id"] == "identifier"


# ---------------------------------------------------------------------------
# Partitioned tables.
#
# Sail's HMS catalog carries partition column *names* in TableStatus; actual
# partition discovery is delegated to the object-store-aware reader, which walks
# Hive-style partition directories (e.g. ``.../event_date=2024-01-02/...``).
# These tests validate that path for plain (identity) partitions with simple
# types, in isolation from the complex-type concerns that the existing xfailed
# ``schema_matrix`` / ``mixed_complex_partitioned`` parquet tests confound.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", _DSV2_FORMATS)
def test_spark_creates_sail_reads_partitioned(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Spark writes an identity-partitioned table; Sail discovers partitions and reads.

    ``PARTITIONED BY (event_date DATE)`` with simple types, isolated from complex
    types. Covers: Sail preserves the partition column in schema, the reader
    discovers Hive-style partition directories, partition pruning works, and values
    (including the partition column) are restored correctly.
    """
    table_fqn = f"{hms_s3_database}.part_{fmt}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING,
          event_date DATE
        )
        USING {fmt.upper()}
        PARTITIONED BY (event_date)
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'alice', DATE '2024-01-02'),
          (2, 'bob', DATE '2024-01-03'),
          (3, 'cara', DATE '2024-01-02')
        """
    )

    # Reference sanity check.
    ref_rows = jvm_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert len(ref_rows) == 3, f"Reference Spark expected 3 rows, got {len(ref_rows)}"

    # Sail reads all rows across both partitions.
    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.name) for r in sail_rows] == [(1, "alice"), (2, "bob"), (3, "cara")]

    # Partition pruning: Sail pushes the partition filter down (only one partition read).
    pruned = spark.sql(
        f"""
        SELECT id, name, CAST(event_date AS STRING) AS event_date
        FROM {table_fqn}
        WHERE event_date = DATE '2024-01-02'
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.name, r.event_date) for r in pruned] == [
        (1, "alice", "2024-01-02"),
        (3, "cara", "2024-01-02"),
    ]


@pytest.mark.parametrize("fmt", _SAIL_WRITE_FORMATS)
def test_sail_creates_spark_reads_partitioned(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
    fmt: str,
) -> None:
    """Sail writes an identity-partitioned table; Spark discovers partitions and reads.

    Reverse direction: Sail's ``build_generic_table`` records the partition columns in
    HMS; Sail's writer lays out Hive-style partition directories; reference Spark
    reads them back through HMS.
    """
    table_fqn = f"{hms_s3_database}.reverse_part_{fmt}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING,
          event_date DATE
        )
        USING {fmt.upper()}
        PARTITIONED BY (event_date)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'alice', DATE '2024-01-02'),
          (2, 'bob', DATE '2024-01-03')
        """
    )

    # Assert the partition column (event_date) round-trips in the all-rows path,
    # not just the data columns. Catches a writer that drops the partition column
    # from the schema or writes wrong partition values.
    sail_rows = spark.sql(
        f"""
        SELECT id, name, CAST(event_date AS STRING) AS event_date
        FROM {table_fqn}
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.name, r.event_date) for r in sail_rows] == [
        (1, "alice", "2024-01-02"),
        (2, "bob", "2024-01-03"),
    ]

    # Independent cross-engine validation: JVM Spark reads HMS metadata directly.
    _assert_jvm_spark_sees_datasource_provider(jvm_spark, hms_s3_database, f"reverse_part_{fmt}", provider=fmt)

    # Reference Spark reads Sail's partitioned layout through HMS, including the
    # partition column values.
    ref_rows = jvm_spark.sql(
        f"""
        SELECT id, name, CAST(event_date AS STRING) AS event_date
        FROM {table_fqn}
        ORDER BY id
        """
    ).collect()
    assert [(r.id, r.name, r.event_date) for r in ref_rows] == [
        (1, "alice", "2024-01-02"),
        (2, "bob", "2024-01-03"),
    ]
