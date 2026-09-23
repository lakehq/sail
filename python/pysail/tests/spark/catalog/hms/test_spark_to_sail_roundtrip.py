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


def test_spark_creates_sail_reads_delta_datasource_table(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Spark registers a Delta table in HMS as a datasource table; Sail reads it.

    Delta Lake tables registered in the Hive Metastore are persisted as Spark
    datasource tables carrying ``spark.sql.sources.provider=delta``. Sail must
    recognise the provider and resolve the table location. This covers the Delta
    branch of ``sail_catalog_hms::convert::table_location`` end-to-end.
    """
    table_fqn = f"{hms_s3_database}.roundtrip_delta_datasource"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} USING DELTA AS SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'")

    properties = _describe_extended_properties(spark, table_fqn)
    assert properties.get("Provider", "").lower() == "delta"
    assert properties.get("Location"), "Sail did not resolve a location for the Delta table"

    sail_rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in sail_rows] == [(1, "alice"), (2, "bob")]


def _register_delta_schema_in_hms(jvm_spark: SparkSession, database: str, table: str) -> None:
    """Store the Delta table schema in the HMS entry.

    Depending on the Delta Lake version and configuration, Spark either leaves
    the HMS schema of a Delta table empty or stores the columns there. Storing
    the columns explicitly makes Sail resolve the table through the catalog
    schema, which lacks Delta column-mapping metadata and uses different map
    entry field names than the Delta log schema.
    """
    session = jvm_spark._jsparkSession  # noqa: SLF001
    schema = session.table(f"{database}.{table}").schema()
    session.sessionState().catalog().externalCatalog().alterTableDataSchema(database, table, schema)


def test_spark_creates_sail_aggregates_column_mapped_delta_table(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """Sail plans aggregates and nested field access over a column-mapped Delta table whose schema is stored in HMS.

    The table has no map columns, so this covers the column mapping metadata on
    top-level and nested fields separately from the map entry naming.
    """
    table = "roundtrip_column_mapped_delta_scalar"
    table_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        AS
        SELECT 'event-a' AS file_name, 1 AS id, named_struct('a', 1, 'b', 'x') AS s,
          array(named_struct('x', 1, 'y', 'p')) AS arr_s
        UNION ALL
        SELECT 'event-a' AS file_name, 2 AS id, named_struct('a', 2, 'b', 'y') AS s,
          array(named_struct('x', 2, 'y', 'q')) AS arr_s
        UNION ALL
        SELECT 'event-b' AS file_name, 3 AS id, named_struct('a', 3, 'b', 'z') AS s,
          array(named_struct('x', 3, 'y', 'r')) AS arr_s
        """
    )
    _register_delta_schema_in_hms(jvm_spark, hms_s3_database, table)

    df = spark.table(table_fqn)
    assert sorted(row.file_name for row in df.select("file_name").collect()) == ["event-a", "event-a", "event-b"]
    assert sorted(row.file_name for row in df.select("file_name").dropDuplicates(["file_name"]).collect()) == [
        "event-a",
        "event-b",
    ]
    counts = spark.sql(
        f"SELECT file_name, count(*) AS n, sum(id) AS total, sum(s.a) AS sa"
        f" FROM {table_fqn} GROUP BY file_name ORDER BY file_name"
    )
    assert [(row.file_name, row.n, row.total, row.sa) for row in counts.collect()] == [
        ("event-a", 2, 3, 3),
        ("event-b", 1, 3, 3),
    ]
    nested = spark.sql(f"SELECT id, s.b, arr_s[0].x AS x, arr_s[0].y AS y FROM {table_fqn} ORDER BY id")
    assert [(row.id, row.b, row.x, row.y) for row in nested.collect()] == [
        (1, "x", 1, "p"),
        (2, "y", 2, "q"),
        (3, "z", 3, "r"),
    ]


@pytest.mark.parametrize("column_mapping_mode", ["none", "name"])
def test_spark_creates_sail_reads_delta_table_with_map(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    column_mapping_mode: str,
) -> None:
    """Sail reads and aggregates a Delta table with a map column whose schema is stored in HMS."""
    table = f"roundtrip_delta_map_{column_mapping_mode}"
    table_fqn = f"{hms_s3_database}.{table}"

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = '{column_mapping_mode}')
        AS
        SELECT 'event-a' AS file_name, map('dag_id', 'dag-a') AS properties
        UNION ALL
        SELECT 'event-a' AS file_name, map('dag_id', 'dag-a') AS properties
        UNION ALL
        SELECT 'event-b' AS file_name, map('dag_id', 'dag-b') AS properties
        """
    )
    _register_delta_schema_in_hms(jvm_spark, hms_s3_database, table)

    df = spark.table(table_fqn)
    assert df.schema["properties"].dataType.simpleString() == "map<string,string>"
    assert sorted(row.file_name for row in df.select("file_name").dropDuplicates(["file_name"]).collect()) == [
        "event-a",
        "event-b",
    ]
    assert sorted(row.properties["dag_id"] for row in df.select("properties").collect()) == ["dag-a", "dag-a", "dag-b"]
    rows = df.dropDuplicates(["file_name"]).orderBy("file_name").collect()
    assert [(row.file_name, row.properties) for row in rows] == [
        ("event-a", {"dag_id": "dag-a"}),
        ("event-b", {"dag_id": "dag-b"}),
    ]
    filtered = spark.sql(f"SELECT file_name FROM {table_fqn} WHERE properties['dag_id'] = 'dag-b'").collect()
    assert [row.file_name for row in filtered] == ["event-b"]


_DELTA_SCHEMA_MATRIX_QUERIES = [
    "SELECT id, big, dbl, dec, flag, name, dt, CAST(ts AS STRING) AS ts, bin FROM {t} ORDER BY id",
    "SELECT id, s, arr, arr_s, m, m_s, m_arr, nested FROM {t} ORDER BY id",
    "SELECT id, s.a, s.b FROM {t} ORDER BY id",
    "SELECT id, arr[0] AS first, size(arr) AS n, arr_s[0].x AS x, arr_s[0].y AS y FROM {t} ORDER BY id",
    "SELECT id, m['k1'] AS k1, m_s['k1'].p AS p, m_arr['k1'] AS k1_arr, size(m) AS n FROM {t} ORDER BY id",
    "SELECT id, map_keys(m) AS keys, map_values(m_s) AS vals FROM {t} ORDER BY id",
    "SELECT id, nested.inner_map['a'] AS a, nested.inner_arr AS inner_arr FROM {t} ORDER BY id",
    "SELECT id, explode(arr) AS v FROM {t} ORDER BY id, v",
    "SELECT id, k, v FROM {t} LATERAL VIEW explode(m) AS k, v ORDER BY id, k",
    "SELECT DISTINCT name FROM {t} ORDER BY name",
    "SELECT DISTINCT name, flag FROM {t} ORDER BY name, flag",
    "SELECT name, count(*) AS n, sum(dec) AS total, max(dbl) AS mx, min(dt) AS first_dt, sum(s.a) AS sa"
    " FROM {t} GROUP BY name ORDER BY name",
    "SELECT name, collect_list(id) AS ids FROM (SELECT * FROM {t} ORDER BY id) GROUP BY name ORDER BY name",
    "SELECT id FROM {t} WHERE s.a > 1 AND m['k1'] IS NOT NULL ORDER BY id",
    "SELECT id FROM {t} WHERE name = 'b' OR array_contains(arr, 5) ORDER BY id",
    "SELECT a.id, b.name FROM {t} a JOIN {t} b ON a.id = b.id ORDER BY a.id",
    "SELECT count(*) AS n FROM (SELECT id, m FROM {t} UNION ALL SELECT id, m FROM {t})",
    "SELECT id, name, m FROM {t} ORDER BY dbl DESC LIMIT 2",
    "SELECT s.* FROM {t} ORDER BY a",
    "SELECT DISTINCT s FROM {t} ORDER BY s.a",
    "SELECT s, count(*) AS n FROM {t} GROUP BY s ORDER BY s.a",
    "SELECT min(s) AS mn, max(s) AS mx, count(DISTINCT s) AS n FROM {t}",
    "SELECT a.id FROM {t} a JOIN {t} b ON a.s = b.s ORDER BY a.id",
    "SELECT id, s FROM {t} UNION SELECT id, s FROM {t} ORDER BY id",
    "SELECT id, s, arr_s FROM {t} INTERSECT SELECT id, s, arr_s FROM {t} ORDER BY id",
    "SELECT id, s FROM {t} EXCEPT SELECT id, s FROM {t} WHERE id = 1 ORDER BY id",
    "SELECT id, row_number() OVER (PARTITION BY name ORDER BY s.a) AS rn, max(s) OVER (PARTITION BY name) AS mx"
    " FROM {t} ORDER BY id",
    "SELECT id, transform(arr_s, e -> e.x) AS xs, filter(arr_s, e -> e.x > 1) AS f, arr_s.y AS ys FROM {t} ORDER BY id",
    "SELECT id, inline(arr_s) FROM {t} ORDER BY id",
    "SELECT id, to_json(s) AS j, CAST(s AS STRING) AS str FROM {t} ORDER BY id",
    "SELECT id, named_struct('x', s.a, 'y', nested.inner_arr) AS n, struct(s.a, s.b) AS t2 FROM {t} ORDER BY id",
]


@pytest.mark.parametrize("partitioned", [False, True], ids=["unpartitioned", "partitioned"])
@pytest.mark.parametrize("column_mapping_mode", ["none", "name"])
def test_spark_creates_sail_queries_delta_schema_matrix(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
    column_mapping_mode: str,
    partitioned: bool,  # noqa: FBT001
) -> None:
    """Sail and Spark agree on queries over scalar and nested Delta columns whose schema is stored in HMS."""
    table = f"roundtrip_delta_matrix_{column_mapping_mode}_{'part' if partitioned else 'flat'}"
    table_fqn = f"{hms_s3_database}.{table}"
    partition_clause = "PARTITIONED BY (name)" if partitioned else ""

    jvm_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          big BIGINT,
          dbl DOUBLE,
          dec DECIMAL(10, 2),
          flag BOOLEAN,
          name STRING,
          dt DATE,
          ts TIMESTAMP,
          bin BINARY,
          s STRUCT<a: INT, b: STRING>,
          arr ARRAY<INT>,
          arr_s ARRAY<STRUCT<x: INT, y: STRING>>,
          m MAP<STRING, INT>,
          m_s MAP<STRING, STRUCT<p: INT>>,
          m_arr MAP<STRING, ARRAY<INT>>,
          nested STRUCT<inner_map: MAP<STRING, STRING>, inner_arr: ARRAY<STRING>>
        )
        USING DELTA
        {partition_clause}
        TBLPROPERTIES ('delta.columnMapping.mode' = '{column_mapping_mode}')
        """
    )
    jvm_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 10, 1.5, 1.25, true, 'a', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:01', X'01',
           named_struct('a', 1, 'b', 'x'), array(1, 2), array(named_struct('x', 1, 'y', 'p')),
           map('k1', 1, 'k2', 2), map('k1', named_struct('p', 1)), map('k1', array(1, 2)),
           named_struct('inner_map', map('a', 'A'), 'inner_arr', array('i'))),
          (2, 20, 2.5, 2.50, false, 'a', DATE '2024-01-02', TIMESTAMP '2024-01-02 00:00:02', X'02',
           named_struct('a', 2, 'b', 'y'), array(3), array(named_struct('x', 2, 'y', 'q')),
           map('k1', 3), map('k1', named_struct('p', 2)), map('k1', array(3)),
           named_struct('inner_map', map('a', 'B'), 'inner_arr', array('j', 'k'))),
          (3, 30, 3.5, 3.75, true, 'b', DATE '2024-01-03', TIMESTAMP '2024-01-03 00:00:03', X'03',
           named_struct('a', 3, 'b', 'z'), array(5, 6), array(named_struct('x', 3, 'y', 'r')),
           map('k2', 4), map('k2', named_struct('p', 3)), map('k2', array(4)),
           named_struct('inner_map', map('b', 'C'), 'inner_arr', array('l'))),
          (4, NULL, NULL, NULL, NULL, 'c', NULL, NULL, NULL,
           NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    _register_delta_schema_in_hms(jvm_spark, hms_s3_database, table)

    previous_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        assert spark.table(table_fqn).schema == jvm_spark.table(table_fqn).schema
        for query in _DELTA_SCHEMA_MATRIX_QUERIES:
            sql = query.format(t=table_fqn)
            assert spark.sql(sql).collect() == jvm_spark.sql(sql).collect(), sql
        deduplicated = spark.table(table_fqn).dropDuplicates(["name"]).select("name").orderBy("name").collect()
        assert [row.name for row in deduplicated] == ["a", "b", "c"]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_timezone)


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
