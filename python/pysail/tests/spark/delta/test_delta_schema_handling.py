"""
Test Delta Lake schema handling (mergeSchema, overwriteSchema, evolution) in Sail.
"""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _as_utc(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in UTC for comparison purposes."""
    if dt.tzinfo is None:
        # Treat naive datetimes as local timestamps and normalize to UTC.
        return datetime.fromtimestamp(dt.timestamp(), tz=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_delta_schema(delta_path):
    schema = None
    for log_file in sorted((delta_path / "_delta_log").glob("*.json")):
        with log_file.open(encoding="utf-8") as f:
            for line in f:
                action = json.loads(line)
                metadata = action.get("metaData")
                if metadata is not None:
                    schema = json.loads(metadata["schemaString"])
    assert schema is not None
    return schema


def _delta_log_actions(delta_path):
    actions = []
    for log_file in sorted((delta_path / "_delta_log").glob("*.json")):
        with log_file.open(encoding="utf-8") as f:
            actions.extend(json.loads(line) for line in f)
    return actions


def _delta_field(schema, name):
    for field in schema["fields"]:
        if field["name"] == name:
            return field
    msg = f"field {name!r} not found in Delta schema"
    raise AssertionError(msg)


def _catalog_nullability_schema(shape, nullable):
    array_type = ArrayType(StringType(), containsNull=nullable)
    payload_type = {
        "array": array_type,
        "struct": StructType([StructField("items", array_type, nullable)]),
        "map": MapType(StringType(), array_type, valueContainsNull=nullable),
    }[shape]
    return StructType([StructField("id", IntegerType(), False), StructField("payload", payload_type, True)])


def _catalog_nullability_rows(shape):
    populated, empty, nested_null, null_child = {
        "array": (["a"], [], [None, "b"], None),
        "struct": (Row(items=["a"]), Row(items=[]), Row(items=[None, "b"]), Row(items=None)),
        "map": ({"items": ["a"]}, {}, {"items": [None, "b"]}, {"items": None}),
    }[shape]
    return [(0, populated)], [(1, populated), (2, empty)], [(3, nested_null), (4, null_child), (5, None)]


def _assert_catalog_nullability(spark, table_name, delta_path, expected_type, expected_rows):
    logged_payload = _delta_field(_latest_delta_schema(delta_path), "payload")
    if isinstance(expected_type, StructType):
        # Spark field metadata has a separate encoding in Sail's log schema.
        assert logged_payload["type"]["type"] == "struct"
        [logged_items] = logged_payload["type"]["fields"]
        assert logged_items["name"] == "items"
        assert logged_items["nullable"] == expected_type["items"].nullable
        assert logged_items["type"] == expected_type["items"].dataType.jsonValue()
    else:
        assert logged_payload["type"] == expected_type.jsonValue()
    assert logged_payload["nullable"] is True
    named = spark.table(table_name)
    by_path = spark.read.format("delta").load(str(delta_path))
    assert named.schema == by_path.schema
    assert named.schema["payload"].dataType == expected_type
    assert named.schema["payload"].nullable is True
    for frame in (named, by_path):
        rows = sorted(frame.collect(), key=lambda row: row.id)
        assert [(row.id, row.payload) for row in rows] == expected_rows


def _exercise_catalog_nullability(spark, table_name, delta_path, creation, mode, shape, initial_nullable):
    initial_schema = _catalog_nullability_schema(shape, initial_nullable)
    initial_rows, strict_rows, nullable_rows = _catalog_nullability_rows(shape)
    initial = spark.createDataFrame(initial_rows, initial_schema)
    if creation == "ddl":
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, payload {initial_schema['payload'].dataType.simpleString()}) "
            f"USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'"
        ).collect()
        initial.write.format("delta").mode("append").saveAsTable(table_name)
    else:
        initial.write.format("delta").option("path", str(delta_path)).saveAsTable(table_name)
    expected_type = initial_schema["payload"].dataType
    _assert_catalog_nullability(spark, table_name, delta_path, expected_type, initial_rows)

    strict = spark.createDataFrame(strict_rows, _catalog_nullability_schema(shape, False))
    strict.write.format("delta").mode(mode).saveAsTable(table_name)
    expected_rows = (initial_rows if mode == "append" else []) + strict_rows
    _assert_catalog_nullability(spark, table_name, delta_path, expected_type, expected_rows)

    if initial_nullable:
        nullable = spark.createDataFrame(nullable_rows, _catalog_nullability_schema(shape, True))
        nullable.write.format("delta").mode("append").saveAsTable(table_name)
        _assert_catalog_nullability(spark, table_name, delta_path, expected_type, expected_rows + nullable_rows)


@pytest.mark.parametrize(
    ("creation", "mode", "shape", "initial_nullable"),
    [
        pytest.param("ddl", "overwrite", "array", True, id="ddl-array"),
        pytest.param("dataframe", "overwrite", "array", True, id="dataframe-array"),
        pytest.param("dataframe", "append", "array", True, id="append-array"),
        pytest.param("dataframe", "overwrite", "array", False, id="always-strict-array"),
        pytest.param("dataframe", "overwrite", "struct", True, id="nested-struct"),
        pytest.param("dataframe", "overwrite", "map", True, id="nested-map"),
    ],
)
def test_delta_catalog_uses_snapshot_nullability(spark, tmp_path, creation, mode, shape, initial_nullable):
    table_name = "delta_catalog_nullability"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
    try:
        _exercise_catalog_nullability(spark, table_name, tmp_path / "table", creation, mode, shape, initial_nullable)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("creation", "shape"),
    [("ddl", "array"), ("dataframe", "array"), ("dataframe", "struct"), ("dataframe", "map")],
)
def test_delta_catalog_nullability_delta_spark(spark, delta_jvm_spark, tmp_path, creation, shape):
    """Reference lifecycle, then alternate Sail and Delta-Spark writes to the same log."""
    delta_path = tmp_path / "table"
    sail_table = "delta_catalog_nullability_sail"
    jvm_table = "delta_catalog_nullability_jvm"
    spark.sql(f"DROP TABLE IF EXISTS {sail_table}").collect()
    delta_jvm_spark.sql(f"DROP TABLE IF EXISTS {jvm_table}").collect()
    try:
        _exercise_catalog_nullability(delta_jvm_spark, jvm_table, delta_path, creation, "overwrite", shape, True)
        spark.sql(
            f"CREATE TABLE {sail_table} USING DELTA LOCATION '{escape_sql_string_literal(str(delta_path))}'"
        ).collect()
        _, strict_rows, nullable_rows = _catalog_nullability_rows(shape)
        expected_type = _catalog_nullability_schema(shape, True)["payload"].dataType
        _assert_catalog_nullability(spark, sail_table, delta_path, expected_type, strict_rows + nullable_rows)

        strict = spark.createDataFrame(strict_rows, _catalog_nullability_schema(shape, False))
        strict.write.format("delta").mode("overwrite").saveAsTable(sail_table)
        for engine, table in ((spark, sail_table), (delta_jvm_spark, jvm_table)):
            _assert_catalog_nullability(engine, table, delta_path, expected_type, strict_rows)

        nullable = delta_jvm_spark.createDataFrame(nullable_rows, _catalog_nullability_schema(shape, True))
        nullable.write.format("delta").mode("append").save(str(delta_path))
        for engine, table in ((spark, sail_table), (delta_jvm_spark, jvm_table)):
            _assert_catalog_nullability(engine, table, delta_path, expected_type, strict_rows + nullable_rows)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {sail_table}").collect()
        delta_jvm_spark.sql(f"DROP TABLE IF EXISTS {jvm_table}").collect()


def test_delta_schema_read_with_custom_schema(spark, tmp_path):
    """Test with a custom schema and filter conditions."""
    delta_path = tmp_path / "delta_custom_schema"
    delta_table_path = f"{delta_path}"

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("score", IntegerType(), True),
        ]
    )

    data = [(1, "Alice", 90), (2, "Bob", None), (3, "Charlie", 85)]
    spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(str(delta_path))

    result_df = spark.read.format("delta").load(delta_table_path).filter("name IS NOT NULL AND score > 80")
    result = result_df.collect()
    assert len(result) == 2  # noqa: PLR2004
    assert {row.name for row in result} == {"Alice", "Charlie"}

    loaded_schema = result_df.schema
    assert loaded_schema == schema, f"Schema mismatch: expected {schema}, got {loaded_schema}"


def test_delta_schema_evolution_with_merge_schema(spark, tmp_path):
    """Test mergeSchema=true allows adding new columns during append."""
    delta_path = tmp_path / "delta_merge_schema"

    initial_data = [
        Row(id=1, name="Alice"),
        Row(id=2, name="Bob"),
    ]
    df1 = spark.createDataFrame(initial_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Append data with additional column using mergeSchema
    extended_data = [
        Row(id=3, name="Charlie", age=30),
        Row(id=4, name="Diana", age=25),
    ]
    df2 = spark.createDataFrame(extended_data)
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

    # Verify the merged schema and data
    result_df = spark.read.format("delta").load(str(delta_path)).sort("id")
    result_pandas = result_df.toPandas()

    expected_data = pd.DataFrame(
        {"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Charlie", "Diana"], "age": [None, None, 30, 25]}
    ).astype({"id": "int32", "name": "string", "age": "Int32"})

    pd.testing.assert_frame_equal(
        result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=False
    )


def test_delta_schema_enforcement_without_merge_schema(spark, tmp_path):
    """Test that mergeSchema=false (default) rejects new columns."""
    delta_path = tmp_path / "delta_no_merge_schema"

    initial_data = [Row(id=1, name="Alice")]
    df1 = spark.createDataFrame(initial_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Try to append data with additional column without mergeSchema
    extended_data = [Row(id=2, name="Bob", age=30)]
    df2 = spark.createDataFrame(extended_data)

    with pytest.raises(Exception, match=r"(?i)(schema|field)"):
        df2.write.format("delta").mode("append").save(str(delta_path))


def test_delta_schema_allows_safe_cast_without_merge(spark, tmp_path):
    """Appending columns that can be safely widened should succeed without mergeSchema."""
    delta_path = tmp_path / "delta_safe_cast"

    table_schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("score", LongType(), True),
        ]
    )
    initial_data = [Row(id=1, score=100)]
    spark.createDataFrame(initial_data, schema=table_schema).write.format("delta").mode("overwrite").save(
        str(delta_path)
    )

    append_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("score", IntegerType(), True),
        ]
    )
    new_data = [Row(id=2, score=200)]
    spark.createDataFrame(new_data, schema=append_schema).write.format("delta").mode("append").save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).sort("id")
    assert result_df.schema["id"].dataType == LongType()
    assert result_df.schema["score"].dataType == LongType()
    assert [(row.id, row.score) for row in result_df.collect()] == [(1, 100), (2, 200)]


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_delta_schema_handles_timezone_timestamps(spark, tmp_path, session_timezone):
    """Ensure timezone-aware inputs are stored as UTC TimestampType values."""
    _ = session_timezone
    delta_path = tmp_path / "delta_timezone_timestamps"

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("event_time", TimestampType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(id=1, event_time=datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)),
            Row(
                id=2,
                event_time=datetime(2024, 5, 1, 11, 0, tzinfo=timezone(timedelta(hours=2))),
            ),
        ],
        schema=schema,
    ).orderBy("id")

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).orderBy("id")
    assert isinstance(result_df.schema["event_time"].dataType, TimestampType)

    result = [(row.id, _as_utc(row.event_time)) for row in result_df.collect()]
    assert result == [
        (1, datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)),
        (2, datetime(2024, 5, 1, 9, 0, tzinfo=timezone.utc)),
    ]


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_delta_schema_timestamp_ntz_cast(spark, tmp_path, session_timezone):
    """Appending TimestampNTZType data to a TimestampType table should not require mergeSchema."""
    _ = session_timezone
    delta_path = tmp_path / "delta_timestamp_ntz_cast"

    base_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("event_time", TimestampType(), True),
        ]
    )
    base_data = [Row(id=1, event_time=datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc))]
    spark.createDataFrame(base_data, schema=base_schema).write.format("delta").mode("overwrite").save(str(delta_path))

    ntz_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("event_time", TimestampNTZType(), True),
        ]
    )
    # TimestampNTZType columns expect naive datetimes from the Python client
    ntz_wall_clock = datetime(2024, 5, 2, 7, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    ntz_data = [Row(id=2, event_time=ntz_wall_clock)]
    spark.createDataFrame(ntz_data, schema=ntz_schema).write.format("delta").mode("append").save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).orderBy("id")
    assert isinstance(result_df.schema["event_time"].dataType, TimestampType)

    # Base TimestampType rows are interpreted relative to the session timezone
    assert [(row.id, _as_utc(row.event_time)) for row in result_df.collect()] == [
        (1, datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)),
        (2, datetime(2024, 5, 2, 7, 30, tzinfo=timezone.utc)),
    ]


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_delta_schema_timestamp_ntz_write_artifacts(spark, tmp_path, session_timezone):
    """TimestampNTZ partition values, stats, and Parquet metadata preserve wall-clock time."""
    _ = session_timezone
    delta_path = tmp_path / "delta_timestamp_ntz_write_artifacts"
    table_name = "delta_timestamp_ntz_write_artifacts_table"
    partition_time = "2024-01-15 10:30:00.123456"
    event_time = "2024-07-01 23:45:12.654321"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (
              id INT,
              event_time TIMESTAMP_NTZ,
              partition_time TIMESTAMP_NTZ
            )
            USING DELTA
            PARTITIONED BY (partition_time)
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            """
        )
        spark.sql(
            f"""
            INSERT INTO {table_name} VALUES (
              1,
              CAST('{event_time}' AS TIMESTAMP_NTZ),
              CAST('{partition_time}' AS TIMESTAMP_NTZ)
            )
            """  # noqa: S608
        )

        data_files = sorted(path for path in delta_path.rglob("*.parquet") if "_delta_log" not in path.parts)
        assert len(data_files) == 1
        assert data_files[0].parent.relative_to(delta_path).as_posix() == (
            "partition_time=2024-01-15%2010%3A30%3A00.123456"
        )

        add = next(action["add"] for action in reversed(_delta_log_actions(delta_path)) if "add" in action)
        assert add["partitionValues"] == {"partition_time": partition_time}
        stats = json.loads(add["stats"])
        expected_stat = "2024-07-01T23:45:12.654"
        assert stats["minValues"]["event_time"] == expected_stat
        assert stats["maxValues"]["event_time"] == expected_stat
        assert not stats["minValues"]["event_time"].endswith("Z")
        assert not stats["maxValues"]["event_time"].endswith("Z")

        parquet_schema = pq.ParquetFile(data_files[0]).schema
        event_time_column = next(
            parquet_schema.column(index)
            for index in range(len(parquet_schema))
            if parquet_schema.column(index).name == "event_time"
        )
        logical_type = str(event_time_column.logical_type)
        assert "Timestamp(" in logical_type
        assert "isAdjustedToUTC=false" in logical_type
        arrow_type = parquet_schema.to_arrow_schema().field("event_time").type
        assert pa.types.is_timestamp(arrow_type)
        assert arrow_type.tz is None
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize(
    ("timestamp_type", "sql_type", "expected_stat", "expected_protocol"),
    [
        (
            TimestampType(),
            "TIMESTAMP",
            "2024-07-01T23:45:12.654Z",
            {"minReaderVersion": 1, "minWriterVersion": 2},
        ),
        (
            TimestampNTZType(),
            "TIMESTAMP_NTZ",
            "2024-07-01T23:45:12.654",
            {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["timestampNtz"],
                "writerFeatures": ["timestampNtz", "appendOnly", "invariants"],
            },
        ),
    ],
    ids=["timestamp", "timestamp_ntz"],
)
@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_delta_timestamp_stats_do_not_replace_or_prune_microseconds(
    spark,
    tmp_path,
    session_timezone,
    timestamp_type,
    sql_type,
    expected_stat,
    expected_protocol,
):
    """Millisecond JSON stats remain conservative for driver and metadata reads."""
    _ = session_timezone
    suffix = "timestamp_ntz" if isinstance(timestamp_type, TimestampNTZType) else "timestamp"
    delta_path = tmp_path / f"delta_timestamp_stats_{suffix}"
    table_name = f"delta_timestamp_stats_{suffix}_table"
    location = escape_sql_string_literal(str(delta_path))
    expected = datetime(2024, 7, 1, 23, 45, 12, 654321)  # noqa: DTZ001
    if isinstance(timestamp_type, TimestampType):
        expected = expected.replace(tzinfo=timezone.utc)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def normalized(value):
        if isinstance(timestamp_type, TimestampType):
            return _as_utc(value)
        return value

    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id INT, event_time {sql_type})
            USING DELTA
            LOCATION '{location}'
            TBLPROPERTIES (
              'delta.checkpointInterval' = '1',
              'delta.checkpoint.writeStatsAsStruct' = 'true'
            )
            """
        ).collect()
        protocol = next(action["protocol"] for action in _delta_log_actions(delta_path) if "protocol" in action)
        assert protocol == expected_protocol
        spark.sql(
            f"""
            INSERT INTO {table_name}
            VALUES (1, CAST('2024-07-01 23:45:12.654321' AS {sql_type}))
            """  # noqa: S608
        )

        add = next(action["add"] for action in reversed(_delta_log_actions(delta_path)) if "add" in action)
        stats = json.loads(add["stats"])
        assert stats["minValues"]["event_time"] == expected_stat
        assert stats["maxValues"]["event_time"] == expected_stat
        assert list((delta_path / "_delta_log").glob("00000000000000000001.checkpoint*.parquet"))

        driver_read = spark.read.format("delta").load(str(delta_path))
        driver_value = driver_read.select("event_time").first().event_time
        assert normalized(driver_value) == expected
        bounds = driver_read.selectExpr("min(event_time) AS min_value", "max(event_time) AS max_value").first()
        assert normalized(bounds.min_value) == expected
        assert normalized(bounds.max_value) == expected

        metadata_read = spark.read.format("delta").option("metadataAsDataRead", "true").load(str(delta_path))
        filtered = metadata_read.where(f"event_time > CAST('2024-07-01 23:45:12.654100' AS {sql_type})").collect()
        assert len(filtered) == 1
        assert normalized(filtered[0].event_time) == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


@pytest.mark.parametrize(
    ("session_timezone", "local_timezone"),
    [("America/Los_Angeles", "America/Los_Angeles")],
    indirect=True,
)
def test_delta_schema_timestamp_partition_with_session_timezone(spark, tmp_path, session_timezone, local_timezone):
    """Writing TimestampType partition columns should convert to UTC inside the engine."""
    _ = session_timezone
    _ = local_timezone
    delta_path = tmp_path / "delta_timestamp_partition_session_tz"

    df = spark.createDataFrame(
        [
            Row(id=1, some_ts=datetime(2025, 11, 27, 1, 2, 3, 987654)),  # noqa: DTZ001
            Row(id=2, some_ts=datetime(1990, 11, 27, 1, 2, 3, 987654)),  # noqa: DTZ001
        ]
    )

    df.write.format("delta").mode("overwrite").partitionBy("some_ts").save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).orderBy("id")
    assert isinstance(result_df.schema["some_ts"].dataType, TimestampType)
    assert [(row.id, _as_utc(row.some_ts)) for row in result_df.collect()] == [
        (1, datetime(2025, 11, 27, 9, 2, 3, 987654, tzinfo=timezone.utc)),
        (2, datetime(1990, 11, 27, 9, 2, 3, 987654, tzinfo=timezone.utc)),
    ]


def test_delta_schema_overwrite_with_overwrite_schema(spark, tmp_path):
    """Test overwriteSchema=true with overwrite mode."""
    delta_path = tmp_path / "delta_overwrite_schema"

    initial_data = [
        Row(id=1, name="Alice", score=95.5),
        Row(id=2, name="Bob", score=87.2),
    ]
    df1 = spark.createDataFrame(initial_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Overwrite with completely different schema
    new_data = [
        Row(user_id=101, username="charlie", active=True),
        Row(user_id=102, username="diana", active=False),
    ]
    df2 = spark.createDataFrame(new_data)
    df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(delta_path))

    # Verify the new schema and data
    result_df = spark.read.format("delta").load(str(delta_path)).sort("user_id")
    result_pandas = result_df.toPandas()

    expected_data = pd.DataFrame(
        {"user_id": [101, 102], "username": ["charlie", "diana"], "active": [True, False]}
    ).astype({"user_id": "int32", "username": "string", "active": "bool"})

    pd.testing.assert_frame_equal(
        result_pandas.sort_values("user_id").reset_index(drop=True), expected_data, check_dtype=False
    )


def test_delta_schema_overwrite_fails_with_append_mode(spark, tmp_path):
    """Test that overwriteSchema=true fails with append mode."""
    delta_path = tmp_path / "delta_overwrite_schema_append"

    initial_data = [Row(id=1, name="Alice")]
    df1 = spark.createDataFrame(initial_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Try to use overwriteSchema with append mode (should fail)
    new_data = [Row(user_id=101, username="charlie")]
    df2 = spark.createDataFrame(new_data)

    with pytest.raises(Exception, match=r"(?i)overwrite.*(?:mode|schema)"):
        df2.write.format("delta").mode("append").option("overwriteSchema", "true").save(str(delta_path))


def test_delta_schema_merge_and_overwrite_fails_together(spark, tmp_path):
    """Test that specifying both mergeSchema and overwriteSchema fails."""
    delta_path = tmp_path / "delta_both_options"

    initial_data = [Row(id=1, name="Alice")]
    df1 = spark.createDataFrame(initial_data)
    df1.write.format("delta").mode("overwrite").save(str(delta_path))

    # Try to use both options (should fail)
    new_data = [Row(id=2, name="Bob")]
    df2 = spark.createDataFrame(new_data)

    with pytest.raises(Exception, match=r"(?i).*merge.*overwrite.*"):
        df2.write.format("delta").mode("append").option("mergeSchema", "true").option("overwriteSchema", "true").save(
            str(delta_path)
        )


def test_delta_schema_merge_with_compatible_types(spark, tmp_path):
    """Test mergeSchema behavior with compatible type changes."""
    delta_path = tmp_path / "delta_merge_type_changes"

    initial_schema = StructType([StructField("id", LongType(), True), StructField("value", IntegerType(), True)])
    initial_data = [Row(id=1, value=100)]
    df1 = spark.createDataFrame(initial_data, schema=initial_schema)
    df1.createOrReplaceTempView("_tw_merge_compat_initial")
    spark.sql("DROP TABLE IF EXISTS delta_merge_with_compatible_types_tbl")
    spark.sql(
        f"""
        CREATE TABLE delta_merge_with_compatible_types_tbl
        USING DELTA
        LOCATION '{escape_sql_string_literal(str(delta_path))}'
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        AS SELECT * FROM _tw_merge_compat_initial
        """  # noqa: S608
    )

    initial_table_schema = spark.read.format("delta").load(str(delta_path)).schema
    assert initial_table_schema["value"].dataType == IntegerType()

    new_schema = StructType([StructField("id", LongType(), True), StructField("value", LongType(), True)])
    new_data = [Row(id=2, value=200)]
    df2 = spark.createDataFrame(new_data, schema=new_schema)

    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path))

    final_schema = result_df.schema

    assert final_schema["value"].dataType == LongType()

    result_pandas = result_df.sort("id").toPandas()

    expected_data = pd.DataFrame({"id": [1, 2], "value": [100, 200]})

    expected_data = expected_data.astype({"id": "int64", "value": "int64"})

    pd.testing.assert_frame_equal(
        result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=True
    )

    value_field = _delta_field(_latest_delta_schema(delta_path), "value")
    type_changes = value_field["metadata"]["delta.typeChanges"]
    assert type_changes[-1]["fromType"] == "integer"
    assert type_changes[-1]["toType"] == "long"
    assert "fieldPath" not in type_changes[-1]


def test_delta_schema_merge_float_promotion(spark, tmp_path):
    """Test mergeSchema with Float32 to Float64 promotion."""
    delta_path = tmp_path / "delta_float_promotion"

    # Create table with Float32 and type widening enabled
    initial_schema = StructType([StructField("id", IntegerType(), True), StructField("value", FloatType(), True)])
    initial_data = [Row(id=1, value=1.5)]
    df1 = spark.createDataFrame(initial_data, schema=initial_schema)
    df1.createOrReplaceTempView("_tw_float_promo_initial")
    spark.sql("DROP TABLE IF EXISTS delta_merge_float_promotion_tbl")
    spark.sql(
        f"""
        CREATE TABLE delta_merge_float_promotion_tbl
        USING DELTA
        LOCATION '{escape_sql_string_literal(str(delta_path))}'
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        AS SELECT * FROM _tw_float_promo_initial
        """  # noqa: S608
    )

    # Append data with Float64
    new_schema = StructType([StructField("id", IntegerType(), True), StructField("value", DoubleType(), True)])
    new_data = [Row(id=2, value=2.5)]
    df2 = spark.createDataFrame(new_data, schema=new_schema)
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

    # Verify schema was promoted to Float64
    result_df = spark.read.format("delta").load(str(delta_path))
    final_schema = result_df.schema
    assert final_schema["value"].dataType == DoubleType()

    # Verify data integrity
    result_pandas = result_df.sort("id").toPandas()
    expected_data = pd.DataFrame({"id": [1, 2], "value": [1.5, 2.5]})
    expected_data = expected_data.astype({"id": "int32", "value": "float64"})
    pd.testing.assert_frame_equal(
        result_pandas.sort_values("id").reset_index(drop=True), expected_data, check_dtype=True
    )

    value_field = _delta_field(_latest_delta_schema(delta_path), "value")
    type_changes = value_field["metadata"]["delta.typeChanges"]
    assert type_changes[-1]["fromType"] == "float"
    assert type_changes[-1]["toType"] == "double"


def test_delta_schema_merge_decimal_type_widening(spark, tmp_path):
    """mergeSchema widens decimal precision and scale when type widening is enabled."""
    delta_path = tmp_path / "delta_invoice_decimal_widening"

    initial_schema = StructType(
        [
            StructField("invoice_id", IntegerType(), True),
            StructField("amount", DecimalType(8, 2), True),
        ]
    )
    initial_rows = [
        Row(invoice_id=10, amount=Decimal("12.30")),
        Row(invoice_id=20, amount=Decimal("0.05")),
    ]
    df1 = spark.createDataFrame(initial_rows, schema=initial_schema)
    df1.createOrReplaceTempView("_tw_decimal_initial")
    spark.sql("DROP TABLE IF EXISTS delta_merge_decimal_type_widening_tbl")
    spark.sql(
        f"""
        CREATE TABLE delta_merge_decimal_type_widening_tbl
        USING DELTA
        LOCATION '{escape_sql_string_literal(str(delta_path))}'
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        AS SELECT * FROM _tw_decimal_initial
        """  # noqa: S608
    )

    widened_schema = StructType(
        [
            StructField("invoice_id", IntegerType(), True),
            StructField("amount", DecimalType(12, 4), True),
        ]
    )
    widened_rows = [
        Row(invoice_id=30, amount=Decimal("3456.7890")),
        Row(invoice_id=40, amount=Decimal("-7.5001")),
    ]
    spark.createDataFrame(widened_rows, schema=widened_schema).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).orderBy("invoice_id")
    assert result_df.schema["amount"].dataType == DecimalType(12, 4)
    assert [(row.invoice_id, row.amount) for row in result_df.collect()] == [
        (10, Decimal("12.3000")),
        (20, Decimal("0.0500")),
        (30, Decimal("3456.7890")),
        (40, Decimal("-7.5001")),
    ]

    amount_field = _delta_field(_latest_delta_schema(delta_path), "amount")
    type_changes = amount_field["metadata"]["delta.typeChanges"]
    assert type_changes[-1]["fromType"] == "decimal(8,2)"
    assert type_changes[-1]["toType"] == "decimal(12,4)"


def test_delta_schema_merge_integral_to_fractional_type_widening(spark, tmp_path):
    """mergeSchema follows default automatic widening for integral targets."""
    cases = [
        (
            "double",
            DoubleType(),
            Row(sensor_id=2, reading=17.25),
            DoubleType(),
            [(1, "17.0"), (2, "17.25")],
            "double",
        ),
        (
            "decimal",
            DecimalType(11, 1),
            Row(sensor_id=2, reading=Decimal("17.5")),
            DecimalType(11, 1),
            [(1, "17.0"), (2, "17.5")],
            "decimal(11,1)",
        ),
    ]

    for suffix, target_type, widened_row, expected_type, expected_rows, expected_to_type in cases:
        delta_path = tmp_path / f"delta_integral_to_{suffix}_widening"
        table_name = f"delta_merge_integral_to_{suffix}_widening_tbl"
        view_name = f"_tw_integral_to_{suffix}_initial"

        initial_schema = StructType(
            [
                StructField("sensor_id", IntegerType(), True),
                StructField("reading", IntegerType(), True),
            ]
        )
        df1 = spark.createDataFrame([Row(sensor_id=1, reading=17)], schema=initial_schema)
        df1.createOrReplaceTempView(view_name)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(
            f"""
            CREATE TABLE {table_name}
            USING DELTA
            LOCATION '{escape_sql_string_literal(str(delta_path))}'
            TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
            AS SELECT * FROM {view_name}
            """  # noqa: S608
        )

        widened_schema = StructType(
            [
                StructField("sensor_id", IntegerType(), True),
                StructField("reading", target_type, True),
            ]
        )
        widened_df = spark.createDataFrame([widened_row], schema=widened_schema)

        widened_df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))

        result_df = spark.read.format("delta").load(str(delta_path)).orderBy("sensor_id")
        assert result_df.schema["reading"].dataType == expected_type
        assert [(row.sensor_id, str(row.reading)) for row in result_df.collect()] == expected_rows

        reading_field = _delta_field(_latest_delta_schema(delta_path), "reading")
        type_changes = reading_field["metadata"]["delta.typeChanges"]
        assert type_changes[-1]["fromType"] == "integer"
        assert type_changes[-1]["toType"] == expected_to_type


def test_delta_schema_merge_map_key_type_widening(spark, tmp_path):
    """mergeSchema records type widening metadata for map key promotion."""
    delta_path = tmp_path / "delta_map_key_type_widening"

    initial_schema = StructType([StructField("attrs", MapType(IntegerType(), StringType()), True)])
    initial_data = [Row(attrs={1: "one"})]
    df1 = spark.createDataFrame(initial_data, schema=initial_schema)
    df1.createOrReplaceTempView("_tw_map_key_initial")
    spark.sql("DROP TABLE IF EXISTS delta_merge_map_key_type_widening_tbl")
    spark.sql(
        f"""
        CREATE TABLE delta_merge_map_key_type_widening_tbl
        USING DELTA
        LOCATION '{escape_sql_string_literal(str(delta_path))}'
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        AS SELECT * FROM _tw_map_key_initial
        """  # noqa: S608
    )

    widened_schema = StructType([StructField("attrs", MapType(LongType(), StringType()), True)])
    widened_data = [Row(attrs={2147483648: "wide"})]
    spark.createDataFrame(widened_data, schema=widened_schema).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path))
    assert result_df.schema["attrs"].dataType == MapType(LongType(), StringType())
    assert {tuple(row.attrs.items()) for row in result_df.collect()} == {
        ((1, "one"),),
        ((2147483648, "wide"),),
    }

    attrs_field = _delta_field(_latest_delta_schema(delta_path), "attrs")
    type_changes = attrs_field["metadata"]["delta.typeChanges"]
    assert type_changes[-1]["fromType"] == "integer"
    assert type_changes[-1]["toType"] == "long"
    assert type_changes[-1]["fieldPath"] == "key"


def test_delta_schema_merge_array_element_type_widening(spark, tmp_path):
    """mergeSchema records type widening metadata for array element promotion."""
    delta_path = tmp_path / "delta_array_element_type_widening"

    initial_schema = StructType(
        [
            StructField("batch_id", IntegerType(), True),
            StructField("readings", ArrayType(IntegerType()), True),
        ]
    )
    initial_data = [
        Row(batch_id=1, readings=[3, 5, 8]),
        Row(batch_id=2, readings=[13]),
    ]
    df1 = spark.createDataFrame(initial_data, schema=initial_schema)
    df1.createOrReplaceTempView("_tw_array_element_initial")
    spark.sql("DROP TABLE IF EXISTS delta_merge_array_element_type_widening_tbl")
    spark.sql(
        f"""
        CREATE TABLE delta_merge_array_element_type_widening_tbl
        USING DELTA
        LOCATION '{escape_sql_string_literal(str(delta_path))}'
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        AS SELECT * FROM _tw_array_element_initial
        """  # noqa: S608
    )

    widened_schema = StructType(
        [
            StructField("batch_id", IntegerType(), True),
            StructField("readings", ArrayType(LongType()), True),
        ]
    )
    widened_data = [Row(batch_id=3, readings=[2147483648, -2147483649])]
    spark.createDataFrame(widened_data, schema=widened_schema).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(delta_path))

    result_df = spark.read.format("delta").load(str(delta_path)).orderBy("batch_id")
    assert result_df.schema["readings"].dataType == ArrayType(LongType())
    assert [(row.batch_id, row.readings) for row in result_df.collect()] == [
        (1, [3, 5, 8]),
        (2, [13]),
        (3, [2147483648, -2147483649]),
    ]

    readings_field = _delta_field(_latest_delta_schema(delta_path), "readings")
    type_changes = readings_field["metadata"]["delta.typeChanges"]
    assert type_changes[-1]["fromType"] == "integer"
    assert type_changes[-1]["toType"] == "long"
    assert type_changes[-1]["fieldPath"] == "element"


def test_delta_schema_merge_type_widening_requires_table_property(spark, tmp_path):
    """mergeSchema rejects widening existing columns unless type widening is enabled."""
    delta_path = tmp_path / "delta_type_widening_disabled"

    initial_schema = StructType([StructField("id", IntegerType(), True)])
    spark.createDataFrame([Row(id=1)], schema=initial_schema).write.format("delta").mode("overwrite").save(
        str(delta_path)
    )

    widened_schema = StructType([StructField("id", LongType(), True)])
    widened_df = spark.createDataFrame([Row(id=2)], schema=widened_schema)
    with pytest.raises(Exception, match=r"(?i)(type widening|enableTypeWidening|schema)"):
        widened_df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(delta_path))
