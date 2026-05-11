"""
Test Delta Lake schema handling (mergeSchema, overwriteSchema, evolution) in Sail.
"""

import json
import platform
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
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


def _delta_field(schema, name):
    for field in schema["fields"]:
        if field["name"] == name:
            return field
    msg = f"field {name!r} not found in Delta schema"
    raise AssertionError(msg)


class TestDeltaSchemaHandling:
    """Test Delta Lake schema handling functionality."""

    def test_delta_schema_read_with_custom_schema(self, spark, tmp_path):
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

    def test_delta_schema_evolution_with_merge_schema(self, spark, tmp_path):
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

    def test_delta_schema_enforcement_without_merge_schema(self, spark, tmp_path):
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

    def test_delta_schema_allows_safe_cast_without_merge(self, spark, tmp_path):
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
    def test_delta_schema_handles_timezone_timestamps(self, spark, tmp_path, session_timezone):
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
    def test_delta_schema_timestamp_ntz_cast(self, spark, tmp_path, session_timezone):
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
        spark.createDataFrame(base_data, schema=base_schema).write.format("delta").mode("overwrite").save(
            str(delta_path)
        )

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

    @pytest.mark.parametrize(
        ("session_timezone", "local_timezone"),
        [("America/Los_Angeles", "America/Los_Angeles")],
        indirect=True,
    )
    @pytest.mark.skipif(platform.system() == "Windows", reason="`time.tzset()` is not available on Windows")
    def test_delta_schema_timestamp_partition_with_session_timezone(
        self, spark, tmp_path, session_timezone, local_timezone
    ):
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

    def test_delta_schema_overwrite_with_overwrite_schema(self, spark, tmp_path):
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

    def test_delta_schema_overwrite_fails_with_append_mode(self, spark, tmp_path):
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

    def test_delta_schema_merge_and_overwrite_fails_together(self, spark, tmp_path):
        """Test that specifying both mergeSchema and overwriteSchema fails."""
        delta_path = tmp_path / "delta_both_options"

        initial_data = [Row(id=1, name="Alice")]
        df1 = spark.createDataFrame(initial_data)
        df1.write.format("delta").mode("overwrite").save(str(delta_path))

        # Try to use both options (should fail)
        new_data = [Row(id=2, name="Bob")]
        df2 = spark.createDataFrame(new_data)

        with pytest.raises(Exception, match=r"(?i).*merge.*overwrite.*"):
            df2.write.format("delta").mode("append").option("mergeSchema", "true").option(
                "overwriteSchema", "true"
            ).save(str(delta_path))

    def test_delta_schema_merge_with_compatible_types(self, spark, tmp_path):
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
            LOCATION '{delta_path}'
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

    def test_delta_schema_merge_float_promotion(self, spark, tmp_path):
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
            LOCATION '{delta_path}'
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

    def test_delta_schema_merge_decimal_type_widening(self, spark, tmp_path):
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
            LOCATION '{delta_path}'
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

    def test_delta_schema_merge_integral_to_fractional_type_widening(self, spark, tmp_path):
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
                LOCATION '{delta_path}'
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

    def test_delta_schema_merge_map_key_type_widening(self, spark, tmp_path):
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
            LOCATION '{delta_path}'
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

    def test_delta_schema_merge_array_element_type_widening(self, spark, tmp_path):
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
            LOCATION '{delta_path}'
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

    def test_delta_schema_merge_type_widening_requires_table_property(self, spark, tmp_path):
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
