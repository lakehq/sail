import uuid
from decimal import Decimal

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    UUIDType,
)
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DoubleType as SparkDoubleType
from pyspark.sql.types import LongType as SparkLongType
from pyspark.sql.types import MapType as SparkMapType


@pytest.mark.skip(reason="pyiceberg client does not support list literals for default values")
def test_complex_type_default_values(spark, sql_catalog):
    identifier = "default.complex_defaults"
    schema_v1 = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema_v1)
    try:
        df1 = spark.createDataFrame([(1,), (2,)], schema="id INT")
        df1.write.format("iceberg").mode("append").save(table.location())

        with table.update_schema() as update:
            update.add_column(
                "tags",
                ListType(element_id=3, element=StringType(), element_required=False),
                required=False,
                default_value=["unknown"],
            )
        table.refresh()

        rows = spark.read.format("iceberg").load(table.location()).sort("id").collect()
        assert rows[0].tags == ["unknown"]
        assert rows[1].tags == ["unknown"]
    finally:
        sql_catalog.drop_table(identifier)


def test_schema_evolution_nested_type_promotion(spark, sql_catalog):
    identifier = "default.nested_evolution"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(
            field_id=2,
            name="data",
            field_type=ListType(element_id=3, element=IntegerType(), element_required=False),
            required=False,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        spark.createDataFrame(
            [(1, [10, 20])],
            schema="id INT, data ARRAY<INT>",
        ).write.format("iceberg").mode("append").save(table.location())

        df_long = spark.createDataFrame(
            [(2, [30, 40])],
            schema="id INT, data ARRAY<LONG>",
        )
        df_long.write.format("iceberg").option("mergeSchema", "true").mode("append").save(table.location())

        result = spark.read.format("iceberg").load(table.location()).sort("id")
        assert isinstance(result.schema["data"].dataType.elementType, SparkLongType)
        rows = result.collect()
        assert rows[1].data == [30, 40]
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_nested_struct_promotes_float_to_double(spark, sql_catalog):
    identifier = "default.merge_schema_nested_struct_float_to_double"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(
            field_id=2,
            name="location",
            field_type=StructType(
                NestedField(field_id=3, name="latitude", field_type=FloatType(), required=False),
                NestedField(field_id=4, name="longitude", field_type=FloatType(), required=False),
            ),
            required=False,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        base_df = spark.createDataFrame(
            [(1, (10.0, 20.0))],
            schema="id LONG, location STRUCT<latitude: FLOAT, longitude: FLOAT>",
        )
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, (30.5, 40.5))],
            schema="id LONG, location STRUCT<latitude: DOUBLE, longitude: DOUBLE>",
        )
        (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))

        result = spark.read.format("iceberg").load(table.location()).orderBy("id")
        location_field = result.schema["location"].dataType
        assert isinstance(location_field["latitude"].dataType, SparkDoubleType)
        assert isinstance(location_field["longitude"].dataType, SparkDoubleType)
        rows = result.collect()
        assert rows[0].location.latitude == 10.0  # noqa: PLR2004
        assert rows[0].location.longitude == 20.0  # noqa: PLR2004
        assert rows[1].location.latitude == 30.5  # noqa: PLR2004
        assert rows[1].location.longitude == 40.5  # noqa: PLR2004
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_map_value_promotes_int_to_long(spark, sql_catalog):
    identifier = "default.merge_schema_map_value_int_to_long"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(
            field_id=2,
            name="metrics",
            field_type=MapType(
                key_id=3,
                key_type=StringType(),
                value_id=4,
                value_type=IntegerType(),
                value_required=False,
            ),
            required=False,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        base_df = spark.createDataFrame(
            [(1, {"cpu": 80, "mem": 65})],
            schema="id LONG, metrics MAP<STRING, INT>",
        )
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, {"cpu": 95, "mem": 70})],
            schema="id LONG, metrics MAP<STRING, LONG>",
        )
        (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))

        result = spark.read.format("iceberg").load(table.location()).orderBy("id")
        metrics_type = result.schema["metrics"].dataType
        assert isinstance(metrics_type, SparkMapType)
        assert isinstance(metrics_type.valueType, SparkLongType)
        rows = result.collect()
        assert rows[0].metrics == {"cpu": 80, "mem": 65}
        assert rows[1].metrics == {"cpu": 95, "mem": 70}
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_map_key_promotion(spark, sql_catalog):
    identifier = "default.merge_schema_map_key_promotion"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(
            field_id=2,
            name="metrics",
            field_type=MapType(
                key_id=3,
                key_type=IntegerType(),
                value_id=4,
                value_type=IntegerType(),
                value_required=False,
            ),
            required=False,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        base_df = spark.createDataFrame(
            [(1, {10: 80})],
            schema="id LONG, metrics MAP<INT, INT>",
        )
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, {20: 90})],
            schema="id LONG, metrics MAP<BIGINT, INT>",
        )
        with pytest.raises(Exception, match=r"(?i)Schema evolution for map keys is not allowed"):
            (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_map_key_invalid_promotion(spark, sql_catalog):
    identifier = "default.merge_schema_map_key_invalid"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(
            field_id=2,
            name="metrics",
            field_type=MapType(
                key_id=3,
                key_type=StringType(),
                value_id=4,
                value_type=IntegerType(),
                value_required=False,
            ),
            required=False,
        ),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        base_df = spark.createDataFrame(
            [(1, {"cpu": 80})],
            schema="id LONG, metrics MAP<STRING, INT>",
        )
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, {10: 90})],
            schema="id LONG, metrics MAP<INT, INT>",
        )
        with pytest.raises(
            Exception, match=r"(?i)Column 'key' has type.*Set mergeSchema=true to allow schema evolution"
        ):
            (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))
    finally:
        sql_catalog.drop_table(identifier)


def test_pruning_exotic_types(spark, sql_catalog):
    identifier = "default.pruning_exotic"
    first_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddee00")
    second_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="f_val", field_type=FloatType(), required=False),
        NestedField(field_id=3, name="u_val", field_type=UUIDType(), required=False),
        NestedField(field_id=4, name="d_val", field_type=DecimalType(9, 2), required=False),
        NestedField(field_id=5, name="fixed_val", field_type=FixedType(4), required=False),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        spark.createDataFrame(
            [(1, 0.5, first_uuid.bytes, Decimal("1.23"), b"\x00\x00\x00\x01")],
            schema="id INT, f_val FLOAT, u_val BINARY, d_val DECIMAL(9, 2), fixed_val BINARY",
        ).write.format("iceberg").mode("append").save(table.location())

        spark.createDataFrame(
            [(2, 10.5, second_uuid.bytes, Decimal("9.87"), b"\x00\x00\x00\x02")],
            schema="id INT, f_val FLOAT, u_val BINARY, d_val DECIMAL(9, 2), fixed_val BINARY",
        ).write.format("iceberg").mode("append").save(table.location())

        data = spark.read.format("iceberg").load(table.location())
        predicates = [
            F.col("f_val") > 5.0,  # noqa: PLR2004
            F.col("u_val") == F.lit(second_uuid.bytes),
            F.col("d_val") >= F.lit(Decimal("9.00")),
            F.col("fixed_val") == F.lit(b"\x00\x00\x00\x02"),
        ]
        for predicate in predicates:
            rows = data.filter(predicate).select("id").collect()
            assert [row.id for row in rows] == [2]
    finally:
        sql_catalog.drop_table(identifier)
