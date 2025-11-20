import uuid

from pyiceberg.schema import Schema
from pyiceberg.types import FloatType, IntegerType, ListType, NestedField, StringType, UUIDType
from pyspark.sql.types import LongType as SparkLongType
import pytest

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

        rows = (
            spark.read.format("iceberg").load(table.location()).sort("id").collect()
        )
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
        df_long.write.format("iceberg").option("mergeSchema", "true").mode("append").save(
            table.location()
        )

        result = spark.read.format("iceberg").load(table.location()).sort("id")
        assert isinstance(result.schema["data"].dataType.elementType, SparkLongType)
        rows = result.collect()
        assert rows[1].data == [30, 40]
    finally:
        sql_catalog.drop_table(identifier)


def test_pruning_exotic_types(spark, sql_catalog):
    identifier = "default.pruning_exotic"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="f_val", field_type=FloatType(), required=False),
        NestedField(field_id=3, name="u_val", field_type=UUIDType(), required=False),
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema)
    try:
        spark.createDataFrame(
            [(1, 0.5, uuid.uuid4().bytes)],
            schema="id INT, f_val FLOAT, u_val BINARY",
        ).write.format("iceberg").mode("append").save(table.location())

        spark.createDataFrame(
            [(2, 10.5, uuid.uuid4().bytes)],
            schema="id INT, f_val FLOAT, u_val BINARY",
        ).write.format("iceberg").mode("append").save(table.location())

        filtered = spark.read.format("iceberg").load(table.location()).filter("f_val > 5.0")
        rows = filtered.collect()
        assert len(rows) == 1
        assert rows[0].id == 2
    finally:
        sql_catalog.drop_table(identifier)

