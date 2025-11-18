import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, StructType

from pysail.tests.spark.utils import escape_sql_string_literal


def test_iceberg_write_overwrite_and_read(spark, sql_catalog):
    identifier = "default.write_overwrite"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        ),
    )
    try:
        df = spark.createDataFrame(
            [(10, "A", 0.98), (11, "B", 0.54), (12, "A", 0.76)],
            schema="id LONG, event STRING, score DOUBLE",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame(
            {
                "id": [10, 11, 12],
                "event": ["A", "B", "A"],
                "score": [0.98, 0.54, 0.76],
            }
        )
        pdf = result_df.toPandas()
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_write_overwrite_existing_data(spark, sql_catalog):
    identifier = "default.write_overwrite_existing"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        ),
    )
    try:
        seed_df = pd.DataFrame({"id": [1, 2, 3], "event": ["X", "Y", "Z"], "score": [0.1, 0.2, 0.3]})
        table.append(pa.Table.from_pandas(seed_df))

        df = spark.createDataFrame(
            [(10, "A", 0.98), (11, "B", 0.54), (12, "A", 0.76)],
            schema="id LONG, event STRING, score DOUBLE",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame(
            {
                "id": [10, 11, 12],
                "event": ["A", "B", "A"],
                "score": [0.98, 0.54, 0.76],
            }
        )
        pdf = result_df.toPandas()
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_write_append_mode(spark, sql_catalog):
    identifier = "default.write_append"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="event", field_type=StringType(), required=False),
        ),
    )
    try:
        seed_df = pd.DataFrame({"id": [1, 2], "event": ["a", "b"]})
        table.append(pa.Table.from_pandas(seed_df))

        df2 = spark.createDataFrame([(3, "c"), (4, "d")], schema="id LONG, event STRING")
        df2.write.format("iceberg").mode("append").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame({"id": [1, 2, 3, 4], "event": ["a", "b", "c", "d"]})
        assert_frame_equal(result_df.toPandas(), expected.astype(result_df.toPandas().dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_sql_read_after_write(spark, sql_catalog):
    identifier = "default.write_sql_table"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
    )
    try:
        seed_df = pd.DataFrame({"id": [1], "name": ["alice"]})
        table.append(pa.Table.from_pandas(seed_df))

        df = spark.createDataFrame([(2, "bob")], schema="id LONG, name STRING")
        df.write.format("iceberg").mode("append").save(table.location())

        table_path = table.location()
        spark.sql(f"CREATE TABLE tmp_ice USING iceberg LOCATION '{escape_sql_string_literal(table_path)}'")
        try:
            result_df = spark.sql("SELECT * FROM tmp_ice").sort("id")
            expected = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
            assert_frame_equal(result_df.toPandas(), expected.astype(result_df.toPandas().dtypes))
        finally:
            spark.sql("DROP TABLE IF EXISTS tmp_ice")
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_append_bootstrap_first_snapshot(spark, sql_catalog):
    identifier = "default.append_bootstrap"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        ),
    )
    try:
        df = spark.createDataFrame([(10, "x"), (20, "y")], schema="id LONG, value STRING")
        df.write.format("iceberg").mode("append").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame({"id": [10, 20], "value": ["x", "y"]})
        assert_frame_equal(result_df.toPandas(), expected.astype(result_df.toPandas().dtypes))

        table.refresh()
        assert table.current_snapshot() is not None
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_append_adds_new_column(spark, sql_catalog):
    identifier = "default.merge_schema_append"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
    )
    try:
        base_df = spark.createDataFrame([(1, "alice"), (2, "bob")], schema="id LONG, name STRING")
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(3, "carol", 30), (4, "dave", 34)],
            schema="id LONG, name STRING, age INT",
        )
        (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))

        result_df = spark.read.format("iceberg").load(table.location()).sort("id")
        expected = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["alice", "bob", "carol", "dave"],
                "age": [None, None, 30, 34],
            }
        )
        pdf = result_df.toPandas()
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_missing_option_errors(spark, sql_catalog):
    identifier = "default.merge_schema_error"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
    )
    try:
        spark.createDataFrame([(1, "alice")], schema="id LONG, name STRING").write.format("iceberg").mode(
            "overwrite"
        ).save(table.location())

        new_df = spark.createDataFrame([(2, "bob", 30)], schema="id LONG, name STRING, age INT")
        with pytest.raises(Exception, match=r"(?i)mergeSchema"):
            new_df.write.format("iceberg").mode("append").save(table.location())
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_nested_struct(spark, sql_catalog):
    identifier = "default.merge_schema_nested_struct"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(
                field_id=2,
                name="payload",
                field_type=StructType(NestedField(field_id=3, name="inner", field_type=LongType(), required=False)),
                required=False,
            ),
        ),
    )
    try:
        base_df = spark.createDataFrame(
            [(1, (10,))],
            schema="id LONG, payload STRUCT<inner: LONG>",
        )
        base_df.write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, (20, "note"))],
            schema="id LONG, payload STRUCT<inner: LONG, extra: STRING>",
        )
        (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))

        result = (
            spark.read.format("iceberg")
            .load(table.location())
            .orderBy("id")
            .selectExpr("id", "payload.inner AS inner", "payload.extra AS extra")
        )
        expected = pd.DataFrame({"id": [1, 2], "inner": [10, 20], "extra": [None, "note"]})
        pdf = result.toPandas()
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_nested_struct_missing_option_errors(spark, sql_catalog):
    identifier = "default.merge_schema_nested_struct_error"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(
                field_id=2,
                name="payload",
                field_type=StructType(NestedField(field_id=3, name="inner", field_type=LongType(), required=False)),
                required=False,
            ),
        ),
    )
    try:
        spark.createDataFrame(
            [(1, (10,))],
            schema="id LONG, payload STRUCT<inner: LONG>",
        ).write.format("iceberg").mode("overwrite").save(table.location())

        evolved_df = spark.createDataFrame(
            [(2, (20, "note"))],
            schema="id LONG, payload STRUCT<inner: LONG, extra: STRING>",
        )
        with pytest.raises(Exception, match=r"(?i)mergeSchema"):
            evolved_df.write.format("iceberg").mode("append").save(table.location())
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_overwrite_schema_replaces_columns(spark, sql_catalog):
    identifier = "default.overwrite_schema"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        ),
    )
    try:
        spark.createDataFrame([(1, "alice"), (2, "bob")], schema="id LONG, name STRING").write.format("iceberg").mode(
            "overwrite"
        ).save(table.location())

        replacement_df = spark.createDataFrame(
            [(10, True), (20, False)],
            schema="user_id LONG, active BOOLEAN",
        )
        (
            replacement_df.write.format("iceberg")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table.location())
        )

        result_df = spark.read.format("iceberg").load(table.location()).sort("user_id")
        expected = pd.DataFrame({"user_id": [10, 20], "active": [True, False]})
        pdf = result_df.toPandas()
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)
