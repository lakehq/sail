from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)
from pyspark.sql.types import (
    DecimalType as SparkDecimalType,
)
from pyspark.sql.types import (
    DoubleType as SparkDoubleType,
)
from pyspark.sql.types import (
    FloatType as SparkFloatType,
)
from pyspark.sql.types import (
    IntegerType as SparkIntegerType,
)
from pyspark.sql.types import (
    LongType as SparkLongType,
)
from pyspark.sql.types import (
    StringType as SparkStringType,
)
from pyspark.sql.types import (
    StructField as SparkStructField,
)
from pyspark.sql.types import (
    StructType as SparkStructType,
)

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


def test_iceberg_merge_schema_promotes_int_to_long(spark, sql_catalog):
    identifier = "default.merge_schema_int_to_long"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=IntegerType(), required=False),
        ),
    )
    try:
        initial_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("value", SparkIntegerType(), True),
            ]
        )
        spark.createDataFrame([(1, 100)], schema=initial_schema).write.format("iceberg").mode("overwrite").save(
            table.location()
        )

        promoted_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("value", SparkLongType(), True),
            ]
        )
        spark.createDataFrame([(2, 200)], schema=promoted_schema).write.format("iceberg").mode("append").option(
            "mergeSchema", "true"
        ).save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        assert result_df.schema["value"].dataType == SparkLongType()

        pdf = result_df.toPandas().reset_index(drop=True)
        expected = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_promotes_float_to_double(spark, sql_catalog):
    identifier = "default.merge_schema_float_to_double"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=FloatType(), required=False),
        ),
    )
    try:
        initial_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("value", SparkFloatType(), True),
            ]
        )
        spark.createDataFrame([(1, 1.5)], schema=initial_schema).write.format("iceberg").mode("overwrite").save(
            table.location()
        )

        promoted_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("value", SparkDoubleType(), True),
            ]
        )
        spark.createDataFrame([(2, 2.5)], schema=promoted_schema).write.format("iceberg").mode("append").option(
            "mergeSchema", "true"
        ).save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        assert result_df.schema["value"].dataType == SparkDoubleType()

        pdf = result_df.toPandas().reset_index(drop=True)
        expected = pd.DataFrame({"id": [1, 2], "value": [1.5, 2.5]})
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_promotes_decimal_precision(spark, sql_catalog):
    identifier = "default.merge_schema_decimal_precision"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="amount", field_type=DecimalType(5, 2), required=False),
        ),
    )
    try:
        initial_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("amount", SparkDecimalType(5, 2), True),
            ]
        )
        spark.createDataFrame([(1, Decimal("12.34"))], schema=initial_schema).write.format("iceberg").mode(
            "overwrite"
        ).save(table.location())

        promoted_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("amount", SparkDecimalType(10, 2), True),
            ]
        )
        spark.createDataFrame([(2, Decimal("9876.54"))], schema=promoted_schema).write.format("iceberg").mode(
            "append"
        ).option("mergeSchema", "true").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        assert result_df.schema["amount"].dataType == SparkDecimalType(10, 2)

        pdf = result_df.toPandas().reset_index(drop=True)
        expected = pd.DataFrame({"id": [1, 2], "amount": [Decimal("12.34"), Decimal("9876.54")]})
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_merge_schema_relaxes_nullability(spark, sql_catalog):
    identifier = "default.merge_schema_nullability"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="status", field_type=StringType(), required=True),
        ),
    )
    try:
        initial_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), False),
                SparkStructField("status", SparkStringType(), False),
            ]
        )
        spark.createDataFrame([(1, "good")], schema=initial_schema).write.format("iceberg").mode("overwrite").save(
            table.location()
        )

        relaxed_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), False),
                SparkStructField("status", SparkStringType(), True),
            ]
        )
        spark.createDataFrame([(2, None)], schema=relaxed_schema).write.format("iceberg").mode("append").option(
            "mergeSchema", "true"
        ).save(table.location())

        result_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        assert result_df.schema["status"].nullable

        pdf = result_df.toPandas().reset_index(drop=True)
        expected = pd.DataFrame({"id": [1, 2], "status": ["good", None]})
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_append_missing_optional_columns(spark, sql_catalog):
    identifier = "default.append_missing_optional"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="note", field_type=StringType(), required=False),
        ),
    )
    try:
        initial_schema = SparkStructType(
            [
                SparkStructField("id", SparkLongType(), True),
                SparkStructField("note", SparkStringType(), True),
            ]
        )
        spark.createDataFrame([(1, "alpha"), (2, "beta")], schema=initial_schema).write.format("iceberg").mode(
            "overwrite"
        ).save(table.location())

        partial_schema = SparkStructType([SparkStructField("id", SparkLongType(), True)])
        spark.createDataFrame([(3,)], schema=partial_schema).write.format("iceberg").mode("append").save(
            table.location()
        )

        result_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        pdf = result_df.toPandas().reset_index(drop=True)
        expected = pd.DataFrame({"id": [1, 2, 3], "note": ["alpha", "beta", None]})
        assert_frame_equal(pdf, expected.astype(pdf.dtypes))
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


def test_iceberg_overwrite_schema_rejects_dropping_partition_column(spark, sql_catalog):
    identifier = "default.overwrite_schema_partition_error"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event_ts", field_type=TimestampType(), required=False),
        NestedField(field_id=3, name="value", field_type=StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="event_ts_identity")
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema, partition_spec=spec)
    spark_table = "tmp_overwrite_schema_partition_error"
    spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
    spark.sql(
        f"""
        CREATE TABLE {spark_table}
        USING iceberg
        LOCATION '{escape_sql_string_literal(table.location())}'
        """
    )
    try:
        seed_df = spark.createDataFrame(
            [(1, datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "seed")],
            schema="id LONG, event_ts TIMESTAMP, value STRING",
        )
        seed_df.write.format("iceberg").mode("overwrite").save(table.location())

        replacement_df = spark.createDataFrame(
            [(10, "overwrite")],
            schema="id LONG, value STRING",
        )
        with pytest.raises(Exception, match=r"(?i)Partition (?:field|column)"):
            (
                replacement_df.write.format("iceberg")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(spark_table)
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        sql_catalog.drop_table(identifier)


def test_iceberg_write_allows_int_to_long_without_merge_schema(spark, sql_catalog):
    identifier = "default.safe_cast_int_to_long"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=LongType(), required=False),
        ),
    )
    try:
        df = spark.createDataFrame(
            [(1, 100)],
            schema="id LONG, value INT",
        )
        df.write.format("iceberg").mode("overwrite").save(table.location())

        result_df = spark.read.format("iceberg").load(table.location())
        assert isinstance(result_df.schema["value"].dataType, SparkLongType)
        rows = result_df.collect()
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].value == 100  # noqa: PLR2004
    finally:
        sql_catalog.drop_table(identifier)


def test_iceberg_write_rejects_incompatible_type_without_merge_schema(spark, sql_catalog):
    identifier = "default.safe_cast_incompatible"
    table = sql_catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=LongType(), required=False),
        ),
    )
    try:
        df = spark.createDataFrame(
            [(1, "bad")],
            schema="id LONG, value STRING",
        )
        with pytest.raises(Exception, match=r"(?i)Column 'value' has type"):
            df.write.format("iceberg").mode("overwrite").save(table.location())
    finally:
        sql_catalog.drop_table(identifier)
