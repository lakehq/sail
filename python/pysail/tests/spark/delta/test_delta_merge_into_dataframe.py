import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from pysail.tests.spark.utils import escape_sql_string_literal, is_jvm_spark

if is_jvm_spark():
    pytest.skip("mergeInto integration test targets Spark Connect", allow_module_level=True)

if not hasattr(SparkDataFrame, "mergeInto"):
    pytest.skip("DataFrame.mergeInto requires Spark 4.0+ (missing in this PySpark)", allow_module_level=True)


def test_dataframe_merge_into_basic(spark, tmp_path):
    table_name = "delta_merge_into_df"
    table_path = tmp_path / "delta_merge_target"
    table_path_literal = escape_sql_string_literal(str(table_path))

    target_df = spark.createDataFrame([(1, "old"), (2, "keep")], "id INT, value STRING")
    target_df.write.format("delta").mode("overwrite").save(str(table_path))

    spark.sql(f"CREATE TABLE {table_name} (id INT, value STRING) USING DELTA LOCATION '{table_path_literal}'")
    try:
        source_df = spark.createDataFrame([(1, "new"), (3, "insert")], "src_id INT, src_value STRING")
        (
            source_df.mergeInto(table_name, F.expr("id = src_id"))
            .whenMatched()
            .update(assignments={"value": F.col("src_value")})
            .whenNotMatched()
            .insert(assignments={"id": F.col("src_id"), "value": F.col("src_value")})
            .merge()
        )

        result = spark.table(table_name).sort("id").toPandas()
        expected = pd.DataFrame({"id": [1, 2, 3], "value": ["new", "keep", "insert"]}).astype(
            {"id": "int32", "value": "string"}
        )
        assert_frame_equal(result, expected, check_dtype=False)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_dataframe_merge_into_insert_all_and_delete(spark, tmp_path):
    table_name = "delta_merge_into_df_insert_all_delete"
    table_path = tmp_path / "delta_merge_target_insert_all_delete"
    table_path_literal = escape_sql_string_literal(str(table_path))

    target_df = spark.createDataFrame([(14, "old_tom"), (23, "old_alice"), (99, "orphan")], "id INT, name STRING")
    target_df.write.format("delta").mode("overwrite").save(str(table_path))

    spark.sql(f"CREATE TABLE {table_name} (id INT, name STRING) USING DELTA LOCATION '{table_path_literal}'")
    try:
        source_df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], "id INT, name STRING")

        (
            source_df.mergeInto(table_name, F.col("id") == source_df["id"])
            .whenMatched()
            .update(assignments={"name": source_df["name"]})
            .whenNotMatched()
            .insertAll()
            .whenNotMatchedBySource()
            .delete()
            .merge()
        )

        result = spark.table(table_name).sort("id").toPandas()
        expected = pd.DataFrame({"id": [14, 16, 23], "name": ["Tom", "Bob", "Alice"]}).astype(
            {"id": "int32", "name": "string"}
        )
        assert_frame_equal(result, expected, check_dtype=False)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
