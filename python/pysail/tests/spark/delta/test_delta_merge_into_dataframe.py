import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version
from pysail.testing.spark.utils.sql import escape_sql_string_literal

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


def test_merge_into_path_target_with_temp_view_source(spark, tmp_path):
    r"""MERGE INTO using delta.`/path` syntax as target with a temp view as source.

    Regression test for https://github.com/lakehq/sail/issues/1671: the `get_merge_target_info`
    function previously failed to resolve path-based targets like `delta.\`/path\`` because it
    called `get_table_or_view()` which does not handle the `format.path` notation.
    """
    table_path = tmp_path / "delta_merge_path_target"
    table_path_str = str(table_path)

    # Create delta table at path (no named-table registration in catalog).
    target_df = spark.createDataFrame([(1, "old"), (2, "keep")], "id INT, value STRING")
    target_df.write.format("delta").mode("overwrite").save(table_path_str)

    # Register source as a temporary view.
    source_df = spark.createDataFrame([(1, "new"), (3, "insert")], "id INT, value STRING")
    source_df.createOrReplaceTempView("test_merge_path_src")

    spark.sql(f"""
        MERGE INTO delta.`{table_path_str}` AS tgt
        USING test_merge_path_src AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET tgt.value = src.value
        WHEN NOT MATCHED THEN INSERT *
    """)

    result = spark.read.format("delta").load(table_path_str).sort("id").toPandas()
    expected = pd.DataFrame({"id": [1, 2, 3], "value": ["new", "keep", "insert"]}).astype(
        {"id": "int32", "value": "string"}
    )
    assert_frame_equal(result, expected, check_dtype=False)


@pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="DataFrame arguments in spark.sql() require Spark 4+",
)
def test_merge_into_path_target_with_dataframe_source(spark, tmp_path):
    """MERGE INTO using delta.`/path` target and a DataFrame passed via spark.sql(stmt, df=df).

    Regression test for https://github.com/lakehq/sail/issues/1671: when a DataFrame is passed
    as a named argument to spark.sql(), PySpark wraps the command in a WithRelations node whose
    root is the MERGE command. Sail previously failed to handle WithRelations with a command root.
    """
    table_path = tmp_path / "delta_merge_df_arg_target"
    table_path_str = str(table_path)

    # Create delta table at path.
    target_df = spark.createDataFrame([(1, "old"), (2, "keep")], "id INT, value STRING")
    target_df.write.format("delta").mode("overwrite").save(table_path_str)

    # Source is a plain DataFrame passed as a named argument.
    source_df = spark.createDataFrame([(1, "new"), (3, "insert")], "id INT, value STRING")

    spark.sql(
        f"""
        MERGE INTO delta.`{table_path_str}` AS tgt
        USING {{src_df}} AS src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET tgt.value = src.value
        WHEN NOT MATCHED THEN INSERT *
        """,
        src_df=source_df,
    )

    result = spark.read.format("delta").load(table_path_str).sort("id").toPandas()
    expected = pd.DataFrame({"id": [1, 2, 3], "value": ["new", "keep", "insert"]}).astype(
        {"id": "int32", "value": "string"}
    )
    assert_frame_equal(result, expected, check_dtype=False)
