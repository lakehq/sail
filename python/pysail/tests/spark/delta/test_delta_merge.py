import pytest
from pyspark.sql.types import Row

from ..utils import escape_sql_string_literal, is_jvm_spark


@pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake MERGE INTO")
def test_delta_merge_basic_insert_update_delete(spark, tmp_path):
    delta_path = tmp_path / "delta_merge_table"
    delta_table_path = f"{delta_path}"

    # Initial target table
    target_data = [
        Row(id=1, value="old", flag="keep"),
        Row(id=2, value="old", flag="update"),
        Row(id=3, value="old", flag="delete"),
    ]
    target_df = spark.createDataFrame(target_data)
    target_df.write.format("delta").mode("overwrite").save(delta_table_path)

    # Source rows that will drive MERGE behavior
    # flag is only used for inserted rows; matched rows keep target.flag
    source_data = [
        Row(id=2, value="new", flag="insert"),  # update id=2 (flag from target)
        Row(id=3, value="any", flag="ignored"),  # match id=3 to trigger delete
        Row(id=4, value="ins", flag="insert"),  # insert id=4
    ]
    source_df = spark.createDataFrame(source_data)
    source_df.createOrReplaceTempView("src_merge_delta")

    table_name = "delta_merge_basic"
    spark.sql(
        f"CREATE TABLE {table_name} USING delta LOCATION '{escape_sql_string_literal(delta_table_path)}'"
    )

    # Basic MERGE:
    # - when matched and flag='update' -> update value
    # - when matched and flag='delete' -> delete
    # - when not matched -> insert
    try:
        spark.sql(
            f"""
            MERGE INTO {table_name} AS t
            USING src_merge_delta AS s
            ON t.id = s.id
            WHEN MATCHED AND t.flag = 'update' THEN
              UPDATE SET value = s.value
            WHEN MATCHED AND t.flag = 'delete' THEN
              DELETE
            WHEN NOT MATCHED THEN
              INSERT *
            """
        )

        result = (
            spark.read.format("delta")
            .load(delta_table_path)
            .select("id", "value", "flag")
            .sort("id")
            .collect()
        )

        assert result == [
            Row(id=1, value="old", flag="keep"),  # unchanged
            Row(id=2, value="new", flag="update"),  # updated
            # id=3 deleted
            Row(id=4, value="ins", flag="insert"),  # inserted
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

