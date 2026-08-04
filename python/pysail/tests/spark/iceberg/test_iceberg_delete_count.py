def test_iceberg_delete_returns_affected_row_count(spark, tmp_path):
    table_name = "iceberg_delete_affected_count"
    location = (tmp_path / table_name).as_uri()
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} (id BIGINT, category STRING) USING iceberg LOCATION '{location}'")
    try:
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'delete'), (2, 'delete'), (3, 'keep')")

        result = (
            spark.sql(f"DELETE FROM {table_name} WHERE category = 'delete'")
            .selectExpr("CAST(count AS BIGINT) AS count")
            .collect()
        )

        assert [row["count"] for row in result] == [2]
        remaining = spark.table(table_name).collect()
        assert len(remaining) == 1
        assert remaining[0]["id"] == 3
        assert remaining[0]["category"] == "keep"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
