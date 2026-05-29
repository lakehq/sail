import uuid


def test_stat_cov_matches_spark_null_inputs(spark):
    df = spark.createDataFrame([(1.0, None), (None, 2.0)], "x double, y double")

    assert df.stat.cov("x", "y") == -1.0


def test_stat_cov_matches_spark_single_row(spark):
    df = spark.createDataFrame([(1.0, 2.0)], "x double, y double")

    assert df.stat.cov("x", "y") == 0.0


def test_describe_computes_numeric_stats_for_string_columns(spark):
    table_name = f"test_describe_numeric_strings_{uuid.uuid4().hex}"
    spark.createDataFrame([(0, "0"), (1, "1"), (2, "2")], "id int, name string").write.saveAsTable(table_name)
    try:
        rows = {row["summary"]: row.asDict() for row in spark.read.table(table_name).describe("id", "name").collect()}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    assert rows["mean"]["name"] == "1.0"
    assert rows["stddev"]["name"] == "1.0"
