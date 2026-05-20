def test_noop_write_executes_without_path(spark):
    df = spark.range(10).selectExpr("id", "id + 1 AS value")

    df.write.format("noop").mode("overwrite").save()


def test_noop_write_does_not_create_output_path(spark, tmp_path):
    output_path = tmp_path / "noop-output"

    df = spark.range(10).selectExpr("id", "id + 1 AS value")
    df.write.format("noop").mode("overwrite").save(str(output_path))

    assert not output_path.exists()
