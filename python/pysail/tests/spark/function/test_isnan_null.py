import pyspark.sql.functions as F  # noqa: N812


def test_isnan_of_null_is_false(spark):
    df = spark.sql('SELECT * FROM VALUES (0, float("NAN")), (1, NULL), (2, 2.1) AS tab(a, b)')
    actual = df.select(F.isnan(df.a).alias("x"), F.isnan("b").alias("y"))
    assert [(row.x, row.y) for row in actual.collect()] == [(False, True), (False, False), (False, False)]
    assert not actual.schema["y"].nullable
