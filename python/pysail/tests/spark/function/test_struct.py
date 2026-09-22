import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_struct_preserves_lambda_field_name(spark):
    observations = spark.createDataFrame(
        [([(-2.5,), (6.0,)],), ([(8.0,), (16.5,)],)],
        "readings array<struct<temperature:double>>",
    )

    result = observations.select(
        F.exists("readings", lambda reading: F.struct(reading.temperature).temperature < 0).alias("freezing")
    )

    assert result.collect() == [Row(freezing=True), Row(freezing=False)]
