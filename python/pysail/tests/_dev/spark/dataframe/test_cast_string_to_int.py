import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_legacy_string_to_int_cast_handles_decimals_and_overflow(spark):
    expected_filter_value = 100
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        source = spark.createDataFrame(
            [
                (0, "2178802287"),
                (1, "100"),
                (2, "2147483648"),
                (3, "1.23"),
                (4, "-4.56"),
            ],
            "id INT, value STRING",
        )

        projected = source.select("id", F.col("value").cast("int").alias("result")).orderBy("id").collect()
        assert projected == [
            Row(id=0, result=None),
            Row(id=1, result=100),
            Row(id=2, result=None),
            Row(id=3, result=1),
            Row(id=4, result=-4),
        ]

        filtered = source.filter(F.col("value").cast("int") == expected_filter_value).collect()
        assert filtered == [Row(id=1, value="100")]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
