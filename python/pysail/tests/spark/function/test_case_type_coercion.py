from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_when_with_inferred_bigint_sequence_bound(spark):
    df = spark.createDataFrame([(3,), (1,), (5,)], ["c"])
    assert df.schema["c"].dataType == T.LongType()

    conditional = F.when(F.col("c") <= 0, F.lit(1)).otherwise(F.col("c"))
    assert df.select(conditional.alias("bound")).schema["bound"].dataType == T.LongType()

    result = df.select("c", F.explode(F.sequence(F.lit(0), conditional - 1)).alias("value"))
    assert result.schema["value"].dataType == T.LongType()
    assert [(row.c, row.value) for row in result.orderBy("c", "value").collect()] == [
        (1, 0),
        (3, 0),
        (3, 1),
        (3, 2),
        (5, 0),
        (5, 1),
        (5, 2),
        (5, 3),
        (5, 4),
    ]
