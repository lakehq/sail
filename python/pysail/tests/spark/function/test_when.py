from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_when_otherwise_widens_before_sequence(spark):
    frame = spark.createDataFrame(
        [(-1,), (1,), (3,), (None,)],
        T.StructType([T.StructField("c", T.LongType(), True)]),
    )
    result = frame.select(F.when(F.col("c") <= 0, 1).otherwise(F.col("c")).alias("stop"))
    assert result.schema == T.StructType([T.StructField("stop", T.LongType(), True)])
    assert result.orderBy("stop").collect() == [Row(stop=None), Row(stop=1), Row(stop=1), Row(stop=3)]
    actual = result.select(F.explode(F.sequence(F.lit(0), F.col("stop") - 1)).alias("i")).collect()
    assert sorted(actual) == [
        Row(i=0),
        Row(i=0),
        Row(i=0),
        Row(i=1),
        Row(i=2),
    ]
