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


def test_when_literal_true_does_not_inherit_column_metadata(spark):
    frame = spark.range(2).select(F.col("id").alias("n", metadata={"tag": "x"}))
    result = frame.select(
        F.col("n").alias("source"),
        F.when(F.col("n") == 0, F.col("n")).otherwise(2).alias("dynamic"),
        F.when(F.lit(True), F.col("n")).otherwise(2).alias("explicit"),
        F.when(F.lit(True), F.col("n")).alias("omitted"),
        F.when(F.lit(True), F.col("n").cast("int")).otherwise(2).alias("cast_explicit"),
        F.when(F.lit(True), F.col("n").cast("int")).alias("cast_omitted"),
    )
    assert result.schema == T.StructType(
        [
            T.StructField("source", T.LongType(), False, {"tag": "x"}),
            T.StructField("dynamic", T.LongType(), False),
            T.StructField("explicit", T.LongType(), False),
            T.StructField("omitted", T.LongType(), False),
            T.StructField("cast_explicit", T.IntegerType(), False),
            T.StructField("cast_omitted", T.IntegerType(), False),
        ]
    )
    assert result.orderBy("source").collect() == [
        Row(source=0, dynamic=0, explicit=0, omitted=0, cast_explicit=0, cast_omitted=0),
        Row(source=1, dynamic=2, explicit=1, omitted=1, cast_explicit=1, cast_omitted=1),
    ]
