"""DataFrame tests for nested ``Column.withField`` replacements."""

from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812


def test_nested_with_field_propagates_nullable_column(spark):
    schema = T.StructType(
        [
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), False),
                        T.StructField(
                            "details",
                            T.StructType(
                                [
                                    T.StructField("score", T.FloatType(), False),
                                    T.StructField("label", T.StringType(), True),
                                ]
                            ),
                            False,
                        ),
                    ]
                ),
                False,
            ),
            T.StructField("replacement", T.IntegerType(), True),
        ]
    )
    dataframe = spark.createDataFrame(
        [((1, (1.5, "x")), 7), ((2, (2.5, "y")), None)],
        schema,
    )

    actual = dataframe.select(F.col("payload").withField("details.score", F.col("replacement")).alias("result"))

    assert actual.schema == T.StructType(
        [
            T.StructField(
                "result",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), False),
                        T.StructField(
                            "details",
                            T.StructType(
                                [
                                    T.StructField("score", T.IntegerType(), True),
                                    T.StructField("label", T.StringType(), True),
                                ]
                            ),
                            False,
                        ),
                    ]
                ),
                False,
            )
        ]
    )
    assert actual.collect() == [
        Row(result=Row(id=1, details=Row(score=7, label="x"))),
        Row(result=Row(id=2, details=Row(score=None, label="y"))),
    ]


def test_nested_with_field_propagates_non_null_literal(spark):
    schema = T.StructType(
        [
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), False),
                        T.StructField(
                            "details",
                            T.StructType(
                                [
                                    T.StructField("score", T.IntegerType(), True),
                                    T.StructField("label", T.StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            )
        ]
    )
    dataframe = spark.createDataFrame(
        [((1, (None, "x")),), (None,)],
        schema,
    )

    actual = dataframe.select(F.col("payload").withField("details.score", F.lit(9)).alias("result"))

    assert actual.schema == T.StructType(
        [
            T.StructField(
                "result",
                T.StructType(
                    [
                        T.StructField("id", T.IntegerType(), False),
                        T.StructField(
                            "details",
                            T.StructType(
                                [
                                    T.StructField("score", T.IntegerType(), False),
                                    T.StructField("label", T.StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            )
        ]
    )
    assert actual.collect() == [
        Row(result=Row(id=1, details=Row(score=9, label="x"))),
        Row(result=None),
    ]
