"""DataFrame tests for nested ``Column.withField`` replacements."""

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version


@pytest.mark.parametrize("field_name", ["target", "added"])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("replacement_type", ["interval", "string"])
def test_with_field_preserves_new_logical_type_and_clears_user_metadata(spark, field_name, nested, replacement_type):
    from datetime import timedelta

    old_type = T.DayTimeIntervalType(T.DayTimeIntervalType.HOUR, T.DayTimeIntervalType.HOUR)
    new_type = (
        T.DayTimeIntervalType(T.DayTimeIntervalType.DAY, T.DayTimeIntervalType.DAY)
        if replacement_type == "interval"
        else T.StringType()
    )
    original_fields = [
        T.StructField("target", old_type, False, {"comment": "old target"}),
        T.StructField("keep", T.IntegerType(), False, {"comment": "keep me"}),
    ]
    payload_type = T.StructType(original_fields)
    source_payload_type = T.StructType(
        [T.StructField("target", T.DayTimeIntervalType(), False, original_fields[0].metadata), original_fields[1]]
    )
    if nested:
        payload_type = T.StructType([T.StructField("details", payload_type, True, {"comment": "old parent"})])
        source_payload_type = T.StructType(
            [T.StructField("details", source_payload_type, True, {"comment": "old parent"})]
        )
    source_value_type = T.DayTimeIntervalType() if replacement_type == "interval" else T.StringType()
    schema = T.StructType(
        [T.StructField("payload", source_payload_type), T.StructField("replacement", source_value_type)]
    )
    payload = (timedelta(hours=1), 7)
    if nested:
        payload = (payload,)
    value = timedelta(days=2) if replacement_type == "interval" else "new"
    # Spark Connect transports durations as DAY TO SECOND; apply narrower qualifiers on the server.
    frame = spark.createDataFrame([(payload, value), (payload, None), (None, value)], schema).select(
        F.col("payload").cast(payload_type).alias("payload"),
        F.col("replacement").cast(new_type).alias("replacement", metadata={"comment": "new value"}),
    )
    path = f"details.{field_name}" if nested else field_name
    result = frame.select(F.col("payload").withField(path, F.col("replacement")).alias("result"))

    # WithField replaces the logical type and clears user metadata only on changed fields.
    new_field = T.StructField(field_name, new_type, True)
    fields = [new_field, original_fields[1]] if field_name == "target" else [*original_fields, new_field]
    expected_type = T.StructType(fields)
    if nested:
        expected_type = T.StructType([T.StructField("details", expected_type)])
    assert result.schema == T.StructType([T.StructField("result", expected_type)])

    expected = []
    for replacement in [value, None]:
        row = {"target": timedelta(hours=1), "keep": 7}
        row[field_name] = replacement
        expected.append({"result": {"details": row} if nested else row})
    expected.append({"result": None})
    assert [row.asDict(recursive=True) for row in result.collect()] == expected
    # Spark's toArrow() rejects NULL structs with non-nullable children; collect() covers that row.
    if pyspark_version() >= (4,):
        assert result.where(F.col("result").isNotNull()).toArrow().to_pylist() == expected[:-1]


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
