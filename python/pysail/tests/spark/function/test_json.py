import pytest


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (
            "json_object_keys(value) AS keys",
            [(0, ["a", "b"]), (1, []), (2, None), (3, None)],
        ),
        (
            "json_tuple(value, 'a', 'b') AS (a, b)",
            [(0, "1", "text"), (1, None, None), (2, None, None), (3, None, None)],
        ),
    ],
    ids=["json_object_keys", "json_tuple"],
)
def test_json_functions(spark, expression, expected):
    df = spark.createDataFrame(
        [(0, '{"a":1,"b":"text"}'), (1, "{}"), (2, "invalid"), (3, None)],
        "id INT, value STRING",
    ).repartition(2)
    rows = df.selectExpr("id", expression).orderBy("id").collect()
    assert [tuple(row) for row in rows] == expected


def test_from_json_preserves_schema_metadata(spark):
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import LongType, StructField, StructType

    metadata = {"comment": "keep me", "id": 7, "flags": [True, False]}
    schema = StructType(
        [
            StructField(
                "nested",
                StructType([StructField("value", LongType(), True, metadata)]),
                True,
                {"parent": "keep me too"},
            )
        ]
    )
    result = spark.range(1).select(F.from_json(F.lit('{"nested":{"value":1}}'), schema).alias("result"))
    assert result.schema["result"].dataType == schema
    assert result.collect()[0].result.nested.value == 1
