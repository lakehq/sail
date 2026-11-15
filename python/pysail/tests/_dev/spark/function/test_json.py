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
