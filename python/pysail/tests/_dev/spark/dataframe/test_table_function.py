import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import ArrayType, IntegerType, Row, StringType, StructField, StructType


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM range(-1)", []),
        ("SELECT * FROM range(1)", [0]),
        ("SELECT * FROM range(2, 5)", [2, 3, 4]),
        ("SELECT * FROM range(1 + 2)", [0, 1, 2]),
        ("SELECT * FROM range(CAST('2' AS INT))", [0, 1]),
        ("SELECT * FROM range(3, 0, -1)", [1, 2, 3]),
        ("SELECT * FROM range(10, 0, -2, 3)", [2, 4, 6, 8, 10]),
    ],
)
def test_range(spark, sql, expected):
    assert_frame_equal(
        spark.sql(sql).toPandas().sort_values("id").reset_index(drop=True),
        pd.DataFrame({"id": expected}, dtype="int64"),
    )


def test_lateral_view(spark):
    df = spark.sql("""
        SELECT * FROM range(2)
            LATERAL VIEW explode(array(id, id + 1)) AS v
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0, 1, 1], "v": [0, 1, 1, 2]}, dtype="int64"),
    )

    df = spark.sql("""
        SELECT * FROM range(2)
            LATERAL VIEW explode(array(id, id + 1)) t AS u
            LATERAL VIEW explode(array(u, t.u * 2)) AS v
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame(
            {"id": [0, 0, 0, 0, 1, 1, 1, 1], "u": [0, 0, 1, 1, 1, 1, 2, 2], "v": [0, 0, 1, 2, 1, 2, 2, 4]},
            dtype="int64",
        ),
    )


def test_lateral_view_outer(spark):
    df = spark.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW explode(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == []

    df = spark.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW OUTER explode(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == [Row(id=0, v=None)]

    df = spark.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW explode_outer(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == [Row(id=0, v=None)]


def test_explode_outer_null_empty_nonempty(spark):
    df = spark.sql("""
        SELECT case_id, explode_outer(items) AS item
        FROM VALUES
            (0, CAST(NULL AS ARRAY<INT>)),
            (1, CAST(ARRAY() AS ARRAY<INT>)),
            (2, ARRAY(10, 20))
        AS t(case_id, items)
        ORDER BY case_id, item
    """)
    assert df.collect() == [
        Row(case_id=0, item=None),
        Row(case_id=1, item=None),
        Row(case_id=2, item=10),
        Row(case_id=2, item=20),
    ]


def test_posexplode_outer_null_empty_nonempty(spark):
    df = spark.sql("""
        SELECT case_id, posexplode_outer(items) AS (pos, item)
        FROM VALUES
            (0, CAST(NULL AS ARRAY<INT>)),
            (1, CAST(ARRAY() AS ARRAY<INT>)),
            (2, ARRAY(10, 20))
        AS t(case_id, items)
        ORDER BY case_id, pos
    """)
    assert df.collect() == [
        Row(case_id=0, pos=None, item=None),
        Row(case_id=1, pos=None, item=None),
        Row(case_id=2, pos=0, item=10),
        Row(case_id=2, pos=1, item=20),
    ]


def test_inline_outer_null_empty_nonempty(spark):
    item_type = StructType(
        [
            StructField("number", IntegerType(), True),
            StructField("label", StringType(), True),
        ]
    )
    schema = StructType(
        [
            StructField("case_id", IntegerType(), False),
            StructField("items", ArrayType(item_type, True), True),
        ]
    )
    df = spark.createDataFrame(
        [
            (0, None),
            (1, []),
            (2, [Row(number=10, label="a"), Row(number=20, label="b")]),
        ],
        schema,
    )
    df = df.selectExpr("case_id", "inline_outer(items) AS (number, label)").orderBy("case_id", "number")
    assert df.collect() == [
        Row(case_id=0, number=None, label=None),
        Row(case_id=1, number=None, label=None),
        Row(case_id=2, number=10, label="a"),
        Row(case_id=2, number=20, label="b"),
    ]


def test_lateral_join(spark):
    df = spark.sql("""
        SELECT * FROM range(1), LATERAL explode(array(id, id + 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0], "v": [0, 1]}, dtype="int64"),
    )

    df = spark.sql("""
        SELECT * FROM range(1) JOIN LATERAL explode(array(id, id + 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0], "v": [0, 1]}, dtype="int64"),
    )


def test_lateral_join_without_table(spark):
    df = spark.sql("""
        SELECT * FROM LATERAL explode(array(0, 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"v": [0, 1]}, dtype="int32"),
    )
