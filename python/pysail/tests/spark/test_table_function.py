import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row


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
def test_range(sail, sql, expected):
    assert_frame_equal(
        sail.sql(sql).toPandas().sort_values("id").reset_index(drop=True),
        pd.DataFrame({"id": expected}, dtype="int64"),
    )


def test_lateral_view(sail):
    df = sail.sql("""
        SELECT * FROM range(2)
            LATERAL VIEW explode(array(id, id + 1)) AS v
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0, 1, 1], "v": [0, 1, 1, 2]}, dtype="int64"),
    )

    df = sail.sql("""
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


def test_lateral_view_outer(sail):
    df = sail.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW explode(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == []

    df = sail.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW OUTER explode(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == [Row(id=0, v=None)]

    df = sail.sql("""
        SELECT * FROM range(1)
            LATERAL VIEW explode_outer(CAST(NULL AS array<int>)) AS v
    """)
    assert df.collect() == [Row(id=0, v=None)]


def test_lateral_join(sail):
    df = sail.sql("""
        SELECT * FROM range(1), LATERAL explode(array(id, id + 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0], "v": [0, 1]}, dtype="int64"),
    )

    df = sail.sql("""
        SELECT * FROM range(1) JOIN LATERAL explode(array(id, id + 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"id": [0, 0], "v": [0, 1]}, dtype="int64"),
    )


def test_lateral_join_without_table(sail):
    df = sail.sql("""
        SELECT * FROM LATERAL explode(array(0, 1)) AS t(v)
    """)
    assert_frame_equal(
        df.toPandas(),
        pd.DataFrame({"v": [0, 1]}, dtype="int32"),
    )
