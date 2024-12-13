import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


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
        sail.sql(sql).toPandas().sort_values("id").reset_index(drop=True), pd.DataFrame({"id": expected}, dtype="int64")
    )
