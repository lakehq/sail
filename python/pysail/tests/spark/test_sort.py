import pandas as pd
from pandas.testing import assert_frame_equal


def test_sort(sail):
    # Reference: pyspark.sql.tests.connect.test_connect_basic.SparkConnectBasicTests.test_sort
    # We need to make sure sorting works since we have patched PySpark tests to ignore row order.
    query = """
        SELECT * FROM VALUES
        (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
        AS tab(a, b, c)
    """
    # +-----+----+----+
    # |    a|   b|   c|
    # +-----+----+----+
    # |false|   1|NULL|
    # |false|NULL| 2.0|
    # | NULL|   3| 3.0|
    # +-----+----+----+

    df = sail.sql(query)
    assert_frame_equal(
        df.sort("a").toPandas()[["a"]],
        pd.DataFrame({"a": [None, False, False]}),
    )
    assert_frame_equal(
        df.sort("a").toPandas().sort_values(by=["a", "b"], ignore_index=True),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort("c").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort("b").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [None, 1, 3], "c": [2.0, None, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c, "b").toPandas(),
        pd.DataFrame(
            {"a": [False, False, None], "b": [1, None, 3], "c": [None, 2.0, 3.0]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c.desc(), "b").toPandas(),
        pd.DataFrame(
            {"a": [None, False, False], "b": [3, None, 1], "c": [3.0, 2.0, None]},
        ).astype({"c": object}),
    )
    assert_frame_equal(
        df.sort(df.c.desc(), df.a.asc()).toPandas(),
        pd.DataFrame(
            {"a": [None, False, False], "b": [3, None, 1], "c": [3.0, 2.0, None]},
        ).astype({"c": object}),
    )
