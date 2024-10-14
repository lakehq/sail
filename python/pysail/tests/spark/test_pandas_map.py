import pandas as pd
from pandas.testing import assert_frame_equal


def test_map_in_pandas(sail):
    df = sail.createDataFrame([(1, "Alice"), (2, "Bob")], schema="id long, name string")

    def f(it):
        for pdf in it:
            pdf["id"] = pdf["id"] + 1
            pdf["name"] = pdf["name"].str.upper()
            yield pdf

    actual = df.mapInPandas(f, "id long, name string").toPandas()
    expected = pd.DataFrame({"id": [2, 3], "name": ["ALICE", "BOB"]})
    assert_frame_equal(actual, expected)
