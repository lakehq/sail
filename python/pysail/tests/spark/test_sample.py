import pandas as pd
from pandas.testing import assert_frame_equal


def test_dataframe_sample_without_replacement(spark):
    df = spark.range(10)
    result = sorted([r.id for r in df.sample(False, 0.3, 42).collect()])

    assert_frame_equal(
        pd.DataFrame({"id": result}),
        pd.DataFrame({"id": [4, 8]}),
    )


def test_dataframe_sample_replacement_small_fraction(spark):
    df = spark.range(10)
    result = sorted([r.id for r in df.sample(True, 0.1, 1).collect()])

    assert_frame_equal(
        pd.DataFrame({"id": result}),
        pd.DataFrame({"id": [0, 2, 5, 7]}),
    )


def test_dataframe_sample_replacement_seed(spark):
    df = spark.range(10)
    result = sorted([r.id for r in df.sample(True, 0.5, 1).collect()])

    assert_frame_equal(
        pd.DataFrame({"id": result}),
        pd.DataFrame({"id": [0, 0, 2, 2, 5, 5, 7, 7]}),
    )
