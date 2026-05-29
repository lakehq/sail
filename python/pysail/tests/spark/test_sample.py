import pandas as pd
from pandas.testing import assert_frame_equal


def test_dataframe_sample_replazement_seed(spark):
    df = spark.range(10)
    df2 = df.sample(True, 0.5, 1)

    assert_frame_equal(
        df2.toPandas(),
        pd.DataFrame({"id": [0, 0, 3, 4, 8]}),
    )
