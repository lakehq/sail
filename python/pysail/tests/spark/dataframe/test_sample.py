import pandas as pd
from pandas.testing import assert_frame_equal


def test_dataframe_sample_replazement_seed(spark):
    df = spark.createDataFrame([(0), (1), (2), (3), (4), (5), (6), (7), (8), (9)], ["id"])
    df2 = df.sample(True, 0.5, 1)

    assert_frame_equal(
        df2.toPandas(),
        pd.DataFrame({"id": [0, 0, 3, 4, 8]}),
    )


def test_dataframe_sample_with_replacement_zero_fraction(spark):
    sampled = spark.range(10, numPartitions=1).sample(True, 1e-12, 1)
    assert sampled.collect() == []

    sampled = spark.range(10, numPartitions=1).sample(True, 0.0, 1)
    assert sampled.collect() == []
