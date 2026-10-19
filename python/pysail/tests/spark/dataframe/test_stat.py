import math

import pytest
from pyspark.errors import AnalysisException, IllegalArgumentException


@pytest.fixture
def numbers(spark):
    return spark.createDataFrame([(x, f"{x}", 2 * x) for x in range(100)], ["col1", "col2", "col3"])


# The expected values are from Spark 3.5.9.
def test_approx_quantile_exact(numbers):
    assert numbers.stat.approxQuantile(["col1"], [0.0, 0.25, 0.5, 1.0], 0.0) == [[0.0, 24.0, 49.0, 99.0]]
    assert numbers.stat.approxQuantile("col3", [0.5], 0.0) == [98.0]


def test_approx_quantile_within_relative_error(numbers):
    probabilities = [0.1, 0.5, 0.9]
    relative_error = 0.1
    quantiles = numbers.stat.approxQuantile(["col1", "col3"], probabilities, relative_error)
    assert len(quantiles) == 2  # noqa: PLR2004
    for values, scale in zip(quantiles, [1, 2]):
        assert len(values) == len(probabilities)
        for p, q in zip(probabilities, values):
            assert isinstance(q, float)
            # The rank of the result must be within `relative_error * n` of `p * n`.
            assert abs(q / scale + 1 - p * 100) <= math.ceil(relative_error * 100)


def test_approx_quantile_ignores_null_and_nan(spark):
    df = spark.createDataFrame([(None,), (1.0,), (float("nan"),), (3.0,)], "x double")
    assert df.stat.approxQuantile("x", [0.5], 0.0) == [1.0]


def test_approx_quantile_without_values(spark):
    df = spark.createDataFrame([(None,)], "x double")
    assert df.stat.approxQuantile(["x"], [0.5], 0.0) == [[]]


def test_approx_quantile_rejects_non_numeric_column(numbers):
    with pytest.raises((AnalysisException, IllegalArgumentException)):
        numbers.stat.approxQuantile(["col2"], [0.5], 0.0)
