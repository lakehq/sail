import math
from decimal import Decimal

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.testing.spark.utils.parity import assert_math_function_frames_equal, normalize_pandas_data_frame


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests.test_math_functions.
def test_transcendental_rounding_error_is_tolerated():
    left = pd.DataFrame({"acosh(b)": [math.acosh(3.0), math.nan, math.inf, -math.inf]})
    right = left.copy()
    right.iloc[0, 0] = math.nextafter(left.iloc[0, 0], math.inf)
    assert_math_function_frames_equal(left, right)


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests.test_math_functions.
@pytest.mark.parametrize(
    ("name", "left", "right"),
    [
        ("acosh(b)", 1.0, 1.0 + 1e-9),
        ("acosh(b)", math.nan, 1.0),
        ("acosh(b)", math.inf, -math.inf),
        ("acosh(b)", 1.0, 1),
        ("round(b, 1)", 1.0, math.nextafter(1.0, math.inf)),
        ("abs(b)", 1, 2),
        ("abs(c)", Decimal("1.0"), Decimal("1.000000000000001")),
        ("bin(b)", "1", "10"),
    ],
)
def test_math_comparison_retains_real_differences(name, left, right):
    with pytest.raises(AssertionError):
        assert_math_function_frames_equal(pd.DataFrame({name: [left]}), pd.DataFrame({name: [right]}))


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests.test_math_functions.
def test_math_comparison_does_not_ignore_extra_columns():
    with pytest.raises(AssertionError):
        assert_math_function_frames_equal(
            pd.DataFrame({"sin(b)": [0.0]}), pd.DataFrame({"sin(b)": [0.0], "extra": [1]})
        )


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py::SparkConnectFunctionTests.test_math_functions.
def test_math_comparison_preserves_correspondence_between_columns(monkeypatch):
    def unordered_comparison(left, right, **kwargs):
        assert_frame_equal(normalize_pandas_data_frame(left), normalize_pandas_data_frame(right), **kwargs)

    monkeypatch.setattr(pd.testing, "assert_frame_equal", unordered_comparison)
    left = pd.DataFrame({"abs(b)": [1, 2], "sin(b)": [0.1, 0.2]})
    right = pd.DataFrame({"abs(b)": [1, 2], "sin(b)": [0.2, 0.1]})
    with pytest.raises(AssertionError):
        assert_math_function_frames_equal(left, right)
