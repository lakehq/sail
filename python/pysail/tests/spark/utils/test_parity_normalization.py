import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.testing.spark.utils.parity import normalize_pandas_data_frame


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_string_functions_multi_args.
@pytest.mark.parametrize("binary_type", [bytes, bytearray])
def test_binary_sort_keys_preserve_values(binary_type):
    frame = pd.DataFrame({"value": [binary_type(b"ghij"), binary_type(b"efghij"), None]}, index=[0, 0, 0])
    original = frame.copy(deep=True)
    actual = normalize_pandas_data_frame(frame)
    expected = pd.DataFrame({"value": [binary_type(b"efghij"), binary_type(b"ghij"), None]})
    assert_frame_equal(actual, expected)
    assert isinstance(actual.iloc[0, 0], binary_type)
    assert_frame_equal(frame, original)


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_string_functions_multi_args.
def test_binary_representations_have_the_same_row_order():
    left = pd.DataFrame({"value": [b"ghij", b"efghij", b"ghij"]})
    right = pd.DataFrame({"value": [bytearray(b"efghij"), bytearray(b"ghij"), bytearray(b"ghij")]})
    assert_frame_equal(normalize_pandas_data_frame(left), normalize_pandas_data_frame(right))


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_string_functions_multi_args.
def test_normalization_does_not_hide_binary_value_changes():
    left = pd.DataFrame({"value": [b"a", b"b"]})
    right = pd.DataFrame({"value": [bytearray(b"a"), bytearray(b"c")]})
    with pytest.raises(AssertionError):
        assert_frame_equal(normalize_pandas_data_frame(left), normalize_pandas_data_frame(right))


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_string_functions_multi_args.
def test_no_sortable_columns_preserves_rows():
    frame = pd.DataFrame({"value": [[2], [1], [2]]}, index=[4, 3, 2])
    assert_frame_equal(normalize_pandas_data_frame(frame), frame.reset_index(drop=True))


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_string_functions_multi_args.
def test_empty_frame_preserves_schema():
    frame = pd.DataFrame({"value": pd.Series([], dtype="int32")})
    assert_frame_equal(normalize_pandas_data_frame(frame), frame)
