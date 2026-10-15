import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pysail.testing.spark.utils.parity import normalize_datetime_dtypes, normalize_pandas_data_frame


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


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_parity_arrow.py, ArrowParityTests.test_toPandas_duplicate_field_names.
def test_duplicate_column_labels_are_sorted_by_position():
    frame = pd.DataFrame([[2, "a"], [1, "b"], [1, "a"], [1, "a"]], columns=["id", "id"])
    expected = pd.DataFrame([[1, "a"], [1, "a"], [1, "b"], [2, "a"]], columns=["id", "id"])
    assert_frame_equal(normalize_pandas_data_frame(frame), expected)


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_parity_arrow.py, ArrowParityTests.test_toPandas_duplicate_field_names.
def test_duplicate_columns_do_not_hide_value_changes():
    left = pd.DataFrame([[1, 2]], columns=["id", "id"])
    right = pd.DataFrame([[1, 3]], columns=["id", "id"])
    with pytest.raises(AssertionError):
        assert_frame_equal(normalize_pandas_data_frame(left), normalize_pandas_data_frame(right))


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_parity_arrow.py, ArrowParityTests.test_toPandas_duplicate_field_names.
def test_datetime_normalization_with_duplicate_column_labels():
    frame = pd.concat(
        [pd.Series(["2024-01-01", None], dtype="datetime64[ns]"), pd.Series([2, 1], dtype="int32")], axis=1
    )
    frame.columns = ["same", "same"]
    expected = pd.concat(
        [pd.Series(["2024-01-01", None], dtype="datetime64[us]"), pd.Series([2, 1], dtype="int32")], axis=1
    )
    expected.columns = frame.columns
    original = frame.copy(deep=True)
    assert_frame_equal(normalize_datetime_dtypes(frame), expected)
    assert_frame_equal(frame, original)


# Sail comparator regression for Spark 3 (3.5.9):
# pyspark/sql/tests/connect/test_parity_arrow.py, ArrowParityTests.test_toPandas_duplicate_field_names.
def test_duplicate_empty_columns_preserve_dtypes():
    frame = pd.concat([pd.Series([], dtype="int32"), pd.Series([], dtype="object")], axis=1)
    frame.columns = ["same", "same"]
    assert_frame_equal(normalize_datetime_dtypes(normalize_pandas_data_frame(frame)), frame)
