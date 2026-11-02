"""Comparators for upstream Spark parity tests."""

from pandas._testing.asserters import assert_frame_equal
from pandas.api.types import is_float_dtype, is_hashable
from pandas.testing import assert_index_equal


def normalize_pandas_data_frame(df):
    # Arrow and the JVM can represent the same binary value as bytes or bytearray.
    # Canonicalize only the sort keys, preserving the values that assertions compare.
    keys = df.apply(lambda column: column.map(lambda value: bytes(value) if isinstance(value, bytearray) else value))
    keys = keys.reset_index(drop=True)
    # A label may identify multiple columns (for example select("id", "id")).
    # Sort by positions without changing the schema of the compared DataFrame.
    keys.columns = range(len(keys.columns))
    columns = [col for col in keys.columns if all(is_hashable(v) for v in keys[col])]
    if not columns:
        return df.reset_index(drop=True)
    order = keys.sort_values(by=columns, kind="stable").index
    return df.iloc[order].reset_index(drop=True)


def normalize_datetime_dtypes(df):
    """Normalize datetime column dtypes from nanosecond to microsecond resolution.

    Sail uses microsecond precision for timestamps (per Spark specification).
    In Pandas 2.0-2.1, Python datetime objects and ``pd.Timestamp.apply()``
    produce ``datetime64[ns]`` dtype, while Sail's ``toPandas()`` returns
    ``datetime64[us]``. This normalization ensures that dtype comparisons in
    ``assert_frame_equal`` do not fail due to this precision difference.
    """
    result = df.copy()
    for position, dtype in enumerate(result.dtypes):
        if str(dtype) == "datetime64[ns]":
            result.isetitem(position, result.iloc[:, position].astype("datetime64[us]"))
    return result


def assert_math_function_frames_equal(left, right):
    """Allow rounding error only in floating transcendental function results."""
    # Use the unpatched comparator: sorting each column subset again would lose
    # the correspondence between exact and approximate values in a row.
    assert_index_equal(left.columns, right.columns, exact=True)
    left = normalize_pandas_data_frame(left)
    right = normalize_pandas_data_frame(right)
    transcendental = {
        "acos",
        "acosh",
        "asin",
        "asinh",
        "atan",
        "atanh",
        "atan2",
        "cbrt",
        "cos",
        "cosh",
        "cot",
        "csc",
        "degrees",
        "exp",
        "expm1",
        "hypot",
        "ln",
        "log",
        "log10",
        "log1p",
        "log2",
        "pow",
        "power",
        "radians",
        "sec",
        "sin",
        "sinh",
        "sqrt",
        "tan",
        "tanh",
    }
    approximate = [
        i
        for i, name in enumerate(left.columns)
        if str(name).split("(", 1)[0].lower() in transcendental and is_float_dtype(left.iloc[:, i].dtype)
    ]
    exact = [i for i in range(len(left.columns)) if i not in approximate]
    # Preserve the complete schema, integer/decimal/string results and special values.
    assert_frame_equal(left.iloc[:, exact], right.iloc[:, exact], check_exact=True)
    assert_frame_equal(left.iloc[:, approximate], right.iloc[:, approximate], check_exact=False, rtol=1e-14, atol=1e-15)
