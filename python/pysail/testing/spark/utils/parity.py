"""Comparators for upstream Spark parity tests."""

from pandas.api.types import is_hashable


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
