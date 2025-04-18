from typing import Any

import pandas as pd
from pyspark.sql.types import DecimalType, DoubleType, FloatType, Row


def to_pandas(df):
    """Converts a Spark DataFrame to a Pandas DataFrame.
    This function additionally converts columns of floating-point types to the
    Pandas nullable float64 type. Otherwise, such columns may have the
    `object` type in Pandas, and approximate comparison would not work.
    """

    def _to_pandas_type(dt):
        if isinstance(dt, (FloatType, DoubleType, DecimalType)):
            return pd.Float64Dtype()
        return None

    dtypes = {f.name: dt for f in df.schema.fields if (dt := _to_pandas_type(f.dataType)) is not None}
    return df.toPandas().astype(dtypes)


class StrictRow:
    """A wrapper around a PySpark row to enable strict comparison.
    Two rows are considered equal if they have the same values and the same schema.
    """

    def __init__(self, expected: Row):
        self.expected = expected

    def __repr__(self):
        return repr(self.expected)

    def __eq__(self, actual):
        if not isinstance(actual, Row):
            return False
        return self.expected == actual and self.expected.asDict(recursive=True) == actual.asDict(recursive=True)

    def __ne__(self, actual):
        return not self.__eq__(actual)


def strict(value: Any) -> Any:
    """Wrapper around a value for strict comparison in pytest assertions."""
    if isinstance(value, Row):
        return StrictRow(value)
    msg = f"unsupported type for strict comparison: {value}"
    raise TypeError(msg)
