import doctest
import os
from typing import Any

import pandas as pd
from pyspark.sql.types import DecimalType, DoubleType, FloatType, Row

# This doctest option flag is used to annotate tests involving
# extended Spark features supported by Sail.
# The test will be skipped when running on JVM Spark.
SAIL_ONLY = doctest.register_optionflag("SAIL_ONLY")


def is_jvm_spark():
    return os.environ.get("SPARK_REMOTE", "").startswith("local")


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


def strict(value: Any) -> Any:
    """Wrapper around a value for strict comparison in pytest assertions."""
    if isinstance(value, Row):
        return StrictRow(value)
    msg = f"unsupported type for strict comparison: {value}"
    raise TypeError(msg)


class AnyOf:
    """A wrapper around a value to enable comparison with any of the values in a list.
    This is useful for comparing against multiple expected values in tests.
    """

    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values

    def __repr__(self):
        return f"AnyOf({', '.join(repr(v) for v in self.values)})"


def any_of(*values):
    """Wrapper around a value for comparison with any of the values in a list."""
    return AnyOf(*values)
