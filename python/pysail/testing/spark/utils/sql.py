import itertools
import re
from decimal import Decimal
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
        if isinstance(dt, FloatType | DoubleType | DecimalType):
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

    def __hash__(self):
        return hash(tuple(self.expected))


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

    def __hash__(self):
        msg = "AnyOf instances are not hashable because they may equal multiple distinct values."
        raise TypeError(msg)

    def __repr__(self):
        return f"AnyOf({', '.join(repr(v) for v in self.values)})"


def any_of(*values):
    """Wrapper around a value for comparison with any of the values in a list."""
    return AnyOf(*values)


def escape_sql_identifier(s: str) -> str:
    """Escapes a string for use as a SQL identifier enclosed in backticks.
    Backtick characters in the raw string are replaced with two backticks.
    """
    return s.replace("`", "``")


def escape_sql_string_literal(s: str) -> str:
    """Escapes a string for use in SQL literals.
    All non-ASCII characters remain unchanged,
    while ASCII characters are converted to their octal representation
    unless they are alphanumeric.
    """
    return "".join(
        f"\\{ord(c):03o}" if ord(c) < 128 and not c.isalnum() else c  # noqa: PLR2004
        for c in s
    )


def parse_show_string(text) -> list[list[str]]:
    """
    Parses `DataFrame.show()` text into a list of rows including the header row.
    The leading and trailing whitespace for each cell is stripped.
    """

    lines = [line for line in text.splitlines() if line.strip()]
    border, header, _, *data, _ = lines
    # determine column width by the positions of the `+` character in the first line
    positions = [i for i, c in enumerate(border) if c == "+"]
    columns = []
    for start, end in itertools.pairwise(positions):
        columns.append(header[start + 1 : end].strip())
    result = [columns]
    for line in data:
        row = []
        for start, end in itertools.pairwise(positions):
            row.append(line[start + 1 : end].strip())
        result.append(row)
    return result


def format_show_string(rows: list[list[str]], normalizer=None):
    """
    Formats the show string result as a simple table.
    This is useful to save the result as a string in the snapshot.

    The optional `normalizer` function can be used to normalize the cell values before formatting.
    """
    header, *values = rows
    if normalizer is not None:
        values = [[normalizer(cell) for cell in row] for row in values]
        rows = [header, *values]
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    output = [" | ".join(f"{cell:{widths[i]}}" for i, cell in enumerate(row)) for row in rows]
    return "\n".join(f"| {x} |" for x in output)


def normalize_floating_point_string(s: str, d: int = 6) -> str:
    """Normalizes a string representation of a floating-point number.
    For example, it can convert "1.230000001" to "1.23" and "1.2339999991" to "1.234".
    It detects noisy fractions of `d` or more consecutive 0s or 9s close to the end of the number
    and rounds the number to remove them.
    This is useful to make test assertions more stable against minor floating-point differences.
    """
    if d < 1:
        msg = f"number of consecutive digits to detect must be at least 1 but got {d}"
        raise ValueError(msg)

    number = s.lower()
    if "e" in number:
        i = number.index("e")
        mantissa, exponent = s[:i], s[i:]
    else:
        mantissa, exponent = s, ""

    try:
        if exponent:
            _ = int(exponent[1:])
        integer, fraction = mantissa.split(".", 1)
        _ = int(integer)
        _ = int(fraction)
    except ValueError:
        return s

    match = re.search(f"(0{{{d},}}[0-9]+$)|(9{{{d},}}[0-9]+$)", fraction)
    if not match:
        return s

    start = match.start()

    # handle the case of consecutive 0s
    if match.group().startswith("0"):
        if start == 0:
            return f"{integer}{exponent}"
        return f"{integer}.{fraction[:start]}{exponent}"

    # handle the case of consecutive 9s
    prefix = f"{integer}.{fraction[:start]}" if start > 0 else f"{integer}."
    val = Decimal(prefix)
    increment = Decimal(f"1e-{start}")
    if integer.startswith("-"):
        val -= increment
    else:
        val += increment
    return f"{val!s}{exponent}"
