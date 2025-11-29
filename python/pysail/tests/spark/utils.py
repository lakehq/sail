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
    for start, end in zip(positions, positions[1:]):
        columns.append(header[start + 1 : end].strip())
    result = [columns]
    for line in data:
        row = []
        for start, end in zip(positions, positions[1:]):
            row.append(line[start + 1 : end].strip())
        result.append(row)
    return result


def get_data_files(path: str, extension: str = ".parquet") -> list[str]:
    """Recursively find all data files under the given path."""
    data_files = [
        os.path.relpath(os.path.join(root, file), path)
        for root, _, files in os.walk(path)
        for file in files
        if file.endswith(extension)
    ]
    return sorted(data_files)


def get_data_directory_size(path: str, extension: str = ".parquet") -> int:
    return sum(os.path.getsize(os.path.join(str(path), f)) for f in os.listdir(path) if f.endswith(extension))


def get_partition_structure(path: str) -> set[str]:
    """Get partition structure."""
    partitions = set()

    def _collect_partitions(current_path: str, relative_path: str = ""):
        """Recursively collect partition paths"""
        try:
            for item in os.listdir(current_path):
                item_path = os.path.join(current_path, item)
                if os.path.isdir(item_path) and "=" in item:
                    new_relative = os.path.join(relative_path, item) if relative_path else item

                    has_sub_partitions = any(
                        os.path.isdir(os.path.join(item_path, sub_item)) and "=" in sub_item
                        for sub_item in os.listdir(item_path)
                    )

                    if has_sub_partitions:
                        _collect_partitions(item_path, new_relative)
                    else:
                        partitions.add(new_relative)
        except (OSError, PermissionError):
            pass

    _collect_partitions(str(path))
    return partitions


def assert_file_count_in_partitions(path: str, expected_files_per_partition: int = 1):
    """Assert file count in partitions."""

    partitions = get_partition_structure(path)

    for partition in partitions:
        partition_path = os.path.join(str(path), partition)
        parquet_files = [f for f in os.listdir(partition_path) if f.endswith(".parquet")]
        assert len(parquet_files) == expected_files_per_partition, (
            f"Expected {expected_files_per_partition} parquet file(s) in {partition}, got {len(parquet_files)}"
        )


def assert_file_lifecycle(files_before: set[str], files_after: set[str], operation: str):
    """Assert file lifecycle changes."""
    if operation == "append":
        assert len(files_after) > len(files_before), "Append should increase file count"
        assert not files_after.issubset(files_before), "Append should add new files"
        assert files_before.issubset(files_after), "Append should preserve existing files"
    else:
        msg = f"Unknown operation: {operation}. Only 'append' is supported for file lifecycle assertions."
        raise ValueError(msg)
