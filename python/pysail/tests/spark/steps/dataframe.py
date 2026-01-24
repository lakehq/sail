"""Step definitions for DataFrame operations in BDD tests."""

from __future__ import annotations

from pytest_bdd import given, parsers, then, when


@given(parsers.parse('a DataFrame with columns "{columns}" and data:'), target_fixture="df")
def dataframe_with_columns_and_data(columns, datatable, spark):
    """Create a DataFrame with specified columns and data."""
    column_list = [c.strip() for c in columns.split(",")]
    _header, *rows = datatable

    # Convert string values to appropriate types
    typed_rows = []
    for row in rows:
        typed_row = []
        for val in row:
            # Try to convert to int, otherwise keep as string
            try:
                typed_row.append(int(val))
            except ValueError:
                typed_row.append(val)
        typed_rows.append(tuple(typed_row))

    return spark.createDataFrame(typed_rows, column_list)


@when(parsers.parse('I select columns using colRegex "{regex}"'), target_fixture="df")
def select_columns_with_colregex(regex, df):
    """Select columns using colRegex."""
    return df.select(df.colRegex(regex))


@then(parsers.parse('the resulting DataFrame contains only columns "{columns}"'))
def dataframe_contains_only_columns(columns, df):
    """Verify DataFrame contains only specified columns."""
    expected_columns = [c.strip() for c in columns.split(",")]
    actual_columns = df.columns
    assert actual_columns == expected_columns, f"Expected columns {expected_columns}, got {actual_columns}"


@then("the data is:")
def dataframe_data_matches(datatable, df):
    """Verify DataFrame data matches expected data."""
    _header, *expected_rows = datatable

    # Collect actual data
    actual_rows = df.collect()

    # Convert Row objects to tuples of strings for comparison
    actual_data = [[str(val) for val in row] for row in actual_rows]

    assert len(actual_data) == len(expected_rows), f"Expected {len(expected_rows)} rows, got {len(actual_data)}"

    for i, (expected, actual) in enumerate(zip(expected_rows, actual_data)):
        assert expected == actual, f"Row {i}: expected {expected}, got {actual}"


@then("the resulting DataFrame has no columns")
def dataframe_has_no_columns(df):
    """Verify DataFrame has no columns."""
    assert len(df.columns) == 0, f"Expected no columns, got {df.columns}"
