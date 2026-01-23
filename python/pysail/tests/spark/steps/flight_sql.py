"""Step definitions for Arrow Flight SQL tests."""

import contextlib
import os

import pytest
from adbc_driver_flightsql import dbapi
from pytest_bdd import given, parsers, then, when


def get_flight_uri():
    """Get the URI for the Flight SQL server."""
    # Allow overriding with environment variable
    return os.environ.get("SAIL_FLIGHT_URI", "grpc://127.0.0.1:32010")


@given("a running Flight SQL server", target_fixture="flight_connection")
def running_flight_server():
    """Ensure Flight SQL server is running and return a connection."""
    uri = get_flight_uri()
    try:
        conn = dbapi.connect(uri)
        yield conn
        # Cleanup: close the connection after the scenario
        with contextlib.suppress(Exception):
            conn.close()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Flight SQL server not available at {uri}: {e}")


@when("I connect to the Flight SQL server", target_fixture="connection_result")
def connect_to_server(flight_connection):
    """Attempt to connect to the Flight SQL server."""
    return flight_connection is not None


@then("the connection should be successful")
def connection_successful(connection_result):
    """Verify connection was successful."""
    assert connection_result is True


@when(parsers.parse('I execute the SQL query "{query}"'), target_fixture="query_result")
def execute_query(flight_connection, query):
    """Execute a SQL query and return the result."""
    cur = flight_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return {"rows": rows, "error": None}


@when(parsers.parse('I try to execute the SQL query "{query}"'), target_fixture="query_result")
def try_execute_query(flight_connection, query):
    """Try to execute a SQL query that may fail."""
    cur = flight_connection.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
    except Exception as e:  # noqa: BLE001
        cur.close()
        return {"rows": None, "error": e}
    else:
        cur.close()
        return {"rows": rows, "error": None}


@then(parsers.parse("I should get {count:d} row"))
@then(parsers.parse("I should get {count:d} rows"))
def check_row_count(query_result, count):
    """Verify the number of rows returned."""
    assert query_result["rows"] is not None
    assert len(query_result["rows"]) == count


@then(parsers.parse("I should get at least {count:d} row"))
@then(parsers.parse("I should get at least {count:d} rows"))
def check_min_row_count(query_result, count):
    """Verify at least the specified number of rows returned."""
    assert query_result["rows"] is not None
    assert len(query_result["rows"]) >= count


@then(parsers.parse("row {index:d} should contain {expected}"))
def check_row_content(query_result, index, expected):
    """Verify row content matches expected tuple."""
    # Parse expected tuple string like "(1,)" or "(1, 2, 3)"
    expected_tuple = eval(expected)  # noqa: S307
    assert query_result["rows"][index] == expected_tuple


@then(parsers.parse("the row should have {count:d} columns"))
@then(parsers.parse("the row should have {count:d} column"))
def check_column_count(query_result, count):
    """Verify the number of columns in the first row."""
    assert len(query_result["rows"][0]) == count


@then(parsers.parse('I should get an error containing "{text}"'))
def check_error_contains(query_result, text):
    """Verify the error message contains specific text."""
    assert query_result["error"] is not None
    error_msg = str(query_result["error"]).lower()
    assert text.lower() in error_msg, f"Expected '{text}' in error: {query_result['error']}"


@then(parsers.parse("row {row:d} column {col:d} should equal {value:d}"))
def check_cell_value(query_result, row, col, value):
    """Verify a specific cell value."""
    assert query_result["rows"][row][col] == value


# Cursor management steps


@when("I create a cursor", target_fixture="current_cursor")
def create_cursor(flight_connection):
    """Create a new cursor."""
    return flight_connection.cursor()


@pytest.fixture
def cursors():
    """Fixture to store multiple named cursors in a scenario."""
    return {}


@when(parsers.parse('I create cursor "{name}"'))
def create_named_cursor(flight_connection, cursors, name):
    """Create a named cursor."""
    cursors[name] = flight_connection.cursor()


@when(parsers.parse('I execute "{query}" with the cursor'), target_fixture="cursor_result")
def execute_with_cursor(current_cursor, query):
    """Execute query with the current cursor."""
    current_cursor.execute(query)
    rows = current_cursor.fetchall()
    return rows[0][0] if rows else None


@when(parsers.parse('I execute "{query}" with the same cursor'), target_fixture="cursor_result")
def execute_with_same_cursor(current_cursor, query):
    """Execute query with the same cursor (for reuse tests)."""
    current_cursor.execute(query)
    rows = current_cursor.fetchall()
    return rows[0][0] if rows else None


@pytest.fixture
def cursor_results():
    """Fixture to store results from multiple named cursors."""
    return {}


@when(parsers.parse('I execute "{query}" with cursor "{name}"'))
def execute_with_named_cursor(cursors, cursor_results, name, query):
    """Execute query with a named cursor."""
    cursors[name].execute(query)
    rows = cursors[name].fetchall()
    cursor_results[name] = rows[0][0] if rows else None


@then(parsers.parse("the cursor result should be {value:d}"))
def check_cursor_result(cursor_result, value):
    """Verify cursor result value."""
    assert cursor_result == value


@then(parsers.parse('cursor "{name}" result should be {value:d}'))
def check_named_cursor_result(cursor_results, name, value):
    """Verify named cursor result value."""
    assert cursor_results[name] == value
