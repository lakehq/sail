"""Tests for Sail Arrow Flight SQL integration using ADBC.

This module tests the connectivity and basic functionality of the Sail Arrow Flight SQL server
using the ADBC Flight SQL driver, which is the recommended way to interact with Flight SQL
servers in Python.
"""

import logging
import os
import shutil
import subprocess
import time
from contextlib import contextmanager

import adbc_driver_manager
import pytest
from adbc_driver_flightsql import dbapi

ADBC_AVAILABLE = True
PYARROW_FLIGHT_AVAILABLE = True


pytestmark = pytest.mark.skipif(
    not ADBC_AVAILABLE,
    reason="ADBC Flight SQL driver not available. Install with: pip install adbc-driver-flightsql adbc-driver-manager",
)


@contextmanager
def flight_server(host="127.0.0.1", port=32010):
    """Context manager to start and stop a Sail Flight SQL server for testing.

    Args:
        host: The host to bind the server to.
        port: The port to bind the server to.

    Yields:
        str: The URI where the server is listening (e.g., "grpc://127.0.0.1:32010").
    """
    logger = logging.getLogger(__name__)
    uri = f"grpc://{host}:{port}"

    # Check if server is already running
    try:
        conn = dbapi.connect(uri)
        conn.close()
        logger.info("Using existing Flight SQL server at %s", uri)
    except ConnectionRefusedError:
        logger.info("Server not running at %s, starting new instance", uri)
    except OSError as e:
        logger.warning("Unexpected error checking server: %s", e)
    else:
        yield uri
        return

    # Start the server process
    workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
    cargo_path = shutil.which("cargo")
    if not cargo_path:
        msg = "cargo command not found in PATH"
        raise RuntimeError(msg)

    process = subprocess.Popen(
        [cargo_path, "run", "--bin", "sail-flight", "--", "server", "--port", str(port), "--host", host],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=workspace_root,
        env={**os.environ, "RUST_LOG": "info"},
    )

    # Wait for server to be ready
    max_wait = 30  # seconds (cargo build can be slow)
    start_time = time.time()
    ready = False

    while time.time() - start_time < max_wait:
        if process.poll() is not None:
            # Process died
            stdout, stderr = process.communicate()
            msg = f"Server process died. stdout: {stdout}, stderr: {stderr}"
            raise RuntimeError(msg)

        time.sleep(0.5)

        # Try to connect to check if server is ready
        try:
            conn = dbapi.connect(uri)
            conn.close()
            ready = True
            break
        except OSError:
            # Server not ready yet, continue waiting
            continue

    if not ready:
        process.kill()
        stdout, stderr = process.communicate()
        msg = f"Server did not become ready in {max_wait}s. stdout: {stdout}, stderr: {stderr}"
        raise RuntimeError(msg)

    try:
        yield uri
    finally:
        # Stop the server
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


@pytest.fixture(scope="module")
def flight_uri():
    """Fixture that provides the URI of a running Flight SQL server.

    Yields:
        str: The URI of the Flight SQL server.
    """
    # Allow overriding with environment variable for CI/CD
    if "SAIL_FLIGHT_URI" in os.environ:
        yield os.environ["SAIL_FLIGHT_URI"]
    else:
        with flight_server() as uri:
            yield uri


@pytest.fixture
def flight_connection(flight_uri):
    """Fixture that provides a connected ADBC Flight SQL connection.

    Yields:
        Connection: An ADBC connection to the Flight SQL server.
    """
    conn = dbapi.connect(flight_uri)
    yield conn
    conn.close()


def test_flight_server_connectivity(flight_connection):
    """Test that we can connect to the Flight SQL server."""
    # Simple connectivity test - if we got here, connection succeeded
    assert flight_connection is not None


def test_flight_execute_simple_query(flight_connection):
    """Test executing a simple SQL query via Flight SQL."""
    cur = flight_connection.cursor()
    cur.execute("SELECT * FROM (VALUES (1), (2)) AS t(one)")
    rows = cur.fetchall()

    expected_row_count = 2
    assert len(rows) == expected_row_count
    assert rows[0] == (1,)
    assert rows[1] == (2,)
    cur.close()
    cur.close()


def test_flight_multiple_columns(flight_connection):
    """Test query with multiple columns."""
    cur = flight_connection.cursor()
    cur.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
    rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0] == (1, 2, 3)
    cur.close()


def test_flight_error_handling(flight_connection):
    """Test that errors are properly handled and reported."""
    cur = flight_connection.cursor()

    # Try an invalid query - nonexistent table should raise an error
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="nonexistent_table"):
        cur.execute("SELECT * FROM nonexistent_table_12345")

    cur.close()


def test_flight_not_implemented_function(flight_connection):
    """Test that unimplemented SQL functions return proper error."""
    cur = flight_connection.cursor()

    # json_tuple is not yet implemented
    with pytest.raises((adbc_driver_manager.ProgrammingError, adbc_driver_manager.OperationalError)) as exc_info:
        cur.execute("SELECT json_tuple('{\"a\":1,\"b\":2}', 'a', 'b')")

    # Verify the error message indicates the function is not implemented
    error_message = str(exc_info.value).lower()
    assert "not" in error_message, f"Expected 'not' in error, got: {exc_info.value}"
    assert "implemented" in error_message or "found" in error_message, (
        f"Expected 'implemented' or 'found' in error, got: {exc_info.value}"
    )

    cur.close()


@pytest.mark.parametrize(
    ("query", "expected_cols"),
    [
        ("SELECT 1 AS a", 1),
        ("SELECT 1 AS a, 2 AS b", 2),
        ("SELECT 1 AS a, 2 AS b, 3 AS c", 3),
    ],
)
def test_flight_column_count(flight_connection, query, expected_cols):
    """Test queries with different numbers of columns."""
    cur = flight_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # Check we got at least one row
    assert len(rows) > 0

    # Check number of columns
    assert len(rows[0]) == expected_cols
    cur.close()


@pytest.mark.parametrize(
    ("query", "expected_result"),
    [
        ("SELECT concat_ws('-', 'hello', 'world') AS result", "hello-world"),
        ("SELECT concat_ws(',', 'a', 'b', 'c') AS result", "a,b,c"),
        ("SELECT concat_ws('|', 'foo', 'bar') AS result", "foo|bar"),
        ("SELECT concat_ws('', 'foo', 'bar') AS result", "foobar"),
    ],
)
def test_flight_concat_ws(flight_connection, query, expected_result):
    """Test concat_ws string function via Flight SQL."""
    cur = flight_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    assert len(rows) == 1
    assert len(rows[0]) == 1
    assert rows[0][0] == expected_result
    cur.close()


def test_flight_string_operations(flight_connection):
    """Test various string operations via Flight SQL."""
    test_cases = [
        ("SELECT upper('hello') AS result", "HELLO"),
        ("SELECT lower('WORLD') AS result", "world"),
        ("SELECT concat('hello', ' ', 'world') AS result", "hello world"),
    ]

    for query, expected in test_cases:
        cur = flight_connection.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == expected
        cur.close()


def test_flight_numeric_operations(flight_connection):
    """Test numeric operations via Flight SQL."""
    test_cases = [
        ("SELECT 1 + 2 AS result", 3),
        ("SELECT 10 - 3 AS result", 7),
        ("SELECT 4 * 5 AS result", 20),
        ("SELECT 20 / 4 AS result", 5),
    ]

    for query, expected in test_cases:
        cur = flight_connection.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == expected
        cur.close()


@pytest.mark.skip(reason="CREATE TEMP VIEW requires catalog manager which is not yet available in Flight SQL mode")
def test_flight_create_temp_view(flight_connection):
    """Test creating a temporary view and querying it."""
    cur = flight_connection.cursor()

    # Create a temp view
    cur.execute("CREATE OR REPLACE TEMP VIEW test_view AS SELECT 1 AS id, 'test' AS name")

    # Query the view
    cur.execute("SELECT * FROM test_view")
    rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0] == (1, "test")

    cur.close()


def test_flight_cursor_reuse(flight_connection):
    """Test that we can reuse a cursor for multiple queries."""
    cur = flight_connection.cursor()

    # Expected values
    expected_first = 1
    expected_second = 2
    expected_third = 3

    # First query
    cur.execute("SELECT 1 AS first")
    rows1 = cur.fetchall()
    assert rows1[0][0] == expected_first

    # Second query with same cursor
    cur.execute("SELECT 2 AS second")
    rows2 = cur.fetchall()
    assert rows2[0][0] == expected_second

    # Third query
    cur.execute("SELECT 3 AS third")
    rows3 = cur.fetchall()
    assert rows3[0][0] == expected_third

    cur.close()


def test_flight_multiple_cursors(flight_connection):
    """Test that we can have multiple cursors on the same connection."""
    cur1 = flight_connection.cursor()
    cur2 = flight_connection.cursor()

    expected_val1 = 1
    expected_val2 = 2

    # Execute different queries on different cursors
    cur1.execute("SELECT 1 AS result")
    cur2.execute("SELECT 2 AS result")

    rows1 = cur1.fetchall()
    rows2 = cur2.fetchall()

    assert rows1[0][0] == expected_val1
    assert rows2[0][0] == expected_val2

    cur1.close()
    cur2.close()
