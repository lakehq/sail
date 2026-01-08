from __future__ import annotations

import platform
from pathlib import Path

import pytest
from pytest_bdd import given, parsers

from pysail.tests.spark.utils import escape_sql_string_literal
from pysail.tests.spark.steps.sql import PathWrapper


def _ensure_duckdb_ducklake_available() -> None:
    try:
        import duckdb  # noqa: F401
    except ModuleNotFoundError as e:  # pragma: no cover
        pytest.skip(f"duckdb is required for DuckLake BDD setup: {e}")

    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        try:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
        except (RuntimeError, duckdb.Error) as e:
            pytest.skip(f"DuckLake extension not available: {e}")
    finally:
        conn.close()


@given(
    parsers.parse("ducklake test table is created in {metadata_var} and {data_var}"),
    target_fixture="variables",
)
def ducklake_test_table_created(metadata_var: str, data_var: str, tmp_path: Path, variables: dict) -> dict:
    """
    Create a minimal DuckLake table via DuckDB (DuckLake extension) so Spark can read it.

    This mirrors the setup in `python/pysail/tests/spark/ducklake/test_ducklake_read.py`, but in BDD form.
    """
    if platform.system() == "Windows":
        pytest.skip("DuckLake BDD setup may not work on Windows")

    _ensure_duckdb_ducklake_available()
    import duckdb

    meta = variables.get(metadata_var)
    data = variables.get(data_var)
    assert isinstance(meta, PathWrapper), f"Variable {metadata_var!r} not found or not a PathWrapper"
    assert isinstance(data, PathWrapper), f"Variable {data_var!r} not found or not a PathWrapper"

    metadata_path = Path(meta.path)
    data_path = Path(data.path)
    data_path.mkdir(parents=True, exist_ok=True)

    # Create DuckLake metadata+data with DuckDB.
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
        conn.execute(
            f"""
            ATTACH 'ducklake:sqlite:{metadata_path}' AS my_ducklake
            (DATA_PATH '{data_path}/')
            """
        )
        conn.execute(
            """
            CREATE TABLE my_ducklake.test_table (
                id INTEGER,
                name VARCHAR,
                score DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO my_ducklake.test_table
            VALUES
                (1, 'Alice', 95.5),
                (2, 'Bob', 87.3),
                (3, 'Charlie', 92.1)
            """
        )
        conn.execute("DETACH my_ducklake")
    finally:
        conn.close()

    # Provide pre-escaped strings for SQL templates.
    variables["ducklake_url"] = escape_sql_string_literal(f"sqlite:///{metadata_path}")
    variables["ducklake_table"] = "test_table"
    variables["ducklake_base_path"] = escape_sql_string_literal(f"file://{data_path}/")
    return variables

