# ruff: noqa: RUF002, TC003
"""Minimal HMS smoke test – Sail can connect to HMS and list databases."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.hms_interop


def test_hms_list_default_database(hms_spark):
    """Sail connected to HMS should see at least the ``default`` database.

    This is the smallest meaningful smoke assertion: verify that the Sail
    server can talk to the HMS container and that ``SHOW DATABASES``
    returns the built-in ``default`` database that every Hive Metastore
    creates on first startup.
    """
    databases = [row.name for row in hms_spark.sql("SHOW DATABASES").collect()]
    assert "default" in databases, f"HMS 'default' database not found; got: {databases}"


def test_hms_create_database_with_relative_location(
    hms_spark,
    reference_spark,
    hms_warehouse_dir: Path,
):
    """Sail resolves relative database LOCATION against the default warehouse path."""
    database = "hms_relative_location_db"
    relative_location = "relative/databases/hms_relative_location_db"
    expected_location = (f"{hms_warehouse_dir.as_uri().rstrip('/')}/{relative_location}").replace(
        "file:///", "file:/", 1
    )

    reference_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    try:
        hms_spark.sql(f"CREATE DATABASE {database} LOCATION '{relative_location}'")

        rows = reference_spark.sql(f"DESCRIBE DATABASE EXTENDED {database}").collect()
        props = {row.info_name: row.info_value for row in rows}
        assert props.get("Location") == expected_location
    finally:
        reference_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
