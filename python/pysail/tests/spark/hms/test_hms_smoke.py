# ruff: noqa: RUF002
"""Minimal HMS smoke tests – Sail can connect to HMS and list databases."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.catalog_integration


def test_hms_list_default_database(hms_s3_spark):
    """Sail connected to HMS should see at least the ``default`` database.

    This is the smallest meaningful smoke assertion: verify that the Sail
    server can talk to the HMS container and that ``SHOW DATABASES``
    returns the built-in ``default`` database that every Hive Metastore
    creates on first startup.
    """
    databases = [row.name for row in hms_s3_spark.sql("SHOW DATABASES").collect()]
    assert "default" in databases, f"HMS 'default' database not found; got: {databases}"
