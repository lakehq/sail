import tempfile
from typing import TYPE_CHECKING

import pytest
from pyiceberg.catalog.sql import SqlCatalog

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


@pytest.fixture
def sql_catalog() -> "Catalog":
    """Create a SQL catalog for Iceberg tests using a temporary directory."""

    with tempfile.TemporaryDirectory() as tmpdir:
        catalog = SqlCatalog(
            "test_catalog",
            uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            warehouse=f"file://{tmpdir}/warehouse",
        )
        # Create default namespace
        catalog.create_namespace("default")
        yield catalog
