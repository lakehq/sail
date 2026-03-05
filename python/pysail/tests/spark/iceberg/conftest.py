import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from pysail.tests.spark.iceberg.utils import pyiceberg_local_location

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog


@pytest.fixture
def sql_catalog() -> "Catalog":
    """Create a SQL catalog for Iceberg tests using a temporary directory."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        catalog = SqlCatalog(
            "test_catalog",
            uri=f"sqlite:///{tmp_path.as_posix()}/pyiceberg_catalog.db",
            warehouse=pyiceberg_local_location(tmp_path.joinpath("warehouse")),
        )
        # Create default namespace
        catalog.create_namespace("default")
        yield catalog

        # Release SQLite file handle before TemporaryDirectory cleanup on Windows.
        if hasattr(catalog, "engine"):
            catalog.engine.dispose()
