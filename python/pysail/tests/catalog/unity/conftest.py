"""Fixtures for native Unity Catalog integration tests."""

from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING

import pytest

from pysail.testing.containers.unity_defaults import DEFAULT_CATALOG

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope="module")
def unity_catalog(
    unity_rest_url: str,
    unity_catalog_initialized: None,
) -> object:
    from pysail import _native

    del unity_catalog_initialized
    return _native._catalog._unity.UnityCatalogProvider(  # noqa: SLF001
        "native-unity-tests",
        unity_rest_url,
        DEFAULT_CATALOG,
    )


@pytest.fixture
def unity_database(unity_catalog: object) -> Generator[list[str], None, None]:
    from pysail import _native

    database = [f"native_unity_{uuid.uuid4().hex}"]
    unity_catalog.create_database(database)
    try:
        yield database
    finally:
        with contextlib.suppress(_native._catalog.DatabaseNotFoundError, RuntimeError):  # noqa: SLF001
            for table in unity_catalog.list_tables(database):
                unity_catalog.drop_table(database, table.name, if_exists=True)
            unity_catalog.drop_database(database, if_exists=True)
