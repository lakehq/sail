"""Fixtures for native Iceberg REST catalog integration tests."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pysail.testing.containers.iceberg_rest import IcebergRestService


@pytest.fixture(scope="session")
def iceberg_rest_catalog(iceberg_rest_service: IcebergRestService) -> object:
    from pysail import _native

    catalog = _native._catalog._iceberg.IcebergRestCatalogProvider(  # noqa: SLF001
        "native-iceberg-rest-tests",
        iceberg_rest_service.endpoint,
    )
    deadline = time.monotonic() + 60
    last_error = None
    while time.monotonic() < deadline:
        try:
            catalog.list_databases()
        except RuntimeError as error:
            last_error = error
            time.sleep(1)
        else:
            return catalog
    message = f"native Iceberg REST catalog did not become queryable: {last_error}"
    raise TimeoutError(message)
