"""Fixtures for native Hive Metastore catalog integration tests."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.hms import HmsService


@pytest.fixture(scope="session")
def hms_catalog(hms_service: HmsService) -> object:
    from pysail import _native

    catalog = _native._catalog._hms.HmsCatalogProvider(  # noqa: SLF001
        "native-hms-tests",
        [hms_service.endpoint],
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
    message = f"native HMS catalog did not become queryable: {last_error}"
    raise TimeoutError(message)


@pytest.fixture
def hms_database(hms_catalog: object) -> Generator[list[str], None, None]:
    database = [f"native_hms_{uuid.uuid4().hex}"]
    hms_catalog.create_database(database)
    try:
        yield database
    finally:
        hms_catalog.drop_database(database, if_exists=True, cascade=True)
