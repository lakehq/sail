"""Fixtures for native AWS Glue catalog integration tests."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope="module")
def glue_catalog(moto_endpoint: str) -> object:
    from pysail import _native

    catalog = _native._catalog._glue.GlueCatalogProvider(  # noqa: SLF001
        "native-glue-tests",
        catalog_id="123456789012",
        region="us-east-1",
        endpoint_url=moto_endpoint,
        access_key_id="testing",
        secret_access_key="testing",  # noqa: S106
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
    message = f"native Glue catalog did not become queryable: {last_error}"
    raise TimeoutError(message)


@pytest.fixture
def glue_database(glue_catalog: object) -> Generator[list[str], None, None]:
    database = [f"native_glue_{uuid.uuid4().hex}"]
    glue_catalog.create_database(database)
    try:
        yield database
    finally:
        for status in glue_catalog.list_views(database):
            glue_catalog.drop_view(database, status.name, if_exists=True)
        for status in glue_catalog.list_tables(database):
            glue_catalog.drop_table(database, status.name, if_exists=True)
        glue_catalog.drop_database(database, if_exists=True)
