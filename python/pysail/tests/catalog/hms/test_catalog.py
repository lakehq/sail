"""Native HMS catalog provider integration tests."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from pysail import _native

if TYPE_CHECKING:
    from pysail.testing.containers.hms import HmsService


HmsCatalogProvider = _native._catalog._hms.HmsCatalogProvider  # noqa: SLF001
TableNotFoundError = _native._catalog.TableNotFoundError  # noqa: SLF001
ViewNotFoundError = _native._catalog.ViewNotFoundError  # noqa: SLF001

_FORMATS = ["parquet", "csv", "delta", "textfile", "json", "orc", "avro"]
_ID_COLUMN = [("id", "int64", True, None)]
_ITEM_COLUMNS = [("id", "int64", True, None), ("value", "utf8", True, None)]
_HMS_CONTAINER_TMP = "/tmp"  # noqa: S108


def _table_location(database: list[str], table: str) -> str:
    return f"{_HMS_CONTAINER_TMP}/{database[0]}_{table}"


def test_create_get_list_drop_database(hms_catalog: HmsCatalogProvider) -> None:
    database = [f"native_hms_database_{uuid.uuid4().hex}"]
    try:
        created = hms_catalog.create_database(database)
        assert created.catalog == "native-hms-tests"
        assert created.database == database

        fetched = hms_catalog.get_database(database)
        assert fetched.database == database
        assert database in [status.database for status in hms_catalog.list_databases()]

        hms_catalog.drop_database(database)
        assert database not in [status.database for status in hms_catalog.list_databases()]
    finally:
        hms_catalog.drop_database(database, if_exists=True, cascade=True)


def test_create_get_list_drop_table(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    created = hms_catalog.create_table(
        hms_database,
        "items",
        _ID_COLUMN,
        location=_table_location(hms_database, "items"),
    )
    assert created.name == "items"
    assert created.kind == "table"
    assert created.format == "parquet"
    assert created.is_external is True
    assert [(column.name, column.data_type, column.nullable) for column in created.columns] == [("id", "Int64", True)]

    fetched = hms_catalog.get_table(hms_database, "items")
    assert fetched.name == "items"
    assert [status.name for status in hms_catalog.list_tables(hms_database)] == ["items"]

    hms_catalog.drop_table(hms_database, "items")
    assert hms_catalog.list_tables(hms_database) == []


@pytest.mark.parametrize("table_format", _FORMATS)
def test_supported_formats_round_trip(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
    table_format: str,
) -> None:
    table = f"{table_format}_items"
    created = hms_catalog.create_table(
        hms_database,
        table,
        _ITEM_COLUMNS,
        format=table_format,
        location=_table_location(hms_database, table),
    )
    assert created.kind == "table"
    assert created.format == table_format
    assert [(column.name, column.data_type) for column in created.columns] == [
        ("id", "Int64"),
        ("value", "Utf8"),
    ]

    fetched = hms_catalog.get_table(hms_database, table)
    assert fetched.kind == "table"
    assert fetched.format == table_format


def test_get_table_returns_table_not_found_for_view(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    hms_catalog.create_view(hms_database, "v_items", _ID_COLUMN, "SELECT 1 AS id")

    with pytest.raises(TableNotFoundError):
        hms_catalog.get_table(hms_database, "v_items")


def test_list_tables_excludes_views(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    hms_catalog.create_table(
        hms_database,
        "items",
        _ID_COLUMN,
        location=_table_location(hms_database, "items"),
    )
    hms_catalog.create_view(hms_database, "v_items", _ID_COLUMN, "SELECT id FROM items")

    assert {status.name for status in hms_catalog.list_tables(hms_database)} == {"items"}


def test_create_get_list_drop_view(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    definition = "SELECT 1 AS id"
    created = hms_catalog.create_view(hms_database, "v_items", _ID_COLUMN, definition)
    assert created.name == "v_items"
    assert created.kind == "view"
    assert created.format is None
    assert created.view_definition == definition
    assert [(column.name, column.data_type) for column in created.columns] == [("id", "Int64")]

    fetched = hms_catalog.get_view(hms_database, "v_items")
    assert fetched.name == "v_items"
    assert [status.name for status in hms_catalog.list_views(hms_database)] == ["v_items"]

    hms_catalog.drop_view(hms_database, "v_items")
    assert hms_catalog.list_views(hms_database) == []


def test_get_view_returns_view_not_found_for_table(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    hms_catalog.create_table(
        hms_database,
        "items",
        _ID_COLUMN,
        location=_table_location(hms_database, "items"),
    )

    with pytest.raises(ViewNotFoundError):
        hms_catalog.get_view(hms_database, "items")


def test_list_views_excludes_tables(
    hms_catalog: HmsCatalogProvider,
    hms_database: list[str],
) -> None:
    hms_catalog.create_table(
        hms_database,
        "items",
        _ID_COLUMN,
        location=_table_location(hms_database, "items"),
    )
    hms_catalog.create_view(hms_database, "v_items", _ID_COLUMN, "SELECT id FROM items")

    assert {status.name for status in hms_catalog.list_views(hms_database)} == {"v_items"}


def test_failover_from_dead_primary_endpoint(hms_service: HmsService) -> None:
    catalog = HmsCatalogProvider(
        "native-hms-failover",
        ["127.0.0.1:1", hms_service.endpoint],
        connect_timeout_secs=1,
    )

    assert catalog.list_databases()
