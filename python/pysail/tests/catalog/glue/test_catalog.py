"""Native AWS Glue catalog provider integration tests."""

from __future__ import annotations

import contextlib
import uuid

import pytest

from pysail import _native

GlueCatalogProvider = _native._catalog._glue.GlueCatalogProvider  # noqa: SLF001
DatabaseNotFoundError = _native._catalog.DatabaseNotFoundError  # noqa: SLF001
TableNotFoundError = _native._catalog.TableNotFoundError  # noqa: SLF001
ViewNotFoundError = _native._catalog.ViewNotFoundError  # noqa: SLF001

_ID_COLUMN = [("id", "int32", True, None)]
_PRODUCT_COLUMNS = [
    ("id", "int64", False, "Primary key"),
    ("name", "utf8", True, None),
    ("price", "float64", True, None),
    ("category", "utf8", False, None),
]
_VIEW_COLUMNS = [
    ("id", "int64", True, "The ID"),
    ("value", "utf8", True, None),
]
_PRODUCT_VIEW_COLUMNS = [
    ("id", "int64", True, "Primary key"),
    ("name", "utf8", True, None),
    ("price", "float64", True, None),
]
_COLUMN_TYPES = [
    pytest.param("boolean", "Boolean", id="boolean"),
    pytest.param("int8", "Int8", id="tinyint"),
    pytest.param("int16", "Int16", id="smallint"),
    pytest.param("int32", "Int32", id="int"),
    pytest.param("int64", "Int64", id="bigint"),
    pytest.param("float32", "Float32", id="float"),
    pytest.param("float64", "Float64", id="double"),
    pytest.param("utf8", "Utf8", id="string"),
    pytest.param("binary", "Binary", id="binary"),
    pytest.param("date32", "Date32", id="date"),
    pytest.param("timestamp", "Timestamp(µs)", id="timestamp"),
    pytest.param("Decimal128(10, 2)", "Decimal128(10, 2)", id="decimal"),
    pytest.param("List(Utf8)", "List", id="array"),
    pytest.param('Struct("name": Utf8, "value": Int32)', "Struct", id="struct"),
    pytest.param(
        'Map("entries": non-null Struct("key": non-null Utf8, "value": Int32), unsorted)',
        "Map",
        id="map",
    ),
]
_FORMATS = ["parquet", "csv", "json", "orc", "avro"]
_COMPLEX_TYPE_NAMES = {"List", "Struct", "Map"}


def _database(prefix: str) -> list[str]:
    return [f"{prefix}_{uuid.uuid4().hex}"]


def _properties(status: object) -> dict[str, str]:
    return dict(status.properties)


def _columns(status: object) -> dict[str, object]:
    return {column.name: column for column in status.columns}


def _location(database: list[str], table: str) -> str:
    return f"s3://bucket/{database[0]}/{table}"


def _cleanup_database(catalog: GlueCatalogProvider, database: list[str]) -> None:
    with contextlib.suppress(DatabaseNotFoundError, RuntimeError):
        for status in catalog.list_views(database):
            catalog.drop_view(database, status.name, if_exists=True)
        for status in catalog.list_tables(database):
            catalog.drop_table(database, status.name, if_exists=True)
        catalog.drop_database(database, if_exists=True)


def test_create_database(glue_catalog: GlueCatalogProvider) -> None:
    database = _database("create_database")
    second_database = _database("create_database_if_not_exists")
    try:
        created = glue_catalog.create_database(
            database,
            comment="test comment",
            location="s3://bucket/path",
            properties=[("key1", "value1")],
        )
        assert created.catalog == "native-glue-tests"
        assert created.database == database
        assert created.comment == "test comment"
        assert created.location == "s3://bucket/path"
        assert _properties(created)["key1"] == "value1"

        with pytest.raises(RuntimeError):
            glue_catalog.create_database(database)

        created_second = glue_catalog.create_database(second_database, if_not_exists=True)
        assert created_second.database == second_database

        created_again = glue_catalog.create_database(
            second_database,
            if_not_exists=True,
            comment="should be ignored",
            location="should be ignored",
            properties=[("ignored", "ignored")],
        )
        assert created_again.database == second_database
        assert created_again.comment is None
        assert created_again.location is None
        assert created_again.properties == []
    finally:
        _cleanup_database(glue_catalog, database)
        _cleanup_database(glue_catalog, second_database)


def test_get_database(glue_catalog: GlueCatalogProvider) -> None:
    database = _database("get_database")
    properties = [("owner", "test_user"), ("team", "data_eng")]
    try:
        with pytest.raises(DatabaseNotFoundError):
            glue_catalog.get_database(database)

        created = glue_catalog.create_database(
            database,
            comment="Get test description",
            location="s3://bucket/get-test",
            properties=properties,
        )
        assert created.database == database

        fetched = glue_catalog.get_database(database)
        assert fetched.database == database
        assert fetched.comment == "Get test description"
        assert fetched.location == "s3://bucket/get-test"
        assert set(properties).issubset(set(fetched.properties))
    finally:
        _cleanup_database(glue_catalog, database)


def test_drop_database(glue_catalog: GlueCatalogProvider) -> None:
    database = _database("drop_database")
    try:
        with pytest.raises(DatabaseNotFoundError):
            glue_catalog.drop_database(database)
        glue_catalog.drop_database(database, if_exists=True)

        glue_catalog.create_database(database, comment="To be dropped")
        assert glue_catalog.get_database(database).database == database

        glue_catalog.drop_database(database)
        with pytest.raises(DatabaseNotFoundError):
            glue_catalog.get_database(database)
    finally:
        _cleanup_database(glue_catalog, database)


def test_list_databases(glue_catalog: GlueCatalogProvider) -> None:
    prefix = f"list_databases_{uuid.uuid4().hex}"
    databases = [[f"{prefix}_{suffix}"] for suffix in ("one", "two", "other")]
    try:
        for database in databases:
            glue_catalog.create_database(database)

        listed = {
            tuple(status.database) for status in glue_catalog.list_databases() if status.database[0].startswith(prefix)
        }
        assert listed == {tuple(database) for database in databases}
    finally:
        for database in databases:
            _cleanup_database(glue_catalog, database)


def test_create_table(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    created = glue_catalog.create_table(
        glue_database,
        "products",
        _PRODUCT_COLUMNS,
        comment="Product catalog table",
        location="s3://bucket/products",
        partition_by=[("category", None)],
        properties=[("owner", "test_user")],
    )
    columns = _columns(created)
    assert created.name == "products"
    assert created.catalog == "native-glue-tests"
    assert created.database == glue_database
    assert created.kind == "table"
    assert created.comment == "Product catalog table"
    assert created.location == "s3://bucket/products"
    assert created.format == "parquet"
    assert created.partition_by == [("category", None)]
    assert _properties(created)["owner"] == "test_user"
    assert set(columns) == {"id", "name", "price", "category"}
    assert columns["id"].comment == "Primary key"
    assert columns["category"].is_partition is True

    with pytest.raises(RuntimeError):
        glue_catalog.create_table(
            glue_database,
            "products",
            _ID_COLUMN,
            location="s3://bucket/duplicate",
        )

    existing = glue_catalog.create_table(
        glue_database,
        "products",
        [("different", "int32", True, None)],
        comment="Different comment",
        location="s3://bucket/different",
        if_not_exists=True,
    )
    assert existing.name == "products"
    assert existing.comment == "Product catalog table"
    assert {column.name for column in existing.columns} == {"id", "name", "price", "category"}


def test_get_table(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    with pytest.raises(TableNotFoundError):
        glue_catalog.get_table(glue_database, "nonexistent")

    glue_catalog.create_table(
        glue_database,
        "test_table",
        [("id", "int64", False, "The ID"), ("value", "utf8", True, None)],
        comment="Test table description",
        location="s3://bucket/test_table",
        properties=[("key1", "value1")],
    )

    table = glue_catalog.get_table(glue_database, "test_table")
    columns = _columns(table)
    assert table.name == "test_table"
    assert table.database == glue_database
    assert table.comment == "Test table description"
    assert table.location == "s3://bucket/test_table"
    assert table.format == "parquet"
    assert _properties(table)["key1"] == "value1"
    assert columns["id"].data_type == "Int64"
    assert columns["id"].comment == "The ID"
    assert columns["value"].data_type == "Utf8"


@pytest.mark.parametrize(("data_type", "expected_type"), _COLUMN_TYPES)
def test_column_types(
    glue_catalog: GlueCatalogProvider,
    glue_database: list[str],
    data_type: str,
    expected_type: str,
) -> None:
    table = glue_catalog.create_table(
        glue_database,
        "all_types",
        [("value", data_type, True, None)],
        location=_location(glue_database, "all_types"),
    )
    actual_type = table.columns[0].data_type
    if expected_type in _COMPLEX_TYPE_NAMES:
        assert actual_type.startswith(f"{expected_type}(")
    else:
        assert actual_type == expected_type


def test_unsupported_column_types(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    union_type = 'Union(Sparse, 0: ("int_field": Int32), 1: ("str_field": Utf8))'
    with pytest.raises(RuntimeError, match="Union types are not supported by Glue"):
        glue_catalog.create_table(
            glue_database,
            "unsupported_types",
            [("col_union", union_type, True, None)],
            location=_location(glue_database, "unsupported_types"),
        )


@pytest.mark.parametrize("table_format", _FORMATS)
def test_storage_formats(
    glue_catalog: GlueCatalogProvider,
    glue_database: list[str],
    table_format: str,
) -> None:
    table_name = f"test_{table_format}_table"
    created = glue_catalog.create_table(
        glue_database,
        table_name,
        [("id", "int32", False, None), ("name", "utf8", True, None)],
        format=table_format,
        comment=f"Table with {table_format} format",
        location=_location(glue_database, table_name),
    )
    assert created.name == table_name
    assert glue_catalog.get_table(glue_database, table_name).format == table_format


def test_list_tables(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    table_names = {"table_alpha", "table_beta", "table_gamma"}
    for table_name in table_names:
        glue_catalog.create_table(
            glue_database,
            table_name,
            _ID_COLUMN,
            location=_location(glue_database, table_name),
        )

    assert {status.name for status in glue_catalog.list_tables(glue_database)} == table_names


def test_drop_table(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    glue_catalog.create_table(
        glue_database,
        "drop_me",
        _ID_COLUMN,
        location=_location(glue_database, "drop_me"),
    )
    assert glue_catalog.get_table(glue_database, "drop_me").name == "drop_me"

    glue_catalog.drop_table(glue_database, "drop_me")
    with pytest.raises(TableNotFoundError):
        glue_catalog.get_table(glue_database, "drop_me")
    with pytest.raises(TableNotFoundError):
        glue_catalog.drop_table(glue_database, "nonexistent")
    glue_catalog.drop_table(glue_database, "nonexistent", if_exists=True)


def test_hive_rejects_transforms(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    with pytest.raises(RuntimeError, match="Partition transforms are only supported for Iceberg tables"):
        glue_catalog.create_table(
            glue_database,
            "hive_with_transforms",
            [("id", "int64", True, None), ("event_time", "timestamp", True, None)],
            location=_location(glue_database, "hive_with_transforms"),
            partition_by=[("event_time", "day")],
        )


def test_iceberg_requires_location(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    with pytest.raises(ValueError, match=r"(?i)location"):
        glue_catalog.create_table(
            glue_database,
            "iceberg_no_location",
            _ID_COLUMN,
            format="iceberg",
        )


def test_create_view(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    created = glue_catalog.create_view(
        glue_database,
        "product_view",
        _PRODUCT_VIEW_COLUMNS,
        "SELECT id, name, price FROM products",
        comment="View of products",
        properties=[("owner", "test_user")],
    )
    assert created.name == "product_view"
    assert created.database == glue_database
    assert created.kind == "view"
    assert created.view_definition == "SELECT id, name, price FROM products"
    assert len(created.columns) == len(_PRODUCT_VIEW_COLUMNS)
    assert created.comment == "View of products"
    assert _properties(created)["owner"] == "test_user"

    with pytest.raises(RuntimeError):
        glue_catalog.create_view(
            glue_database,
            "product_view",
            _ID_COLUMN,
            "SELECT 1",
        )

    existing = glue_catalog.create_view(
        glue_database,
        "product_view",
        [("different", "int32", True, None)],
        "SELECT 2",
        if_not_exists=True,
        comment="Different comment",
    )
    assert existing.name == "product_view"
    assert existing.comment == "View of products"
    assert existing.view_definition == "SELECT id, name, price FROM products"


def test_get_view(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    with pytest.raises(ViewNotFoundError):
        glue_catalog.get_view(glue_database, "nonexistent")

    glue_catalog.create_view(
        glue_database,
        "test_view",
        _VIEW_COLUMNS,
        "SELECT id, value FROM source_table",
        comment="Test view description",
        properties=[("key1", "value1")],
    )

    view = glue_catalog.get_view(glue_database, "test_view")
    columns = _columns(view)
    assert view.name == "test_view"
    assert view.database == glue_database
    assert view.view_definition == "SELECT id, value FROM source_table"
    assert view.comment == "Test view description"
    assert _properties(view)["key1"] == "value1"
    assert columns["id"].data_type == "Int64"
    assert columns["id"].comment == "The ID"
    assert columns["value"].data_type == "Utf8"


def test_get_view_not_found_for_table(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    glue_catalog.create_table(
        glue_database,
        "actual_table",
        _ID_COLUMN,
        location=_location(glue_database, "actual_table"),
    )
    with pytest.raises(ViewNotFoundError):
        glue_catalog.get_view(glue_database, "actual_table")


def test_list_views(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    assert glue_catalog.list_views(glue_database) == []
    view_names = {"view_alpha", "view_beta", "view_gamma"}
    for view_name in view_names:
        glue_catalog.create_view(
            glue_database,
            view_name,
            _ID_COLUMN,
            "SELECT * FROM source_table",
        )
    glue_catalog.create_table(
        glue_database,
        "a_table",
        _ID_COLUMN,
        location=_location(glue_database, "a_table"),
    )

    assert {status.name for status in glue_catalog.list_views(glue_database)} == view_names


def test_drop_view(glue_catalog: GlueCatalogProvider, glue_database: list[str]) -> None:
    glue_catalog.create_view(
        glue_database,
        "drop_me",
        _ID_COLUMN,
        "SELECT 1 AS id",
    )
    assert glue_catalog.get_view(glue_database, "drop_me").name == "drop_me"

    glue_catalog.drop_view(glue_database, "drop_me")
    with pytest.raises(ViewNotFoundError):
        glue_catalog.get_view(glue_database, "drop_me")
    with pytest.raises(ViewNotFoundError):
        glue_catalog.drop_view(glue_database, "nonexistent")
    glue_catalog.drop_view(glue_database, "nonexistent", if_exists=True)
