"""Native Iceberg REST catalog provider integration tests."""

from __future__ import annotations

import uuid

import pytest

from pysail import _native

IcebergRestCatalogProvider = _native._catalog._iceberg.IcebergRestCatalogProvider  # noqa: SLF001
DatabaseNotFoundError = _native._catalog.DatabaseNotFoundError  # noqa: SLF001
TableNotFoundError = _native._catalog.TableNotFoundError  # noqa: SLF001
ViewNotFoundError = _native._catalog.ViewNotFoundError  # noqa: SLF001

_TABLE_COLUMNS = [
    ("foo", "utf8", True, None),
    ("bar", "int32", False, "meow"),
    ("baz", "boolean", True, None),
]
_ID_COLUMN = [("id", "int32", False, None)]
_VIEW_COLUMNS = [
    ("col1", "utf8", True, None),
    ("col2", "int32", False, "important column"),
]
_PARTITION_COLUMNS = [
    ("id", "int64", False, None),
    ("ts", "timestamp", True, None),
    ("name", "utf8", True, None),
]
_DYNAMIC_TABLE_PROPERTIES = {
    "metadata-location",
    "metadata.last-updated-ms",
    "metadata.table-uuid",
}
_BASIC_TABLE_PROPERTY_COUNT = 15
_DETAILED_TABLE_PROPERTY_COUNT = 18
_VIEW_PROPERTY_COUNT = 6


def _namespace(prefix: str, *tail: str) -> list[str]:
    return [f"{prefix}_{uuid.uuid4().hex}", *tail]


def _properties(status: object) -> dict[str, str]:
    return dict(status.properties)


def _column_statuses(status: object) -> set[tuple[str, str, bool, str | None, bool]]:
    return {
        (column.name, column.data_type, column.nullable, column.comment, column.is_partition)
        for column in status.columns
    }


def _create_database(catalog: IcebergRestCatalogProvider, namespace: list[str]) -> None:
    catalog.create_database(namespace)


def _create_detailed_table(
    catalog: IcebergRestCatalogProvider,
    namespace: list[str],
    table: str,
    location: str,
) -> object:
    return catalog.create_table(
        namespace,
        table,
        _TABLE_COLUMNS,
        format="iceberg",
        comment="test table",
        location=location,
        constraints=[("primary_key", "pk_bar", ["bar"])],
        partition_by=[("baz", None)],
        sort_by=[("bar", False), ("foo", True)],
        properties=[
            ("option.key1", "value1"),
            ("owner", "mr. meow"),
            ("team", "data-eng"),
        ],
    )


def _assert_detailed_table(status: object, namespace: list[str], table: str, location: str) -> None:
    properties = _properties(status)
    static_properties = {key: value for key, value in properties.items() if key not in _DYNAMIC_TABLE_PROPERTIES}

    assert len(properties) == _DETAILED_TABLE_PROPERTY_COUNT
    assert static_properties == {
        "comment": "test table",
        "metadata.current-schema-id": "0",
        "metadata.current-snapshot-id": "-1",
        "metadata.default-sort-order-id": "1",
        "metadata.default-spec-id": "0",
        "metadata.format-version": "2",
        "metadata.last-column-id": "3",
        "metadata.last-partition-id": "1000",
        "metadata.last-sequence-number": "0",
        "metadata.partition-statistics": "[]",
        "metadata.statistics": "[]",
        "write.parquet.compression-codec": "zstd",
        "option.key1": "value1",
        "owner": "mr. meow",
        "team": "data-eng",
    }
    assert properties["metadata-location"].startswith(f"{location}/metadata/")
    assert properties["metadata.last-updated-ms"]
    assert properties["metadata.table-uuid"]

    assert status.name == table
    assert status.catalog == "native-iceberg-rest-tests"
    assert status.database == namespace
    assert status.kind == "table"
    assert status.comment == "test table"
    assert status.constraints == [("primary_key", None, ["bar"])]
    assert status.location == location
    assert status.format == "iceberg"
    assert status.partition_by == [("baz", None)]
    assert set(status.sort_by) == {("bar", False), ("foo", True)}
    assert status.bucket_by is None
    assert _column_statuses(status) == {
        ("foo", "Utf8", True, None, False),
        ("bar", "Int32", False, "meow", False),
        ("baz", "Boolean", True, None, True),
    }


def test_create_namespace(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("create_namespace")
    created = iceberg_rest_catalog.create_database(
        namespace,
        comment="test comment",
        location="s3://bucket/path",
        properties=[("key1", "value1")],
    )

    assert created.database == namespace
    assert created.comment == "test comment"
    assert created.location == "s3://bucket/path"
    assert ("key1", "value1") in created.properties

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.create_database(namespace)

    second_namespace = _namespace("create_namespace_if_not_exists")
    created_second = iceberg_rest_catalog.create_database(second_namespace, if_not_exists=True)
    assert created_second.database == second_namespace
    assert created_second.comment is None
    assert created_second.location == f"s3://icebergdata/demo/{second_namespace[0]}"

    created_again = iceberg_rest_catalog.create_database(
        second_namespace,
        if_not_exists=True,
        comment="should be ignored",
        location="should be ignored",
        properties=[("ignored", "ignored")],
    )
    assert created_again.database == second_namespace
    assert created_again.comment is None
    assert created_again.location == f"s3://icebergdata/demo/{second_namespace[0]}"


def test_get_non_exist_namespace(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    with pytest.raises(DatabaseNotFoundError):
        iceberg_rest_catalog.get_database(_namespace("missing_namespace"))


def test_get_namespace(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("get_namespace", "ios")
    properties = [("owner", "Lake"), ("community", "Sail")]

    with pytest.raises(DatabaseNotFoundError):
        iceberg_rest_catalog.get_database(namespace)

    created = iceberg_rest_catalog.create_database(namespace, properties=properties)
    assert created.database == namespace
    assert set(properties).issubset(set(created.properties))

    fetched = iceberg_rest_catalog.get_database(namespace)
    assert fetched.database == namespace
    assert set(properties).issubset(set(fetched.properties))


def test_list_namespaces(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    parent = _namespace("list_namespaces")[0]
    first = [parent, "ios"]
    second = [parent, "macos"]

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.list_databases([parent])

    iceberg_rest_catalog.create_database(first, properties=[("owner", "Lake")])
    iceberg_rest_catalog.create_database(second, properties=[("owner", "Meow")])

    databases = iceberg_rest_catalog.list_databases([parent])
    assert {tuple(status.database) for status in databases} == {tuple(first), tuple(second)}


def test_list_empty_namespaces(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("list_empty_namespaces", "apple")

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.list_databases(namespace)

    iceberg_rest_catalog.create_database(namespace, properties=[("owner", "Lake")])
    assert iceberg_rest_catalog.list_databases(namespace) == []


def test_list_root_namespaces(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    root = _namespace("list_root_namespaces")[0]
    first = [root, "apple", "ios"]
    second = [root, "google", "android"]

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.list_databases([root])

    iceberg_rest_catalog.create_database(first, properties=[("owner", "Lake")])
    iceberg_rest_catalog.create_database(second, properties=[("owner", "Meow")])

    matching_roots = [status.database for status in iceberg_rest_catalog.list_databases() if status.database[0] == root]
    assert matching_roots == [[root]]


def test_list_empty_multi_level_namespaces(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("list_empty_multi_level_namespaces", "a_a", "apple")

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.list_databases(namespace)

    iceberg_rest_catalog.create_database(namespace, properties=[("owner", "Lake")])
    assert iceberg_rest_catalog.list_databases(namespace) == []


def test_drop_namespace(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("drop_namespace")
    iceberg_rest_catalog.create_database(namespace)
    assert iceberg_rest_catalog.get_database(namespace).database == namespace

    iceberg_rest_catalog.drop_database(namespace)
    with pytest.raises(DatabaseNotFoundError):
        iceberg_rest_catalog.get_database(namespace)
    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.drop_database(namespace)

    iceberg_rest_catalog.drop_database(namespace, if_exists=True)


def test_create_table(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("create_table", "apple", "ios")
    iceberg_rest_catalog.create_database(namespace, properties=[("owner", "Lake"), ("community", "Sail")])

    table = iceberg_rest_catalog.create_table(
        namespace,
        "t1",
        _TABLE_COLUMNS,
        format="iceberg",
        comment="peow",
    )
    properties = _properties(table)
    static_properties = {key: value for key, value in properties.items() if key not in _DYNAMIC_TABLE_PROPERTIES}
    expected_location = f"s3://icebergdata/demo/{'/'.join(namespace)}/t1"

    assert len(properties) == _BASIC_TABLE_PROPERTY_COUNT
    assert static_properties == {
        "comment": "peow",
        "metadata.current-schema-id": "0",
        "metadata.current-snapshot-id": "-1",
        "metadata.default-sort-order-id": "0",
        "metadata.default-spec-id": "0",
        "metadata.format-version": "2",
        "metadata.last-column-id": "3",
        "metadata.last-partition-id": "999",
        "metadata.last-sequence-number": "0",
        "metadata.partition-statistics": "[]",
        "metadata.statistics": "[]",
        "write.parquet.compression-codec": "zstd",
    }
    assert properties["metadata-location"].startswith(f"{expected_location}/metadata/")
    assert properties["metadata.last-updated-ms"]
    assert properties["metadata.table-uuid"]
    assert table.name == "t1"
    assert table.catalog == "native-iceberg-rest-tests"
    assert table.database == namespace
    assert table.comment == "peow"
    assert table.constraints == []
    assert table.location == expected_location
    assert table.format == "iceberg"
    assert table.partition_by == []
    assert table.sort_by == []
    assert table.bucket_by is None
    assert _column_statuses(table) == {
        ("foo", "Utf8", True, None, False),
        ("bar", "Int32", False, "meow", False),
        ("baz", "Boolean", True, None, False),
    }

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.create_table(namespace, "t1", _TABLE_COLUMNS, format="iceberg", comment="peow")

    existing = iceberg_rest_catalog.create_table(
        namespace,
        "t1",
        _TABLE_COLUMNS,
        format="iceberg",
        comment="peow",
        if_not_exists=True,
    )
    assert existing.name == "t1"

    detailed_location = f"s3://icebergdata/custom/{namespace[0]}/meow"
    detailed = _create_detailed_table(iceberg_rest_catalog, namespace, "t2", detailed_location)
    assert detailed.name == "t2"
    assert detailed.catalog == "native-iceberg-rest-tests"
    assert detailed.database == namespace
    assert detailed.comment == "test table"
    assert detailed.constraints == [("primary_key", None, ["bar"])]
    assert detailed.location == detailed_location
    assert detailed.format == "iceberg"
    assert detailed.partition_by == [("baz", None)]
    assert set(detailed.sort_by) == {("bar", False), ("foo", True)}
    assert detailed.bucket_by is None
    assert len(detailed.properties) == _DETAILED_TABLE_PROPERTY_COUNT
    assert {("option.key1", "value1"), ("owner", "mr. meow"), ("team", "data-eng")}.issubset(set(detailed.properties))
    assert _column_statuses(detailed) == {
        ("foo", "Utf8", True, None, False),
        ("bar", "Int32", False, "meow", False),
        ("baz", "Boolean", True, None, True),
    }


def test_get_table(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("get_table", "apple", "ios")
    _create_database(iceberg_rest_catalog, namespace)
    location = f"s3://icebergdata/custom/{namespace[0]}/meow"
    _create_detailed_table(iceberg_rest_catalog, namespace, "t2", location)

    _assert_detailed_table(iceberg_rest_catalog.get_table(namespace, "t2"), namespace, "t2", location)


def test_list_tables(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("list_tables")
    _create_database(iceberg_rest_catalog, namespace)
    assert iceberg_rest_catalog.list_tables(namespace) == []

    iceberg_rest_catalog.create_table(namespace, "table1", _ID_COLUMN, format="iceberg")
    iceberg_rest_catalog.create_table(namespace, "table2", _ID_COLUMN, format="iceberg")

    tables = iceberg_rest_catalog.list_tables(namespace)
    assert {table.name for table in tables} == {"table1", "table2"}
    for table in tables:
        assert table.catalog == "native-iceberg-rest-tests"
        assert table.database == namespace
        assert table.kind == "table"
        assert table.format == "iceberg"


def test_drop_table(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("drop_table")
    _create_database(iceberg_rest_catalog, namespace)
    iceberg_rest_catalog.create_table(namespace, "t1", _ID_COLUMN, format="iceberg")
    assert iceberg_rest_catalog.get_table(namespace, "t1").name == "t1"

    iceberg_rest_catalog.drop_table(namespace, "t1")
    with pytest.raises(TableNotFoundError):
        iceberg_rest_catalog.get_table(namespace, "t1")
    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.drop_table(namespace, "t1")
    iceberg_rest_catalog.drop_table(namespace, "t1", if_exists=True)

    iceberg_rest_catalog.create_table(namespace, "t2", _ID_COLUMN, format="iceberg")
    iceberg_rest_catalog.drop_table(namespace, "t2", purge=True)
    with pytest.raises(TableNotFoundError):
        iceberg_rest_catalog.get_table(namespace, "t2")


def test_create_view(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("create_view")
    _create_database(iceberg_rest_catalog, namespace)

    view = iceberg_rest_catalog.create_view(
        namespace,
        "view1",
        _VIEW_COLUMNS,
        "SELECT * FROM table1",
        comment="test view",
    )
    properties = _properties(view)
    static_properties = {
        key: value for key, value in properties.items() if key not in {"metadata-location", "metadata.view-uuid"}
    }
    expected_location = f"s3://icebergdata/demo/{namespace[0]}/view1"

    assert len(properties) == _VIEW_PROPERTY_COUNT
    assert properties["metadata-location"].startswith(f"{expected_location}/metadata/")
    assert properties["metadata.view-uuid"]
    assert static_properties == {
        "comment": "test view",
        "metadata.format-version": "1",
        "metadata.location": expected_location,
        "metadata.current-version-id": "1",
    }
    assert view.name == "view1"
    assert view.catalog == "native-iceberg-rest-tests"
    assert view.database == namespace
    assert view.kind == "view"
    assert view.comment == "test view"
    assert view.view_definition == "SELECT * FROM table1"
    assert _column_statuses(view) == {
        ("col1", "Utf8", True, None, False),
        ("col2", "Int32", False, "important column", False),
    }

    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.create_view(
            namespace,
            "view1",
            _VIEW_COLUMNS,
            "SELECT * FROM table1",
            comment="test view",
        )

    existing = iceberg_rest_catalog.create_view(
        namespace,
        "view1",
        _VIEW_COLUMNS,
        "SELECT * FROM table1",
        if_not_exists=True,
        comment="test view",
    )
    assert existing.name == "view1"

    second = iceberg_rest_catalog.create_view(
        namespace,
        "view2",
        _VIEW_COLUMNS,
        "SELECT col1, col2 FROM table2 WHERE col2 > 10",
        comment="another view",
        properties=[("owner", "alice"), ("team", "analytics")],
    )
    assert second.name == "view2"
    assert second.catalog == "native-iceberg-rest-tests"
    assert second.database == namespace
    assert second.comment == "another view"
    assert second.view_definition == "SELECT col1, col2 FROM table2 WHERE col2 > 10"
    assert {("owner", "alice"), ("team", "analytics")}.issubset(set(second.properties))


def test_get_view(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("get_view")
    _create_database(iceberg_rest_catalog, namespace)
    columns = [("foo", "utf8", True, None), ("bar", "int32", False, "meow")]
    iceberg_rest_catalog.create_view(
        namespace,
        "view1",
        columns,
        "SELECT foo, bar FROM source_table",
        comment="test view",
        properties=[("owner", "bob"), ("version", "1.0")],
    )

    view = iceberg_rest_catalog.get_view(namespace, "view1")
    assert view.name == "view1"
    assert view.catalog == "native-iceberg-rest-tests"
    assert view.database == namespace
    assert view.comment == "test view"
    assert view.view_definition == "SELECT foo, bar FROM source_table"
    assert _column_statuses(view) == {
        ("foo", "Utf8", True, None, False),
        ("bar", "Int32", False, "meow", False),
    }
    assert {("owner", "bob"), ("version", "1.0")}.issubset(set(view.properties))

    with pytest.raises(ViewNotFoundError):
        iceberg_rest_catalog.get_view(namespace, "nonexistent")


def test_list_views(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("list_views")
    _create_database(iceberg_rest_catalog, namespace)
    assert iceberg_rest_catalog.list_views(namespace) == []

    iceberg_rest_catalog.create_view(namespace, "view1", _ID_COLUMN, "SELECT * FROM t1")
    iceberg_rest_catalog.create_view(namespace, "view2", _ID_COLUMN, "SELECT * FROM t2")

    views = iceberg_rest_catalog.list_views(namespace)
    assert {view.name for view in views} == {"view1", "view2"}
    for view in views:
        assert view.catalog == "native-iceberg-rest-tests"
        assert view.database == namespace
        assert view.kind == "view"


def test_drop_view(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    namespace = _namespace("drop_view")
    _create_database(iceberg_rest_catalog, namespace)
    iceberg_rest_catalog.create_view(namespace, "view1", _ID_COLUMN, "SELECT * FROM t1")
    assert iceberg_rest_catalog.get_view(namespace, "view1").name == "view1"

    iceberg_rest_catalog.drop_view(namespace, "view1")
    with pytest.raises(ViewNotFoundError):
        iceberg_rest_catalog.get_view(namespace, "view1")
    with pytest.raises(RuntimeError):
        iceberg_rest_catalog.drop_view(namespace, "view1")
    iceberg_rest_catalog.drop_view(namespace, "view1", if_exists=True)


def _assert_partition_transform(
    catalog: IcebergRestCatalogProvider,
    test_name: str,
    table: str,
    partition: tuple[str, str | None],
) -> None:
    namespace = _namespace(test_name)
    _create_database(catalog, namespace)
    created = catalog.create_table(
        namespace,
        table,
        _PARTITION_COLUMNS,
        format="iceberg",
        partition_by=[partition],
    )
    assert created.partition_by == [partition]


def test_create_table_partition_identity(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    _assert_partition_transform(iceberg_rest_catalog, "partition_identity", "identity_table", ("id", None))


def test_create_table_partition_year(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    _assert_partition_transform(iceberg_rest_catalog, "partition_year", "year_table", ("ts", "year"))


def test_create_table_partition_bucket(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    _assert_partition_transform(iceberg_rest_catalog, "partition_bucket", "bucket_table", ("id", "bucket(16)"))


def test_create_table_partition_truncate(iceberg_rest_catalog: IcebergRestCatalogProvider) -> None:
    _assert_partition_transform(
        iceberg_rest_catalog,
        "partition_truncate",
        "truncate_table",
        ("name", "truncate(10)"),
    )
