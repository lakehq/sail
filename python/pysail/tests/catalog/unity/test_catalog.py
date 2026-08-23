"""Native Unity Catalog provider integration tests."""

from __future__ import annotations

import contextlib
import uuid

import pytest

from pysail import _native
from pysail.testing.containers.unity_defaults import DEFAULT_CATALOG

UnityCatalogProvider = _native._catalog._unity.UnityCatalogProvider  # noqa: SLF001
DatabaseNotFoundError = _native._catalog.DatabaseNotFoundError  # noqa: SLF001
TableNotFoundError = _native._catalog.TableNotFoundError  # noqa: SLF001

_DELTA_UNITY_TABLE_ID_KEY = "io.unitycatalog.tableId"
_DELTA_UNITY_TABLE_ID_LEGACY_KEY = "table_id"
_COMPLEX_COLUMNS = [
    ("foo", "utf8", True, None),
    (
        "bar",
        'List(Struct("a": Utf8, "b": non-null Int32))',
        False,
        "meow",
    ),
    (
        "baz",
        'Map("entries": non-null Struct("key": non-null Utf8, "value": Int32), unsorted)',
        True,
        None,
    ),
    (
        "mew",
        'Struct("a": Utf8, "b": non-null Int32)',
        True,
        None,
    ),
]
_SIMPLE_COLUMNS = [
    ("foo", "utf8", True, None),
    ("bar", "int32", False, "meow"),
    ("baz", "boolean", True, None),
]
_ID_COLUMN = [("id", "int32", False, None)]


def _database(prefix: str) -> list[str]:
    return [f"{prefix}_{uuid.uuid4().hex}"]


def _full_database(database: list[str]) -> list[str]:
    return [DEFAULT_CATALOG, *database]


def _properties(status: object) -> dict[str, str]:
    return dict(status.properties)


def _columns(status: object) -> dict[str, object]:
    return {column.name: column for column in status.columns}


def _location(database: list[str], table: str) -> str:
    return f"s3://deltadata/{database[-1]}/{table}"


def _cleanup_database(catalog: UnityCatalogProvider, database: list[str]) -> None:
    with contextlib.suppress(DatabaseNotFoundError, RuntimeError):
        for table in catalog.list_tables(database):
            catalog.drop_table(database, table.name, if_exists=True)
        catalog.drop_database(database, if_exists=True)


def _assert_simple_table(table: object, database: list[str]) -> None:
    properties = _properties(table)
    columns = _columns(table)

    assert table.name == "t2"
    assert table.catalog == "native-unity-tests"
    assert table.database == _full_database(database)
    assert table.kind == "table"
    assert table.comment == "test table"
    assert table.constraints == []
    assert table.location == _location(database, "t2")
    assert table.format == "delta"
    assert table.partition_by == [("baz", None)]
    assert table.sort_by == []
    assert table.bucket_by is None
    assert set(properties) == {
        "updated_at",
        "created_at",
        _DELTA_UNITY_TABLE_ID_LEGACY_KEY,
        _DELTA_UNITY_TABLE_ID_KEY,
        "comment",
        "table_type",
        "option.key1",
        "owner",
        "team",
    }
    assert properties["option.key1"] == "value1"
    assert properties["owner"] == "mr. meow"
    assert properties["team"] == "data-eng"
    assert properties[_DELTA_UNITY_TABLE_ID_KEY] == properties[_DELTA_UNITY_TABLE_ID_LEGACY_KEY]
    assert set(columns) == {"foo", "bar", "baz"}
    assert columns["foo"].data_type == "Utf8"
    assert columns["foo"].nullable is True
    assert columns["foo"].is_partition is False
    assert columns["bar"].data_type == "Int32"
    assert columns["bar"].nullable is False
    assert columns["bar"].comment == "meow"
    assert columns["baz"].data_type == "Boolean"
    assert columns["baz"].nullable is True
    assert columns["baz"].is_partition is True


def test_create_schema(unity_catalog: UnityCatalogProvider) -> None:
    database = _database("create_schema")
    second_database = _database("create_schema_if_not_exists")
    full_database = _full_database(database)
    full_second_database = _full_database(second_database)
    try:
        created = unity_catalog.create_database(
            database,
            comment="test comment",
            location="s3://bucket/path",
            properties=[("key1", "value1")],
        )
        properties = _properties(created)
        dynamic_keys = {"schema_id", "updated_at", "created_at"}

        assert created.catalog == "native-unity-tests"
        assert created.database == full_database
        assert created.comment == "test comment"
        assert created.location == "s3://bucket/path"
        assert set(properties) == dynamic_keys | {"comment", "location", "key1"}
        assert {key: value for key, value in properties.items() if key not in dynamic_keys} == {
            "comment": "test comment",
            "location": "s3://bucket/path",
            "key1": "value1",
        }
        for key in dynamic_keys:
            assert properties[key]

        with pytest.raises(RuntimeError):
            unity_catalog.create_database(database)
        with pytest.raises(RuntimeError):
            unity_catalog.create_database(full_database)

        created_second = unity_catalog.create_database(full_second_database, if_not_exists=True)
        assert created_second.database == full_second_database
        assert created_second.comment is None
        assert created_second.location is None

        created_again = unity_catalog.create_database(
            second_database,
            if_not_exists=True,
            comment="should be ignored",
            location="should be ignored",
            properties=[("ignored", "ignored")],
        )
        assert created_again.database == full_second_database
        assert created_again.comment is None
        assert created_again.location is None
    finally:
        _cleanup_database(unity_catalog, database)
        _cleanup_database(unity_catalog, second_database)


def test_get_nonexistent_schema(unity_catalog: UnityCatalogProvider) -> None:
    with pytest.raises(DatabaseNotFoundError):
        unity_catalog.get_database(_database("get_nonexistent_schema"))


def test_get_schema(unity_catalog: UnityCatalogProvider) -> None:
    database = _database("get_schema")
    full_database = _full_database(database)
    properties = [("owner", "Lake"), ("community", "Sail")]
    try:
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(database)
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(full_database)

        created = unity_catalog.create_database(database, properties=properties)
        assert created.database == full_database
        assert set(properties).issubset(set(created.properties))

        for namespace in (database, full_database):
            fetched = unity_catalog.get_database(namespace)
            assert fetched.database == full_database
            assert set(properties).issubset(set(fetched.properties))
    finally:
        _cleanup_database(unity_catalog, database)


def test_list_schemas(unity_catalog: UnityCatalogProvider) -> None:
    prefix = f"list_schemas_{uuid.uuid4().hex}"
    databases = [[f"{prefix}_{suffix}"] for suffix in ("ios", "macos")]

    def listed_names(parent: list[str] | None) -> list[tuple[str, ...]]:
        return [
            tuple(status.database)
            for status in unity_catalog.list_databases(parent)
            if status.database[-1].startswith(prefix)
        ]

    try:
        assert listed_names(None) == []
        assert listed_names([DEFAULT_CATALOG]) == []

        unity_catalog.create_database(
            databases[0],
            properties=[("owner", "Lake"), ("community", "Sail")],
        )
        unity_catalog.create_database(
            databases[1],
            properties=[("owner", "Meow"), ("community", "Peow")],
        )

        expected = sorted(tuple(_full_database(database)) for database in databases)
        assert sorted(listed_names([DEFAULT_CATALOG])) == expected
        assert sorted(listed_names(None)) == expected
    finally:
        for database in databases:
            _cleanup_database(unity_catalog, database)


def test_drop_schema(unity_catalog: UnityCatalogProvider) -> None:
    database = _database("drop_schema")
    full_database = _full_database(database)
    cascade_database = _database("drop_schema_cascade")
    full_cascade_database = _full_database(cascade_database)
    try:
        unity_catalog.create_database(database)
        assert unity_catalog.get_database(database).database == full_database
        assert unity_catalog.get_database(full_database).database == full_database

        unity_catalog.drop_database(database)
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(database)
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(full_database)
        with pytest.raises(RuntimeError):
            unity_catalog.drop_database(full_database)
        unity_catalog.drop_database(database, if_exists=True)

        unity_catalog.create_database(cascade_database)
        assert unity_catalog.get_database(cascade_database).database == full_cascade_database
        assert unity_catalog.get_database(full_cascade_database).database == full_cascade_database
        unity_catalog.drop_database(cascade_database, cascade=True)
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(cascade_database)
        with pytest.raises(DatabaseNotFoundError):
            unity_catalog.get_database(full_cascade_database)
    finally:
        _cleanup_database(unity_catalog, database)
        _cleanup_database(unity_catalog, cascade_database)


def test_create_table(
    unity_catalog: UnityCatalogProvider,
    unity_database: list[str],
) -> None:
    table = unity_catalog.create_table(
        unity_database,
        "t1",
        _COMPLEX_COLUMNS,
        format="delta",
        comment="peow",
        location=_location(unity_database, "t1"),
    )
    properties = _properties(table)
    columns = _columns(table)

    assert table.name == "t1"
    assert table.catalog == "native-unity-tests"
    assert table.database == _full_database(unity_database)
    assert table.kind == "table"
    assert table.comment == "peow"
    assert table.constraints == []
    assert table.location == _location(unity_database, "t1")
    assert table.format == "delta"
    assert table.partition_by == []
    assert table.sort_by == []
    assert table.bucket_by is None
    assert set(properties) == {
        "updated_at",
        "created_at",
        _DELTA_UNITY_TABLE_ID_LEGACY_KEY,
        _DELTA_UNITY_TABLE_ID_KEY,
        "comment",
        "table_type",
    }
    assert properties["updated_at"]
    assert properties["created_at"]
    assert properties[_DELTA_UNITY_TABLE_ID_KEY] == properties[_DELTA_UNITY_TABLE_ID_LEGACY_KEY]
    assert properties["comment"] == "peow"
    assert properties["table_type"] == "EXTERNAL"
    assert set(columns) == {"foo", "bar", "baz", "mew"}
    assert columns["foo"].data_type == "Utf8"
    assert columns["foo"].nullable is True
    assert columns["bar"].data_type == 'List(Struct("a": Utf8, "b": non-null Int32))'
    assert columns["bar"].nullable is False
    assert columns["bar"].comment == "meow"
    assert columns["baz"].data_type == 'Map("entries": non-null Struct("key": non-null Utf8, "value": Int32), unsorted)'
    assert columns["baz"].nullable is True
    assert columns["mew"].data_type == 'Struct("a": Utf8, "b": non-null Int32)'
    assert columns["mew"].nullable is True

    with pytest.raises(RuntimeError):
        unity_catalog.create_table(
            unity_database,
            "t1",
            _COMPLEX_COLUMNS,
            format="delta",
            comment="peow",
            location=_location(unity_database, "t1"),
        )
    existing = unity_catalog.create_table(
        unity_database,
        "t1",
        _COMPLEX_COLUMNS,
        format="delta",
        comment="peow",
        location=_location(unity_database, "t1"),
        if_not_exists=True,
    )
    assert existing.name == "t1"

    simple_table = unity_catalog.create_table(
        unity_database,
        "t2",
        _SIMPLE_COLUMNS,
        format="delta",
        comment="test table",
        location=_location(unity_database, "t2"),
        partition_by=[("baz", None)],
        properties=[
            ("option.key1", "value1"),
            ("owner", "mr. meow"),
            ("team", "data-eng"),
        ],
    )
    _assert_simple_table(simple_table, unity_database)


def test_get_table(
    unity_catalog: UnityCatalogProvider,
    unity_database: list[str],
) -> None:
    unity_catalog.create_table(
        unity_database,
        "t2",
        _SIMPLE_COLUMNS,
        format="delta",
        comment="test table",
        location=_location(unity_database, "t2"),
        partition_by=[("baz", None)],
        properties=[
            ("option.key1", "value1"),
            ("owner", "mr. meow"),
            ("team", "data-eng"),
        ],
    )

    short_namespace_table = unity_catalog.get_table(unity_database, "t2")
    full_namespace_table = unity_catalog.get_table(_full_database(unity_database), "t2")
    assert short_namespace_table.name == full_namespace_table.name
    _assert_simple_table(short_namespace_table, unity_database)


def test_list_tables(
    unity_catalog: UnityCatalogProvider,
    unity_database: list[str],
) -> None:
    assert unity_catalog.list_tables(unity_database) == []
    for table_name in ("table1", "table2"):
        unity_catalog.create_table(
            unity_database,
            table_name,
            _ID_COLUMN,
            format="delta",
            location=_location(unity_database, table_name),
        )

    for namespace in (unity_database, _full_database(unity_database)):
        tables = unity_catalog.list_tables(namespace)
        assert sorted(table.name for table in tables) == ["table1", "table2"]
        for table in tables:
            assert table.catalog == "native-unity-tests"
            assert table.database == _full_database(unity_database)
            assert table.kind == "table"
            assert table.format == "delta"


def test_drop_table(
    unity_catalog: UnityCatalogProvider,
    unity_database: list[str],
) -> None:
    unity_catalog.create_table(
        unity_database,
        "t1",
        _ID_COLUMN,
        format="delta",
        location=_location(unity_database, "t1"),
    )
    assert unity_catalog.get_table(unity_database, "t1").name == "t1"

    unity_catalog.drop_table(unity_database, "t1")
    with pytest.raises(TableNotFoundError):
        unity_catalog.get_table(unity_database, "t1")
    with pytest.raises(RuntimeError):
        unity_catalog.drop_table(unity_database, "t1")
    unity_catalog.drop_table(unity_database, "t1", if_exists=True)
