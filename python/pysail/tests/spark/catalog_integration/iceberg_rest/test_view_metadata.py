from __future__ import annotations

import json
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_view_metadata_test"
WAREHOUSE = "s3://icebergdata/demo"


@pytest.fixture(scope="module", autouse=True)
def namespace(iceberg_spark: SparkSession) -> Generator[None, None, None]:
    iceberg_spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    iceberg_spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _load_view_metadata(iceberg_rest_endpoint: str, view_name: str) -> dict[str, Any]:
    namespace = _quote(NAMESPACE)
    view = _quote(view_name)
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/views/{view}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)["metadata"]


def _create_view(
    iceberg_spark: SparkSession,
    view_name: str,
    *,
    definition: str = "SELECT 1 AS id",
    comment: str | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    iceberg_spark.sql(f"DROP VIEW IF EXISTS {NAMESPACE}.{view_name}")

    clauses: list[str] = []
    if comment is not None:
        clauses.append(f"COMMENT '{_sql_string(comment)}'")
    if properties:
        props_sql = ", ".join(
            f"'{_sql_string(key)}'='{_sql_string(value)}'" for key, value in sorted(properties.items())
        )
        clauses.append(f"TBLPROPERTIES ({props_sql})")

    clause_sql = (" " + " ".join(clauses)) if clauses else ""
    iceberg_spark.sql(f"CREATE VIEW {NAMESPACE}.{view_name}{clause_sql} AS {definition}")


def _current_view_version(metadata: dict[str, Any]) -> dict[str, Any]:
    current_version_id = metadata["current-version-id"]
    return next(version for version in metadata["versions"] if version["version-id"] == current_version_id)


def _schemas_by_id(metadata: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {schema["schema-id"]: schema for schema in metadata["schemas"]}


def _assert_all_view_schema_ids_resolved(metadata: dict[str, Any]) -> None:
    schemas_by_id = _schemas_by_id(metadata)
    assert schemas_by_id

    for version in metadata["versions"]:
        schema_id = version["schema-id"]
        assert schema_id != -1
        assert schema_id in schemas_by_id


def _assert_current_schema_fields(metadata: dict[str, Any], expected_field_names: list[str]) -> None:
    current_version = _current_view_version(metadata)
    current_schema = _schemas_by_id(metadata)[current_version["schema-id"]]
    assert [field["name"] for field in current_schema["fields"]] == expected_field_names


def _assert_spark_representation(metadata: dict[str, Any]) -> None:
    current_version = _current_view_version(metadata)
    representations = current_version["representations"]
    assert representations
    assert any(
        representation.get("type") == "sql"
        and representation.get("dialect", "").strip().lower() == "spark"
        and representation.get("sql")
        for representation in representations
    )


def _collect_one_row(iceberg_spark: SparkSession, sql: str) -> dict[str, Any]:
    rows = iceberg_spark.sql(sql).collect()
    assert len(rows) == 1
    return rows[0].asDict(recursive=True)


@pytest.mark.parametrize("property_key", ["path", "location"])
def test_create_view_uses_path_or_location_property_as_metadata_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
    property_key: str,
) -> None:
    view_name = f"{property_key}_property_location_view"
    explicit_location = f"{WAREHOUSE}/{NAMESPACE}/custom/{property_key}/{view_name}"
    definition = f"SELECT 7 AS id, '{property_key}' AS property_key"
    properties = {
        property_key: explicit_location,
        "purpose": f"{property_key}-location-resolution",
    }

    _create_view(iceberg_spark, view_name, definition=definition, properties=properties)

    assert _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}") == {  # noqa: S608
        "id": 7,
        "property_key": property_key,
    }

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)
    assert metadata["location"] == explicit_location
    assert metadata["properties"]["location"] == explicit_location
    if property_key == "path":
        assert metadata["properties"]["path"] == explicit_location
    assert metadata["properties"]["purpose"] == f"{property_key}-location-resolution"
    assert metadata["current-version-id"] == 1
    assert _current_view_version(metadata)["version-id"] == 1
    _assert_all_view_schema_ids_resolved(metadata)
    _assert_current_schema_fields(metadata, ["id", "property_key"])
    _assert_spark_representation(metadata)


def test_create_view_without_path_or_location_property_uses_default_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    view_name = "default_location_view"
    definition = "SELECT 1 AS id, 'default' AS location_mode"
    properties = {"owner": "integration-test", "purpose": "default-location"}

    _create_view(iceberg_spark, view_name, definition=definition, properties=properties)

    assert _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}") == {  # noqa: S608
        "id": 1,
        "location_mode": "default",
    }

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)
    assert metadata["location"] == f"{WAREHOUSE}/{NAMESPACE}/{view_name}"
    assert metadata["properties"]["owner"] == "integration-test"
    assert metadata["properties"]["purpose"] == "default-location"
    _assert_all_view_schema_ids_resolved(metadata)
    _assert_current_schema_fields(metadata, ["id", "location_mode"])
    _assert_spark_representation(metadata)


def test_create_view_resolves_negative_one_schema_id_to_added_schema(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    view_name = "schema_id_sentinel_view"
    definition = "SELECT CAST(1 AS BIGINT) AS id, 'sail' AS engine, CAST(9.99 AS DECIMAL(10, 2)) AS price"

    _create_view(iceberg_spark, view_name, definition=definition)

    assert _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}") == {  # noqa: S608
        "id": 1,
        "engine": "sail",
        "price": Decimal("9.99"),
    }

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)
    current_version = _current_view_version(metadata)
    schemas_by_id = _schemas_by_id(metadata)

    assert current_version["schema-id"] != -1
    assert current_version["schema-id"] in schemas_by_id
    assert all(schema_id != -1 for schema_id in schemas_by_id)
    _assert_all_view_schema_ids_resolved(metadata)
    _assert_current_schema_fields(metadata, ["id", "engine", "price"])
    _assert_spark_representation(metadata)


def test_create_multiple_views_can_share_location_property(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    shared_location = f"{WAREHOUSE}/{NAMESPACE}/shared/location/for/views"
    view_names = [f"same_location_view_{i}" for i in range(1, 5)]

    for i, view_name in enumerate(view_names, start=1):
        _create_view(
            iceberg_spark,
            view_name,
            definition=f"SELECT {i} AS id",
            properties={"location": shared_location},
        )

    loaded_metadata = [_load_view_metadata(iceberg_rest_endpoint, view_name) for view_name in view_names]
    assert len({metadata["view-uuid"] for metadata in loaded_metadata}) == len(view_names)

    for i, (view_name, metadata) in enumerate(zip(view_names, loaded_metadata, strict=True), start=1):
        assert metadata["location"] == shared_location
        assert metadata["properties"]["location"] == shared_location
        assert metadata["current-version-id"] == 1
        assert _current_view_version(metadata)["version-id"] == 1
        _assert_all_view_schema_ids_resolved(metadata)
        _assert_current_schema_fields(metadata, ["id"])
        assert _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}") == {"id": i}  # noqa: S608
