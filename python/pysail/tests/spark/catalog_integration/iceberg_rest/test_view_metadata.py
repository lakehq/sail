from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_view_metadata_test"


@pytest.fixture(scope="module", autouse=True)
def namespace(iceberg_spark: SparkSession) -> Generator[None, None, None]:
    iceberg_spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    iceberg_spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


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
    definition: str = "SELECT 1 AS id",
) -> None:
    iceberg_spark.sql(f"DROP VIEW IF EXISTS {NAMESPACE}.{view_name}")
    iceberg_spark.sql(f"CREATE VIEW {NAMESPACE}.{view_name} AS {definition}")


def _assert_schema_id_resolved(metadata: dict[str, Any]) -> None:
    current_version = _current_view_version(metadata)
    assert current_version["schema-id"] != -1
    assert any(schema["schema-id"] == current_version["schema-id"] for schema in metadata["schemas"])


def _assert_spark_representation_present(metadata: dict[str, Any]) -> None:
    current_version = _current_view_version(metadata)
    representations = current_version["representations"]
    assert representations
    assert any(
        representation.get("type") == "sql" and representation.get("dialect", "").strip().lower() == "spark"
        for representation in representations
    )


def _current_view_version(metadata: dict[str, Any]) -> dict[str, Any]:
    current_version_id = metadata["current-version-id"]
    return next(v for v in metadata["versions"] if v["version-id"] == current_version_id)


def _rest_create_view(
    iceberg_rest_endpoint: str,
    view_name: str,
    *,
    schema: dict[str, Any],
    view_version: dict[str, Any],
    properties: dict[str, str] | None = None,
    location: str | None = None,
) -> dict[str, Any]:
    namespace = _quote(NAMESPACE)
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/views"

    request_body: dict[str, Any] = {
        "name": view_name,
        "schema": schema,
        "view-version": view_version,
        "properties": properties or {},
    }
    if location is not None:
        request_body["location"] = location

    request = urllib.request.Request(  # noqa: S310
        url,
        data=json.dumps(request_body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        return json.load(response)["metadata"]


@pytest.mark.parametrize(
    ("view_name", "definition"),
    [
        ("default_loc_view_1", "SELECT 1 AS id"),
        ("default_loc_view_2", "SELECT CAST(1 AS BIGINT) AS id, 'x' AS value"),
        ("default_loc_view_hyphen", "SELECT 1 AS id, 9.99 AS price"),
    ],
)
def test_create_view_uses_default_location_and_resolves_schema_id(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
    view_name: str,
    definition: str,
) -> None:
    _create_view(iceberg_spark, view_name, definition=definition)

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)

    assert metadata["location"] == f"s3://icebergdata/demo/{NAMESPACE}/{view_name}"
    assert metadata["current-version-id"] == 1
    _assert_schema_id_resolved(metadata)
    _assert_spark_representation_present(metadata)


def test_rest_create_view_location_is_honored(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    seed_view = "seed_view"
    _create_view(iceberg_spark, seed_view)
    seed_metadata = _load_view_metadata(iceberg_rest_endpoint, seed_view)
    seed_current_version = _current_view_version(seed_metadata)

    schema = dict(seed_metadata["schemas"][-1])
    schema.pop("schema-id", None)

    timestamp_ms = int(time.time() * 1000)
    view_version: dict[str, Any] = {
        "version-id": 1,
        "timestamp-ms": timestamp_ms,
        "schema-id": -1,
        "summary": {},
        "representations": seed_current_version["representations"],
        "default-namespace": [NAMESPACE],
    }

    for view_name, explicit_location in [
        (
            "explicit_loc_view",
            f"s3://icebergdata/demo/{NAMESPACE}/explicit_loc_view-custom-location",
        ),
        (
            "explicit_loc_view_2",
            f"s3://icebergdata/demo/{NAMESPACE}/custom/prefix/explicit_loc_view_2",
        ),
    ]:
        created = _rest_create_view(
            iceberg_rest_endpoint,
            view_name,
            schema=schema,
            view_version=view_version,
            location=explicit_location,
        )
        assert created["location"] == explicit_location

        loaded = _load_view_metadata(iceberg_rest_endpoint, view_name)
        assert loaded["location"] == explicit_location
        assert loaded["current-version-id"] == 1
        _assert_schema_id_resolved(loaded)
        _assert_spark_representation_present(loaded)


def test_rest_create_multiple_views_same_location_is_honored(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    seed_view = "seed_view_same_loc"
    _create_view(iceberg_spark, seed_view)
    seed_metadata = _load_view_metadata(iceberg_rest_endpoint, seed_view)
    seed_current_version = _current_view_version(seed_metadata)

    schema = dict(seed_metadata["schemas"][-1])
    schema.pop("schema-id", None)

    timestamp_ms = int(time.time() * 1000)
    view_version: dict[str, Any] = {
        "version-id": 1,
        "timestamp-ms": timestamp_ms,
        "schema-id": -1,
        "summary": {},
        "representations": seed_current_version["representations"],
        "default-namespace": [NAMESPACE],
    }

    shared_location = f"s3://icebergdata/demo/{NAMESPACE}/shared/location/for/views"
    view_names = [f"same_loc_view_{i}" for i in range(1, 6)]
    for view_name in view_names:
        created = _rest_create_view(
            iceberg_rest_endpoint,
            view_name,
            schema=schema,
            view_version=view_version,
            location=shared_location,
        )
        assert created["location"] == shared_location

    loaded_metadata = [_load_view_metadata(iceberg_rest_endpoint, view_name) for view_name in view_names]
    view_uuids = {metadata["view-uuid"] for metadata in loaded_metadata}
    assert len(view_uuids) == len(view_names)

    for metadata in loaded_metadata:
        assert metadata["location"] == shared_location
        assert metadata["current-version-id"] == 1
        _assert_schema_id_resolved(metadata)
        _assert_spark_representation_present(metadata)


def test_rest_create_view_without_location_uses_default(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    seed_view = "seed_view_no_loc"
    _create_view(iceberg_spark, seed_view)
    seed_metadata = _load_view_metadata(iceberg_rest_endpoint, seed_view)
    seed_current_version = _current_view_version(seed_metadata)

    schema = dict(seed_metadata["schemas"][-1])
    schema.pop("schema-id", None)

    timestamp_ms = int(time.time() * 1000)
    view_version: dict[str, Any] = {
        "version-id": 1,
        "timestamp-ms": timestamp_ms,
        "schema-id": -1,
        "summary": {},
        "representations": seed_current_version["representations"],
        "default-namespace": [NAMESPACE],
    }

    view_name = "rest_default_location_view"
    created = _rest_create_view(
        iceberg_rest_endpoint,
        view_name,
        schema=schema,
        view_version=view_version,
        location=None,
    )
    assert created["location"] == f"s3://icebergdata/demo/{NAMESPACE}/{view_name}"

    loaded = _load_view_metadata(iceberg_rest_endpoint, view_name)
    assert loaded["location"] == f"s3://icebergdata/demo/{NAMESPACE}/{view_name}"
    _assert_schema_id_resolved(loaded)


def test_rest_create_view_overrides_provided_schema_id(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    seed_view = "seed_view_schema_id_override"
    _create_view(iceberg_spark, seed_view)
    seed_metadata = _load_view_metadata(iceberg_rest_endpoint, seed_view)
    seed_current_version = _current_view_version(seed_metadata)

    schema = dict(seed_metadata["schemas"][-1])
    schema.pop("schema-id", None)

    provided_schema_id = 12345
    timestamp_ms = int(time.time() * 1000)
    view_version: dict[str, Any] = {
        "version-id": 1,
        "timestamp-ms": timestamp_ms,
        "schema-id": provided_schema_id,
        "summary": {},
        "representations": seed_current_version["representations"],
        "default-namespace": [NAMESPACE],
    }

    view_name = "rest_schema_id_override_view"
    created = _rest_create_view(
        iceberg_rest_endpoint,
        view_name,
        schema=schema,
        view_version=view_version,
        location=f"s3://icebergdata/demo/{NAMESPACE}/schema-id-override-location",
    )
    current_version = _current_view_version(created)
    assert current_version["schema-id"] != provided_schema_id

    loaded = _load_view_metadata(iceberg_rest_endpoint, view_name)
    loaded_current_version = _current_view_version(loaded)
    assert loaded_current_version["schema-id"] != provided_schema_id
    _assert_schema_id_resolved(loaded)
