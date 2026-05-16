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


def _current_view_version(metadata: dict[str, Any]) -> dict[str, Any]:
    current_version_id = metadata["current-version-id"]
    return next(v for v in metadata["versions"] if v["version-id"] == current_version_id)


def test_create_view_uses_default_location_and_resolves_schema_id(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    view_name = "default_loc_view"
    _create_view(iceberg_spark, view_name)

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)

    assert metadata["location"] == f"s3://icebergdata/demo/{NAMESPACE}/{view_name}"
    assert metadata["current-version-id"] == 1

    current_version = _current_view_version(metadata)
    # `create_view` sends schema-id = -1 in the view version ("last added schema"). The REST
    # catalog should assign a schema-id to the provided schema and store that assigned id in the
    # created view version (i.e. it must not persist as -1).
    assert current_version["schema-id"] != -1
    assert any(schema["schema-id"] == current_version["schema-id"] for schema in metadata["schemas"])


def test_rest_create_view_location_is_honored(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    seed_view = "seed_view"
    _create_view(iceberg_spark, seed_view)
    seed_metadata = _load_view_metadata(iceberg_rest_endpoint, seed_view)
    seed_current_version = _current_view_version(seed_metadata)

    explicit_view = "explicit_loc_view"
    explicit_location = f"s3://icebergdata/demo/{NAMESPACE}/{explicit_view}-custom-location"

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

    request_body: dict[str, Any] = {
        "name": explicit_view,
        "location": explicit_location,
        "schema": schema,
        "view-version": view_version,
        "properties": {},
    }

    namespace = _quote(NAMESPACE)
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/views"
    request = urllib.request.Request(  # noqa: S310
        url,
        data=json.dumps(request_body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        created = json.load(response)["metadata"]

    assert created["location"] == explicit_location

    loaded = _load_view_metadata(iceberg_rest_endpoint, explicit_view)
    assert loaded["location"] == explicit_location
