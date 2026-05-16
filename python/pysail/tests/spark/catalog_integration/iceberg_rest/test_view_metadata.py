from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_view_metadata_test"


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


def _assert_view_missing(iceberg_rest_endpoint: str, view_name: str) -> None:
    namespace = _quote(NAMESPACE)
    view = _quote(view_name)
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/views/{view}"
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
        urllib.request.urlopen(url, timeout=30)  # noqa: S310
    assert excinfo.value.code == 404


def _create_view(
    iceberg_spark: SparkSession,
    view_name: str,
    *,
    definition: str = "SELECT 1 AS id",
    location: str | None = None,
    comment: str | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    iceberg_spark.sql(f"DROP VIEW IF EXISTS {NAMESPACE}.{view_name}")

    clauses: list[str] = []
    if comment is not None:
        clauses.append(f"COMMENT '{_sql_string(comment)}'")
    if location is not None:
        clauses.append(f"LOCATION '{_sql_string(location)}'")
    if properties:
        props_sql = ", ".join(
            f"'{_sql_string(k)}'='{_sql_string(v)}'" for k, v in sorted(properties.items())
        )
        clauses.append(f"TBLPROPERTIES ({props_sql})")

    clause_sql = (" " + " ".join(clauses)) if clauses else ""
    iceberg_spark.sql(
        f"CREATE VIEW {NAMESPACE}.{view_name}{clause_sql} AS {definition}"
    )


def _current_view_version(metadata: dict[str, Any]) -> dict[str, Any]:
    current_version_id = metadata["current-version-id"]
    return next(v for v in metadata["versions"] if v["version-id"] == current_version_id)


def _assert_schema_id_resolved(metadata: dict[str, Any]) -> None:
    current_version = _current_view_version(metadata)
    assert current_version["schema-id"] != -1
    assert any(schema["schema-id"] == current_version["schema-id"] for schema in metadata["schemas"])


def _assert_spark_representation_present(metadata: dict[str, Any]) -> None:
    current_version = _current_view_version(metadata)
    representations = current_version["representations"]
    assert representations
    assert any(
        representation.get("type") == "sql"
        and representation.get("dialect", "").strip().lower() == "spark"
        for representation in representations
    )


def _collect_one_row(iceberg_spark: SparkSession, sql: str) -> dict[str, Any]:
    rows = iceberg_spark.sql(sql).collect()
    assert len(rows) == 1
    return rows[0].asDict(recursive=True)


def _extract_view_names(show_views_rows: Iterable[Any]) -> set[str]:
    names: set[str] = set()
    for row in show_views_rows:
        data = row.asDict(recursive=True)
        found = False
        for key in ("viewName", "viewname", "name", "view_name", "view"):
            if key in data and isinstance(data[key], str):
                names.add(data[key])
                found = True
                break
        if not found:
            for value in data.values():
                if isinstance(value, str):
                    names.add(value)
    return names


@pytest.mark.parametrize(
    ("view_name", "definition", "expected_row"),
    [
        ("default_loc_view_1", "SELECT 1 AS id", {"id": 1}),
        ("default_loc_view_2", "SELECT CAST(1 AS BIGINT) AS id, 'x' AS value", {"id": 1, "value": "x"}),
        ("default_loc_view_3", "SELECT 1 AS id, 9.99 AS price", {"id": 1, "price": Decimal("9.99")}),
    ],
)
def test_sail_create_view_default_location_and_round_trip(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
    view_name: str,
    definition: str,
    expected_row: dict[str, Any],
) -> None:
    _create_view(iceberg_spark, view_name, definition=definition)

    row = _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}")
    assert row == expected_row

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)
    assert metadata["location"] == f"s3://icebergdata/demo/{NAMESPACE}/{view_name}"
    assert metadata["current-version-id"] == 1
    assert _current_view_version(metadata)["version-id"] == 1
    _assert_schema_id_resolved(metadata)
    _assert_spark_representation_present(metadata)


def test_sail_create_view_explicit_location_comment_and_properties_are_honored(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    view_name = "explicit_loc_view"
    explicit_location = f"s3://icebergdata/demo/{NAMESPACE}/custom/prefix/{view_name}"
    comment = "created by Sail via CREATE VIEW"
    properties = {"owner": "integration-test", "purpose": "metadata-validation"}
    definition = "SELECT 7 AS id, 'sail' AS engine"

    _create_view(
        iceberg_spark,
        view_name,
        definition=definition,
        location=explicit_location,
        comment=comment,
        properties=properties,
    )

    row = _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}")
    assert row == {"id": 7, "engine": "sail"}

    metadata = _load_view_metadata(iceberg_rest_endpoint, view_name)
    assert metadata["location"] == explicit_location
    assert metadata["current-version-id"] == 1
    _assert_schema_id_resolved(metadata)
    _assert_spark_representation_present(metadata)

    view_properties = metadata.get("properties", {})
    assert view_properties.get("comment") == comment
    for k, v in properties.items():
        assert view_properties.get(k) == v


def test_sail_create_multiple_views_same_location_and_query_each(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    shared_location = f"s3://icebergdata/demo/{NAMESPACE}/shared/location/for/views"
    view_names = [f"same_loc_view_{i}" for i in range(1, 6)]

    for i, view_name in enumerate(view_names, start=1):
        _create_view(
            iceberg_spark,
            view_name,
            definition=f"SELECT {i} AS id",
            location=shared_location,
        )

    loaded_metadata = [_load_view_metadata(iceberg_rest_endpoint, view_name) for view_name in view_names]
    view_uuids = {metadata["view-uuid"] for metadata in loaded_metadata}
    assert len(view_uuids) == len(view_names)

    for i, metadata in enumerate(loaded_metadata, start=1):
        assert metadata["location"] == shared_location
        assert metadata["current-version-id"] == 1
        _assert_schema_id_resolved(metadata)
        _assert_spark_representation_present(metadata)

        view_name = view_names[i - 1]
        row = _collect_one_row(iceberg_spark, f"SELECT * FROM {NAMESPACE}.{view_name}")
        assert row == {"id": i}


def test_sail_show_views_lists_created_views(
    iceberg_spark: SparkSession,
) -> None:
    view_names = ["show_views_1", "show_views_2", "show_views_3"]
    for view_name in view_names:
        _create_view(iceberg_spark, view_name, definition="SELECT 1 AS id")

    rows = iceberg_spark.sql(f"SHOW VIEWS IN {NAMESPACE}").collect()
    listed = _extract_view_names(rows)
    for view_name in view_names:
        assert view_name in listed


def test_sail_drop_view_removes_rest_metadata_and_sail_visibility(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    view_name = "dropped_view"
    _create_view(iceberg_spark, view_name, definition="SELECT 1 AS id")
    _load_view_metadata(iceberg_rest_endpoint, view_name)

    iceberg_spark.sql(f"DROP VIEW {NAMESPACE}.{view_name}")
    _assert_view_missing(iceberg_rest_endpoint, view_name)

    rows = iceberg_spark.sql(f"SHOW VIEWS IN {NAMESPACE}").collect()
    listed = _extract_view_names(rows)
    assert view_name not in listed
