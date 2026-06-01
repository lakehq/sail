"""Pytest fixtures for Unity Catalog integration tests.

Uses PostgreSQL as the backend database and the Unity Catalog OSS Docker image.
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote, urlparse

import pytest
import requests
from pytest_bdd import given, parsers, then
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from pysail.testing.spark.steps.sql import PathWrapper
from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

DEFAULT_CATALOG = "sail_test_catalog"
UNITY_CATALOG_IMAGE = os.environ.get(
    "PYSAIL_UNITY_CATALOG_IMAGE",
    "unitycatalog/unitycatalog:v0.4.0",
)


def _exec_postgres_sql(
    container: DockerContainer,
    sql: str,
    description: str,
) -> str:
    command = [
        "psql",
        "-U",
        "test",
        "-d",
        "test",
        "-v",
        "ON_ERROR_STOP=1",
        "-tA",
    ]
    command.extend(["-c", sql])
    result = container.exec(command)
    output = result.output.decode("utf-8", errors="replace")
    if result.exit_code != 0:
        pytest.fail(f"{description}: {output}")
    return output.strip()


@pytest.fixture(scope="module")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for inter-container communication."""
    network = Network()
    network.create()
    yield network
    network.remove()


@pytest.fixture(scope="module")
def postgres_container(docker_network: Network) -> Generator[DockerContainer, None, None]:
    """Start a PostgreSQL container for Unity Catalog backend."""
    container = (
        DockerContainer("postgres:18.3")
        .with_env("POSTGRES_USER", "test")
        .with_env("POSTGRES_PASSWORD", "test")
        .with_env("POSTGRES_DB", "test")
        .with_network(docker_network)
        .with_network_aliases("postgres")
    )
    container.start()
    wait_for_logs(container, "database system is ready to accept connections", timeout=120)
    # Wait a bit for PostgreSQL to be fully ready
    time.sleep(2)
    delta_commits_sql = """
CREATE TABLE IF NOT EXISTS uc_delta_commits (
    id uuid PRIMARY KEY,
    table_id uuid NOT NULL,
    commit_version bigint NOT NULL,
    commit_filename varchar(255) NOT NULL,
    commit_filesize bigint NOT NULL,
    commit_file_modification_timestamp timestamp NOT NULL,
    commit_timestamp timestamp NOT NULL,
    is_backfilled_latest_commit boolean NOT NULL,
    CONSTRAINT uc_delta_commits_table_version UNIQUE (table_id, commit_version)
)
"""
    _exec_postgres_sql(
        container,
        delta_commits_sql,
        "failed to initialize Unity Delta commits table",
    )
    yield container
    container.stop()


@pytest.fixture(scope="module")
def unity_storage_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Host path mounted into Unity Catalog for managed table storage."""
    return tmp_path_factory.mktemp("unity_storage_root")


@pytest.fixture(scope="module")
def unity_container(
    docker_network: Network,
    postgres_container: DockerContainer,  # noqa: ARG001
    tmp_path_factory: pytest.TempPathFactory,
    unity_storage_root: Path,
) -> Generator[DockerContainer, None, None]:
    """Start a Unity Catalog container with PostgreSQL backend."""
    # Write hibernate config that points to the internal PostgreSQL hostname
    hibernate_config = (
        "hibernate.connection.driver_class=org.postgresql.Driver\n"
        "hibernate.connection.url=jdbc:postgresql://postgres:5432/test\n"
        "hibernate.connection.username=test\n"
        "hibernate.connection.password=test\n"
        "hibernate.hbm2ddl.auto=update\n"
    )
    tmp_dir = tmp_path_factory.mktemp("unity")
    hibernate_path = tmp_dir / "hibernate.properties"
    hibernate_path.write_text(hibernate_config)
    server_config = "server.env=dev\nserver.authorization=disable\nserver.managed-table.enabled=true\n"
    server_path = tmp_dir / "server.properties"
    server_path.write_text(server_config)

    container = (
        DockerContainer(UNITY_CATALOG_IMAGE)
        .with_exposed_ports(8080)
        .with_volume_mapping(str(hibernate_path), "/home/unitycatalog/etc/conf/hibernate.properties", "ro")
        .with_volume_mapping(str(server_path), "/home/unitycatalog/etc/conf/server.properties", "ro")
        .with_volume_mapping(str(unity_storage_root), str(unity_storage_root), "rw")
        .with_network(docker_network)
        .with_network_aliases("unity-catalog")
    )
    container.start()
    wait_for_logs(
        container,
        "###################################################################",
        timeout=120,
    )
    yield container
    container.stop()


@pytest.fixture(scope="module")
def unity_rest_url(unity_container: DockerContainer) -> str:
    """Host-accessible Unity Catalog REST API URL."""
    host = unity_container.get_container_host_ip()
    port = unity_container.get_exposed_port(8080)
    return f"http://{host}:{port}/api/2.1/unity-catalog"


@pytest.fixture(scope="module")
def _create_unity_catalog(unity_rest_url: str, unity_storage_root: Path) -> None:
    """Create the test catalog in Unity Catalog via REST API."""
    url = f"{unity_rest_url}/catalogs"
    payload = {
        "name": DEFAULT_CATALOG,
        "comment": "Main catalog for testing",
        "storage_root": str(unity_storage_root),
    }
    max_retries = 10
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code in (200, 201, 409):
                # 409 = already exists, that's fine
                return
            resp.raise_for_status()
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
        else:
            return


@pytest.fixture(scope="module")
def unity_spark(
    unity_rest_url: str,
    _create_unity_catalog: None,
    unity_storage_root: Path,
) -> Generator[SparkSession, None, None]:
    """Start Sail server with Unity catalog and create a Spark session."""
    catalog_config = f'[{{name="sail", type="unity", uri="{unity_rest_url}", default_catalog="{DEFAULT_CATALOG}"}}]'
    server, remote, saved_env = start_sail_server(
        catalog_list=catalog_config,
        extra_env={"UNITY_ALLOW_HTTP_URL": "true"},
    )
    spark = create_spark_session(remote, "unity_catalog_test")
    spark.conf.set(
        "spark.sql.warehouse.dir",
        str(unity_storage_root / "warehouse"),
    )
    yield spark
    with contextlib.suppress(Exception):
        spark.stop()
    stop_sail_server(server, saved_env)


def _qualified_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) == 3:  # noqa: PLR2004
        return table_name
    if len(parts) == 2:  # noqa: PLR2004
        return f"{DEFAULT_CATALOG}.{table_name}"
    pytest.fail(f"expected table name with schema.table or catalog.schema.table: {table_name}")


def _unity_table_info(unity_rest_url: str, table_name: str) -> dict:
    full_name = _qualified_table_name(table_name)
    response = requests.get(
        f"{unity_rest_url}/tables/{quote(full_name, safe='')}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def _unity_table_parts(table_name: str) -> tuple[str, str, str]:
    full_name = _qualified_table_name(table_name)
    catalog_name, schema_name, name = full_name.split(".", 2)
    return catalog_name, schema_name, name


def _location_to_path(location: str) -> Path:
    parsed = urlparse(location)
    if parsed.scheme == "file":
        return Path(parsed.path)
    if parsed.scheme:
        pytest.fail(f"expected a local table location, got: {location}")
    return Path(location)


def _write_seed_delta_log(location: Path, table_id: str) -> None:
    timestamp = int(time.time() * 1000)
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    actions = [
        {
            "commitInfo": {
                "timestamp": timestamp,
                "operation": "CREATE TABLE",
                "operationParameters": {},
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
                "engineInfo": "sail-unity-catalog-test",
                "txnId": str(uuid.uuid4()),
                "inCommitTimestamp": timestamp,
            }
        },
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["catalogManaged"],
                "writerFeatures": ["catalogManaged", "inCommitTimestamp"],
            }
        },
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema, separators=(",", ":")),
                "partitionColumns": [],
                "configuration": {
                    "delta.feature.catalogManaged": "supported",
                    "delta.enableInCommitTimestamps": "true",
                    "io.unitycatalog.tableId": table_id,
                },
                "createdTime": timestamp,
            }
        },
    ]
    log_dir = location / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    first_log = log_dir / "00000000000000000000.json"
    with first_log.open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action, separators=(",", ":")))
            f.write("\n")


def _unity_column(name: str, type_name: str, position: int) -> dict:
    type_text = type_name.lower()
    type_json = "integer" if type_name == "INT" else type_text
    return {
        "name": name,
        "type_name": type_name,
        "type_text": type_text,
        "type_json": json.dumps(type_json, separators=(",", ":")),
        "nullable": True,
        "position": position,
    }


@given(parsers.parse("Unity Catalog managed Delta table {table_name} exists with id and name columns"))
def unity_managed_delta_table_exists(table_name: str, unity_rest_url: str) -> None:
    catalog_name, schema_name, name = _unity_table_parts(table_name)
    staging_response = requests.post(
        f"{unity_rest_url}/staging-tables",
        json={
            "name": name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
        },
        timeout=10,
    )
    staging_response.raise_for_status()
    staging_table = staging_response.json()
    table_id = staging_table["id"]
    staging_location = staging_table["staging_location"]
    _write_seed_delta_log(_location_to_path(staging_location), table_id)

    create_response = requests.post(
        f"{unity_rest_url}/tables",
        json={
            "name": name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "storage_location": staging_location,
            "columns": [
                _unity_column("id", "INT", 0),
                _unity_column("name", "STRING", 1),
            ],
        },
        timeout=10,
    )
    create_response.raise_for_status()


@given(parsers.parse("final Unity Catalog managed table cleanup for {table_name}"))
def final_unity_managed_table_cleanup(
    table_name: str,
    unity_rest_url: str,
    postgres_container: DockerContainer,
) -> Generator[None, None, None]:
    yield
    try:
        table_info = _unity_table_info(unity_rest_url, table_name)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == requests.codes.not_found:
            return
        raise
    table_id = table_info.get("table_id")
    assert isinstance(table_id, str)
    table_uuid = str(uuid.UUID(table_id))
    sql = f"""
DELETE FROM uc_delta_commits WHERE table_id = '{table_uuid}'::uuid;
DELETE FROM uc_properties WHERE entity_id = '{table_uuid}'::uuid AND entity_type = 'table';
DELETE FROM uc_columns WHERE table_id = '{table_uuid}'::uuid;
DELETE FROM uc_tables WHERE id = '{table_uuid}'::uuid;
"""  # noqa: S608
    _exec_postgres_sql(
        postgres_container,
        sql,
        "failed to clean Unity managed table metadata",
    )


def _table_location(spark: SparkSession, table_name: str) -> str:
    rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    for row in rows:
        if str(row["col_name"]).strip() == "Location":
            location = str(row["data_type"]).strip()
            assert location, f"table {table_name} has an empty location"
            return location
    pytest.fail(f"Location row not found in DESCRIBE EXTENDED {table_name}")


def _first_delta_metadata(location: Path) -> dict:
    first_log = location / "_delta_log" / "00000000000000000000.json"
    assert first_log.exists(), f"first Delta log does not exist: {first_log}"
    with first_log.open(encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            metadata = obj.get("metaData")
            if isinstance(metadata, dict):
                return metadata
    pytest.fail(f"metaData action not found in {first_log}")


@given(parsers.parse("variable {name} for table {table_name}"), target_fixture="variables")
@given(parsers.parse("variable {name} for location of table {table_name}"), target_fixture="variables")
def variable_for_table_location(name: str, table_name: str, spark: SparkSession, variables: dict) -> dict:
    """Defines a variable for the storage location of a catalog table."""
    variables[name] = PathWrapper(_location_to_path(_table_location(spark, table_name)))
    return variables


@then(parsers.parse("Unity Catalog table {table_name} is a managed Delta table"))
def unity_table_is_managed_delta(table_name: str, unity_rest_url: str) -> None:
    table_info = _unity_table_info(unity_rest_url, table_name)
    assert table_info.get("table_type") == "MANAGED"
    assert table_info.get("data_source_format") == "DELTA"
    assert table_info.get("table_id")
    assert table_info.get("storage_location")


@then(parsers.parse("Unity Catalog table {table_name} table id matches Delta metadata in {location_var}"))
def unity_table_id_matches_delta_metadata(
    table_name: str,
    location_var: str,
    unity_rest_url: str,
    variables: dict,
) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    table_info = _unity_table_info(unity_rest_url, table_name)
    table_id = table_info.get("table_id")
    assert isinstance(table_id, str)
    assert table_id

    metadata = _first_delta_metadata(Path(location.path))
    configuration = metadata.get("configuration")
    assert isinstance(configuration, dict)
    assert configuration.get("io.unitycatalog.tableId") == table_id


@then(parsers.parse("staged Delta commit for version {version:d} exists in {location_var}"))
def staged_delta_commit_exists(version: int, location_var: str, variables: dict) -> None:
    location = variables.get(location_var)
    assert location is not None, f"Variable {location_var!r} not found"

    log_dir = Path(location.path) / "_delta_log"
    published = log_dir / f"{version:020}.json"
    staged_dir = log_dir / "_staged_commits"
    staged = list(staged_dir.glob(f"{version:020}.*.json"))
    assert published.exists(), f"published Delta commit does not exist: {published}"
    assert staged, f"staged Delta commit for version {version} does not exist in {staged_dir}"


@then(parsers.parse("Unity Catalog Delta commit for table {table_name} version {version:d} exists"))
def unity_delta_commit_exists(
    table_name: str,
    version: int,
    unity_rest_url: str,
    postgres_container: DockerContainer,
) -> None:
    table_info = _unity_table_info(unity_rest_url, table_name)
    table_id = table_info.get("table_id")
    assert isinstance(table_id, str)
    table_uuid = str(uuid.UUID(table_id))
    sql = (
        "SELECT count(*) FROM uc_delta_commits "  # noqa: S608
        f"WHERE table_id = '{table_uuid}'::uuid AND commit_version = {version}"
    )
    count = _exec_postgres_sql(
        postgres_container,
        sql,
        "failed to query Unity Delta commits table",
    )
    assert count == "1"
