from __future__ import annotations

import json
import re
import socket
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import boto3
import pytest
import requests
from botocore.config import Config
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFileContent, read_manifest_list
from pyiceberg.table import StaticTable
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_commit_test"
HTTP_CONFLICT = 409
UNPARTITIONED_LAST_PARTITION_ID = 999
UUID_METADATA_FILE_PATTERN = re.compile(
    r"^\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$"
)


class _CommitGate:
    def __init__(self) -> None:
        self.arrived = threading.Event()
        self.release = threading.Event()
        self.completed = threading.Event()
        self.request: dict[str, object] | None = None
        self.response_status: int | None = None
        self.response: object = None
        self._target: tuple[list[str], str] | None = None
        self._claimed = False
        self._lock = threading.Lock()
        self.snapshot_only = True
        self.response_fault: int | str | None = None
        self.apply_commit = True
        self.response_payload: dict | None = None
        self.request_count = 0
        self.table_hints: dict | None = None
        self.table_posts = 0
        self.refresh_status: int | None = None

    def arm(self, namespace: str, table: str, *, snapshot_only: bool = True) -> None:
        with self._lock:
            self._target = ([namespace], table)
            self.snapshot_only = snapshot_only

    def claim(self, method: str, body: bytes) -> dict[str, object] | None:
        if method != "POST":
            return None
        try:
            payload = json.loads(body)
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        identifier = payload.get("identifier")
        requirements = payload.get("requirements")
        if not isinstance(identifier, dict) or not isinstance(requirements, list):
            return None
        has_snapshot_requirement = any(
            isinstance(requirement, dict) and requirement.get("type") == "assert-ref-snapshot-id"
            for requirement in requirements
        )
        with self._lock:
            if (
                self._target is None
                or (self.snapshot_only and not has_snapshot_requirement)
                or identifier.get("namespace") != self._target[0]
                or identifier.get("name") != self._target[1]
            ):
                return None
            self.request_count += 1
            if self._claimed:
                return None
            self._claimed = True
            self.request = payload
            return payload

    def record_response(self, status: int, payload: object) -> None:
        self.response_status = status
        self.response = payload
        self.completed.set()


def _commit_gate_handler(
    upstream: str,
    gate: _CommitGate,
) -> type[BaseHTTPRequestHandler]:
    class CommitGateHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self._forward()

        def do_HEAD(self) -> None:
            self._forward()

        def do_POST(self) -> None:
            self._forward()

        def do_DELETE(self) -> None:
            self._forward()

        def _forward(self) -> None:
            if self.command == "GET" and "/tables/" in self.path and gate.completed.is_set() and gate.refresh_status:
                self.send_response(gate.refresh_status)
                self.send_header("content-length", "0")
                self.end_headers()
                return
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length) if length else b""
            if self.command == "POST" and "/tables/" in self.path:
                gate.table_posts += 1
            gated_request = gate.claim(self.command, body)
            if gated_request is not None:
                gate.arrived.set()
                if not gate.release.wait(timeout=60):
                    payload = {
                        "error": {
                            "message": "commit gate timed out",
                            "type": "CommitGateTimeout",
                            "code": 504,
                        }
                    }
                    encoded = json.dumps(payload).encode()
                    gate.record_response(504, payload)
                    self.send_response(504)
                    self.send_header("content-type", "application/json")
                    self.send_header("content-length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                    return
                if gate.response_fault is not None and not gate.apply_commit:
                    self._send_fault()
                    gate.completed.set()
                    return

            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower()
                not in {
                    "accept-encoding",
                    "connection",
                    "content-length",
                    "host",
                    "transfer-encoding",
                }
            }
            response = requests.request(
                self.command,
                f"{upstream}{self.path}",
                data=body if length else None,
                headers=headers,
                allow_redirects=False,
                timeout=30,
            )
            if gated_request is not None:
                try:
                    response_payload: object = response.json()
                except requests.JSONDecodeError:
                    response_payload = response.text
                gate.record_response(response.status_code, response_payload)
                if gate.response_fault is not None:
                    if gate.response_fault == "wrong_uuid":
                        gate.response_payload = response.json()
                        gate.response_payload["metadata"]["table-uuid"] = "00000000-0000-0000-0000-000000000000"
                    elif gate.response_fault == "unapplied_removals":
                        gate.response_payload["metadata-location"] = response.json()["metadata-location"]
                    self._send_fault()
                    return

            content = response.content
            if (
                self.command == "GET"
                and "/tables/" in self.path
                and response.status_code == HTTPStatus.OK
                and gate.table_hints
            ):
                payload = response.json()
                payload.pop("config", None)
                payload.pop("storage-credentials", None)
                payload.update(gate.table_hints)
                content = json.dumps(payload).encode()
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in {
                    "connection",
                    "content-encoding",
                    "content-length",
                    "transfer-encoding",
                }:
                    self.send_header(key, value)
            self.send_header("content-length", str(len(content)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(content)

        def _send_fault(self) -> None:
            if gate.response_fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            status = gate.response_fault if isinstance(gate.response_fault, int) else 200
            body = (
                json.dumps(gate.response_payload).encode()
                if gate.response_payload is not None
                else b"invalid response body"
            )
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return CommitGateHandler


@pytest.fixture(scope="module", autouse=True)
def namespace(spark: SparkSession) -> Generator[None, None, None]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


@pytest.fixture
def commit_gate_proxy(
    iceberg_rest_endpoint: str,
) -> Generator[tuple[str, _CommitGate], None, None]:
    gate = _CommitGate()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _commit_gate_handler(iceberg_rest_endpoint, gate),
    )
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}", gate
    finally:
        gate.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.fixture
def gated_remote(
    commit_gate_proxy: tuple[str, _CommitGate],
    seaweedfs_host_endpoint: str,
) -> Generator[str, None, None]:
    proxy_endpoint, _ = commit_gate_proxy
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{proxy_endpoint}"}}]'
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": catalog_config,
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        },
    ) as server:
        yield server.remote


def _load_table(iceberg_rest_endpoint: str, table_name: str) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(table_name, safe="")
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/tables/{table}"
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _s3_object_keys(endpoint: str, location: str) -> set[str]:
    parsed = urllib.parse.urlparse(location)
    assert parsed.scheme == "s3"
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="admin",
        aws_secret_access_key="password",  # noqa: S106
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc, Prefix=prefix)
    return {item["Key"] for page in pages for item in page.get("Contents", [])}


def _assert_uuid_metadata_location(metadata_location: str, expected_version: int | None = None) -> None:
    filename = PurePosixPath(metadata_location).name
    assert UUID_METADATA_FILE_PATTERN.match(filename), filename
    if expected_version is not None:
        assert filename.startswith(f"{expected_version:05}-"), filename


def _current_schema_field_names(metadata: dict) -> list[str]:
    current_schema_id = metadata["current-schema-id"]
    current_schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == current_schema_id)
    return [field["name"] for field in current_schema["fields"]]


def _current_snapshot(metadata: dict) -> dict:
    current_snapshot_id = metadata["current-snapshot-id"]
    return next(snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] == current_snapshot_id)


def _assert_row_level_commit_metadata(
    metadata: dict,
    *,
    previous_metadata_location: str,
    operation: str,
) -> dict:
    snapshot = _current_snapshot(metadata)
    assert snapshot["summary"]["operation"] == operation
    assert metadata["last-sequence-number"] == snapshot["sequence-number"]
    assert metadata["last-partition-id"] == UNPARTITIONED_LAST_PARTITION_ID
    assert metadata["refs"]["main"]["snapshot-id"] == snapshot["snapshot-id"]
    assert metadata["snapshot-log"][-1]["snapshot-id"] == snapshot["snapshot-id"]
    assert metadata["metadata-log"][-1]["metadata-file"] == previous_metadata_location
    return snapshot


def test_ctas_records_rest_catalog_metadata_location(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "ctas_t"
    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")
    spark.sql(
        f"""
        CREATE TABLE {NAMESPACE}.{table_name}
        USING iceberg
        AS SELECT 1 AS id, 'a' AS name
        """
    )

    table = _load_table(iceberg_rest_endpoint, table_name)
    metadata_location = table["metadata-location"]
    assert metadata_location
    _assert_uuid_metadata_location(metadata_location)
    assert table["metadata"]["current-snapshot-id"] is not None

    rows = spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name}").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a")]


def test_insert_advances_rest_catalog_metadata_location(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "commit_t"
    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")
    spark.sql(
        f"""
        CREATE TABLE {NAMESPACE}.{table_name} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    _assert_uuid_metadata_location(before_location, 0)
    assert before["metadata"]["current-snapshot-id"] == -1
    assert before["metadata"]["snapshots"] == []
    rows = spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name}").collect()  # noqa: S608
    assert rows == []

    spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (1, 'a'), (2, 'b')")  # noqa: S608

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]

    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 1)
    assert after["metadata"]["current-snapshot-id"] is not None
    assert len(after["metadata"]["metadata-log"]) == 1
    assert after["metadata"]["metadata-log"][0]["metadata-file"] == before_location

    spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (3, 'c')")  # noqa: S608
    appended = _load_table(iceberg_rest_endpoint, table_name)
    assert appended["metadata-location"] != after_location
    _assert_uuid_metadata_location(appended["metadata-location"], 2)
    assert [entry["metadata-file"] for entry in appended["metadata"]["metadata-log"]] == [
        before_location,
        after_location,
    ]

    rows = spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]


@pytest.mark.parametrize("read_api", ["table", "load"])
def test_metadata_tables_follow_rest_catalog_pointer(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    read_api: str,
) -> None:
    table_name = f"metadata_tables_{read_api}_t"
    table_fqn = f"sail.{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")  # noqa: S608
    spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b')")  # noqa: S608

    table = _load_table(iceberg_rest_endpoint, table_name)
    metadata = table["metadata"]
    read = spark.table if read_api == "table" else spark.read.format("iceberg").load
    assert [tuple(row) for row in read(table_fqn).orderBy("id").collect()] == [(1, "a"), (2, "b")]
    expected_snapshots = sorted(
        metadata["snapshots"],
        key=lambda snapshot: (snapshot["timestamp-ms"], snapshot["snapshot-id"]),
    )
    snapshots = (
        read(f"{table_fqn}.snapshots")
        .orderBy("committed_at", "snapshot_id")
        .select("snapshot_id", "parent_id", "operation")
        .collect()
    )
    assert [(row.snapshot_id, row.parent_id, row.operation) for row in snapshots] == [
        (
            snapshot["snapshot-id"],
            snapshot.get("parent-snapshot-id"),
            snapshot["summary"]["operation"],
        )
        for snapshot in expected_snapshots
    ]

    references = read(f"{table_fqn}.refs").select("name", "type", "snapshot_id").collect()
    main_reference = metadata["refs"]["main"]
    assert [(row.name, row.type, row.snapshot_id) for row in references] == [
        ("main", main_reference["type"].upper(), main_reference["snapshot-id"])
    ]

    metadata_log = read(f"{table_fqn}.metadata_log_entries").select("file", "latest_snapshot_id").collect()
    expected_metadata_files = {entry["metadata-file"] for entry in metadata["metadata-log"]} | {
        table["metadata-location"]
    }
    assert {row.file for row in metadata_log} == expected_metadata_files
    current_entry = next(row for row in metadata_log if row.file == table["metadata-location"])
    assert current_entry.latest_snapshot_id == metadata["current-snapshot-id"]


def test_snapshot_procedure_commits_through_rest_catalog(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "snapshot_procedure_t"
    table_fqn = f"sail.{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
    spark.sql(f"INSERT INTO {table_fqn} VALUES (2)")  # noqa: S608

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_metadata = before["metadata"]
    before_location = before["metadata-location"]
    snapshots = sorted(
        before_metadata["snapshots"],
        key=lambda snapshot: (snapshot["timestamp-ms"], snapshot["snapshot-id"]),
    )
    target_snapshot_id = snapshots[0]["snapshot-id"]
    previous_snapshot_id = before_metadata["current-snapshot-id"]

    result = spark.sql(
        f"""
        CALL sail.system.rollback_to_snapshot(
          table => '{table_fqn}',
          snapshot_id => {target_snapshot_id}
        )
        """
    ).first()
    assert result.previous_snapshot_id == previous_snapshot_id
    assert result.current_snapshot_id == target_snapshot_id

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_metadata = after["metadata"]
    assert after["metadata-location"] != before_location
    _assert_uuid_metadata_location(after["metadata-location"], 3)
    assert after_metadata["current-snapshot-id"] == target_snapshot_id
    assert after_metadata["refs"]["main"]["snapshot-id"] == target_snapshot_id
    assert after_metadata["metadata-log"][-1]["metadata-file"] == before_location
    assert {snapshot["snapshot-id"] for snapshot in after_metadata["snapshots"]} == {
        snapshot["snapshot-id"] for snapshot in snapshots
    }

    rows = spark.sql(f"SELECT id FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [row.id for row in rows] == [1]


def test_rest_catalog_write_honors_absolute_data_path(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
) -> None:
    table_name = "absolute_data_path_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    data_location = f"s3://icebergdata/managed-data/{NAMESPACE}/{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        TBLPROPERTIES ('write.data.path' = '{data_location}')
        """
    )

    created = _load_table(iceberg_rest_endpoint, table_name)
    assert created["metadata"]["properties"]["write.data.path"] == data_location

    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'one')")  # noqa: S608

    committed = _load_table(iceberg_rest_endpoint, table_name)
    assert committed["metadata-location"] != created["metadata-location"]
    assert committed["metadata"]["current-snapshot-id"] not in (None, -1)
    table_location = committed["metadata"]["location"]
    assert table_location != data_location
    assert committed["metadata"]["properties"]["write.data.path"] == data_location

    data_keys = _s3_object_keys(seaweedfs_host_endpoint, data_location)
    assert any(key.endswith(".parquet") for key in data_keys)
    table_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location)
    assert not any(key.endswith(".parquet") for key in table_keys)

    rows = spark.sql(f"SELECT id, name FROM {table_fqn}").collect()  # noqa: S608
    assert [(row.id, row.name) for row in rows] == [(1, "one")]


def test_merge_schema_append_advances_rest_catalog_metadata_location(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "merge_schema_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")  # noqa: S608
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    _assert_uuid_metadata_location(before_location, 1)

    evolved = spark.createDataFrame([(2, "b", 20)], schema="id INT, name STRING, age INT")
    (evolved.write.format("iceberg").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]
    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 2)
    assert after["metadata"]["metadata-log"][-1]["metadata-file"] == before_location
    assert _current_schema_field_names(after["metadata"]) == ["id", "name", "age"]

    rows = spark.sql(f"SELECT id, name, age FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"], row["age"]) for row in rows] == [(1, "a", None), (2, "b", 20)]


def test_insert_overwrite_advances_rest_catalog_metadata_location(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "overwrite_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )
    created = _load_table(iceberg_rest_endpoint, table_name)
    created_location = created["metadata-location"]
    _assert_uuid_metadata_location(created_location, 0)

    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old'), (2, 'old')")  # noqa: S608
    before_overwrite = _load_table(iceberg_rest_endpoint, table_name)
    before_overwrite_location = before_overwrite["metadata-location"]
    _assert_uuid_metadata_location(before_overwrite_location, 1)

    spark.sql(f"INSERT OVERWRITE TABLE {table_fqn} VALUES (3, 'new'), (4, 'new')")  # noqa: S608
    after_overwrite = _load_table(iceberg_rest_endpoint, table_name)
    after_overwrite_location = after_overwrite["metadata-location"]
    assert after_overwrite_location != before_overwrite_location
    _assert_uuid_metadata_location(after_overwrite_location, 2)
    assert [entry["metadata-file"] for entry in after_overwrite["metadata"]["metadata-log"]] == [
        created_location,
        before_overwrite_location,
    ]
    assert after_overwrite["metadata"]["snapshots"][-1]["summary"]["operation"] == "overwrite"

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(3, "new"), (4, "new")]


@pytest.mark.parametrize("write_mode", ["overwrite", "append"])
@pytest.mark.parametrize("initial_rows", [0, 3], ids=["empty", "populated"])
def test_concurrent_append_during_catalog_commit(
    spark: SparkSession,
    gated_remote: str,
    commit_gate_proxy: tuple[str, _CommitGate],
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
    write_mode: str,
    initial_rows: int,
) -> None:
    table_name = f"concurrent_{write_mode}_{initial_rows}_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT, value INT) USING iceberg")
    if initial_rows:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 10), (2, 20), (3, 30)")  # noqa: S608
    before = _load_table(iceberg_rest_endpoint, table_name)
    base_snapshot_id = before["metadata"].get("current-snapshot-id")
    if base_snapshot_id is not None and base_snapshot_id < 0:
        base_snapshot_id = None

    _, gate = commit_gate_proxy
    with spark_session_factory(gated_remote) as sessions:
        slow = sessions.create()
        gate.arm(NAMESPACE, table_name)

        def write() -> None:
            if write_mode == "overwrite":
                slow.sql(f"INSERT OVERWRITE {table_fqn} SELECT id, value * 2 FROM {table_fqn}").collect()  # noqa: S608
            else:
                slow.sql(f"INSERT INTO {table_fqn} VALUES (5, 50)").collect()  # noqa: S608

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(write)
            try:
                if not gate.arrived.wait(timeout=60):
                    outcome = future.exception(timeout=1) if future.done() else "still running"
                    pytest.fail(f"Write did not reach the catalog commit gate: {outcome}")
                spark.sql(f"INSERT INTO {table_fqn} VALUES (4, 40)")  # noqa: S608
                fast_commit = _load_table(iceberg_rest_endpoint, table_name)
            finally:
                gate.release.set()

            if write_mode == "overwrite":
                with pytest.raises(Exception, match="expected snapshot"):
                    future.result(timeout=60)
            else:
                future.result(timeout=60)

    assert gate.completed.wait(timeout=10)
    assert gate.response_status == HTTP_CONFLICT
    assert gate.request is not None
    snapshot_requirements = [
        requirement for requirement in gate.request["requirements"] if requirement["type"] == "assert-ref-snapshot-id"
    ]
    assert snapshot_requirements
    assert {requirement.get("snapshot-id") for requirement in snapshot_requirements} == {base_snapshot_id}

    after = _load_table(iceberg_rest_endpoint, table_name)
    if write_mode == "overwrite":
        assert after["metadata-location"] == fast_commit["metadata-location"]
        assert after["metadata"] == fast_commit["metadata"]
    else:
        assert after["metadata-location"] != fast_commit["metadata-location"]
        snapshot = _current_snapshot(after["metadata"])
        assert snapshot["parent-snapshot-id"] == fast_commit["metadata"]["current-snapshot-id"]
        assert snapshot["summary"]["operation"] == "append"

    expected = [(i, i * 10) for i in range(1, initial_rows + 1)] + [(4, 40)]
    if write_mode == "append":
        expected.append((5, 50))
    rows = spark.sql(f"SELECT id, value FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.value) for row in rows] == expected
    external_table = StaticTable.from_metadata(
        after["metadata-location"],
        properties={
            "s3.endpoint": seaweedfs_host_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )
    external_rows = external_table.scan().to_arrow().to_pylist()
    assert sorted((row["id"], row["value"]) for row in external_rows) == expected


def test_delete_advances_rest_catalog_metadata_location_with_position_delete(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
) -> None:
    table_name = "delete_t"
    spark.sql("DROP TABLE IF EXISTS iceberg_commit_test.delete_t")
    spark.sql(
        """
        CREATE TABLE iceberg_commit_test.delete_t (
          id INT,
          name STRING,
          flag STRING
        )
        USING iceberg
        TBLPROPERTIES (
          'format-version' = '2',
          'write.delete.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO iceberg_commit_test.delete_t
        SELECT * FROM VALUES
          (1, 'keep-a', 'keep'),
          (2, 'drop-b', 'drop'),
          (3, 'keep-c', 'keep')
        """
    )
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    _assert_uuid_metadata_location(before_location, 1)

    spark.sql("DELETE FROM iceberg_commit_test.delete_t WHERE flag = 'drop'")

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]
    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 2)
    snapshot = _assert_row_level_commit_metadata(
        after["metadata"],
        previous_metadata_location=before_location,
        operation="delete",
    )
    summary = snapshot["summary"]
    assert summary["added-delete-files"] == "1"
    assert summary["added-position-delete-files"] == "1"
    assert summary["added-position-deletes"] == "1"
    assert "deleted-records" not in summary
    assert "added-equality-delete-files" not in summary
    assert "added-equality-deletes" not in summary
    assert summary["total-data-files"] == "1"
    assert summary["total-delete-files"] == "1"
    assert summary["total-position-deletes"] == "1"
    assert summary["total-equality-deletes"] == "0"
    assert summary["total-records"] == "3"

    expected_rows = [(1, "keep-a", "keep"), (3, "keep-c", "keep")]
    rows = spark.sql("SELECT id, name, flag FROM iceberg_commit_test.delete_t ORDER BY id").collect()
    assert [(row["id"], row["name"], row["flag"]) for row in rows] == expected_rows

    table = StaticTable.from_metadata(
        after_location,
        properties={
            "s3.endpoint": seaweedfs_host_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )
    (task,) = table.scan().plan_files()
    (delete_file,) = task.delete_files
    assert delete_file.content == DataFileContent.POSITION_DELETES
    assert delete_file.record_count == 1
    assert not delete_file.equality_ids
    rows = table.scan().to_arrow().sort_by([("id", "ascending")]).to_pylist()
    assert [(row["id"], row["name"], row["flag"]) for row in rows] == expected_rows


def test_merge_advances_rest_catalog_metadata_location_with_position_delete(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "merge_t"
    spark.sql("DROP TABLE IF EXISTS iceberg_commit_test.merge_t")
    spark.sql(
        """
        CREATE TABLE iceberg_commit_test.merge_t (
          id INT,
          name STRING,
          flag STRING
        )
        USING iceberg
        TBLPROPERTIES (
          'format-version' = '2',
          'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO iceberg_commit_test.merge_t
        SELECT * FROM VALUES
          (1, 'keep-a', 'keep'),
          (2, 'old-b', 'update'),
          (3, 'drop-c', 'delete'),
          (5, 'old-e', 'expire'),
          (6, 'drop-f', 'purge')
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW iceberg_rest_merge_source AS
        SELECT * FROM VALUES
          (2, 'new-b', 'insert'),
          (3, 'ignored-c', 'delete'),
          (4, 'new-d', 'insert')
        AS src(id, name, flag)
        """
    )
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    _assert_uuid_metadata_location(before_location, 1)
    previous_data_file_count = int(_current_snapshot(before["metadata"])["summary"]["total-data-files"])
    assert previous_data_file_count == 1

    spark.sql(
        """
        MERGE INTO iceberg_commit_test.merge_t AS t
        USING iceberg_rest_merge_source AS s
        ON t.id = s.id
        WHEN MATCHED AND t.flag = 'update' THEN
          UPDATE SET name = s.name
        WHEN MATCHED AND t.flag = 'delete' THEN
          DELETE
        WHEN NOT MATCHED THEN
          INSERT (id, name, flag) VALUES (s.id, s.name, s.flag)
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'expire' THEN
          UPDATE SET name = 'expired-e'
        WHEN NOT MATCHED BY SOURCE AND t.flag = 'purge' THEN
          DELETE
        """
    )

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]
    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 2)
    snapshot = _assert_row_level_commit_metadata(
        after["metadata"],
        previous_metadata_location=before_location,
        operation="overwrite",
    )
    summary = snapshot["summary"]
    assert summary["added-delete-files"] == "1"
    assert summary["added-position-delete-files"] == "1"
    assert summary["added-position-deletes"] == "4"
    assert "deleted-records" not in summary
    added_data_file_count = int(summary["added-data-files"])
    assert added_data_file_count > 0
    assert summary["added-records"] == "3"
    assert int(summary["total-data-files"]) == previous_data_file_count + added_data_file_count
    assert summary["total-delete-files"] == "1"
    assert summary["total-position-deletes"] == "4"
    assert summary["total-records"] == "8"

    rows = spark.sql("SELECT id, name, flag FROM iceberg_commit_test.merge_t ORDER BY id").collect()
    assert [(row["id"], row["name"], row["flag"]) for row in rows] == [
        (1, "keep-a", "keep"),
        (2, "new-b", "update"),
        (4, "new-d", "insert"),
        (5, "expired-e", "expire"),
    ]


@pytest.mark.parametrize("operation", ["merge", "rewrite"])
def test_stale_write_catalog_conflict_cleans_only_commit_owned_artifacts(
    spark: SparkSession,
    gated_remote: str,
    commit_gate_proxy: tuple[str, _CommitGate],
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
    operation: str,
) -> None:
    table_name = f"stale_{operation}_conflict_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        TBLPROPERTIES ('write.merge.mode' = 'merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'base')")  # noqa: S608

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_metadata = before["metadata"]
    before_snapshot_id = before_metadata["current-snapshot-id"]
    table_location = before_metadata["location"]
    before_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location)

    _, gate = commit_gate_proxy
    with spark_session_factory(gated_remote) as sessions:
        slow = sessions.create()
        slow.conf.set("spark.sql.shuffle.partitions", "1")
        slow.range(1_000).selectExpr(
            "CAST(id + 1000 AS INT) AS id",
            "'slow' AS name",
        ).createOrReplaceTempView("stale_merge_source")
        gate.arm(NAMESPACE, table_name)

        def stale_write() -> None:
            if operation == "rewrite":
                slow.sql(
                    f"""
                    CALL system.rewrite_data_files(
                      '{table_fqn}',
                      options => map('rewrite-all', 'true', 'max-concurrent-file-group-rewrites', '1'))
                    """
                ).collect()
                return
            slow.sql(
                f"""
                MERGE INTO {table_fqn} AS t
                USING stale_merge_source AS s
                ON t.id = s.id
                WHEN NOT MATCHED THEN
                  INSERT (id, name) VALUES (s.id, s.name)
                """  # noqa: S608
            ).collect()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(stale_write)
            try:
                if not gate.arrived.wait(timeout=60):
                    outcome = future.exception(timeout=1) if future.done() else "still running"
                    pytest.fail(f"{operation} did not reach the catalog commit gate: {outcome}")

                blocked_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location)
                slow_created_keys = blocked_keys - before_keys
                slow_data_keys = {key for key in slow_created_keys if key.endswith(".parquet")}
                slow_commit_keys = slow_created_keys - slow_data_keys
                assert slow_data_keys
                assert any("/metadata/manifest-" in key for key in slow_commit_keys)
                assert any("/metadata/snap-" in key for key in slow_commit_keys)

                spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'fast')")  # noqa: S608
                fast_commit = _load_table(iceberg_rest_endpoint, table_name)
                fast_commit_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location) - slow_created_keys
            finally:
                gate.release.set()

            with pytest.raises(Exception, match="expected snapshot"):
                future.result(timeout=60)

    assert gate.completed.wait(timeout=10)
    assert gate.response_status == HTTP_CONFLICT
    assert isinstance(gate.response, dict)
    provider_error = gate.response["error"]
    assert provider_error["type"] == "CommitFailedException"
    assert provider_error["code"] == HTTP_CONFLICT

    assert gate.request is not None
    snapshot_requirements = [
        requirement for requirement in gate.request["requirements"] if requirement["type"] == "assert-ref-snapshot-id"
    ]
    assert snapshot_requirements
    assert {requirement["snapshot-id"] for requirement in snapshot_requirements} == {before_snapshot_id}
    proposed_snapshot = next(
        update["snapshot"] for update in gate.request["updates"] if update["action"] == "add-snapshot"
    )

    cleanup_deadline = time.monotonic() + 10
    while True:
        after_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location)
        after_data_keys = {key for key in after_keys if key.endswith(".parquet")}
        remaining_commit_keys = after_keys - after_data_keys - fast_commit_keys
        if not remaining_commit_keys or time.monotonic() >= cleanup_deadline:
            break
        time.sleep(0.05)
    assert not remaining_commit_keys
    # Writer output can be reused by commit retries and is not owned by this commit attempt.
    assert slow_data_keys <= after_keys
    assert before_keys <= fast_commit_keys <= after_keys

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_metadata = after["metadata"]
    after_snapshot = _current_snapshot(after_metadata)
    assert after["metadata-location"] == fast_commit["metadata-location"]
    assert after_metadata["metadata-log"][-1]["metadata-file"] == before["metadata-location"]
    assert after_snapshot["parent-snapshot-id"] == before_snapshot_id
    assert proposed_snapshot["snapshot-id"] not in {snapshot["snapshot-id"] for snapshot in after_metadata["snapshots"]}

    file_io = PyArrowFileIO(
        {
            "s3.endpoint": seaweedfs_host_endpoint,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        }
    )
    committed_data_keys = {
        urllib.parse.urlparse(entry.data_file.file_path).path.lstrip("/")
        for manifest in read_manifest_list(file_io.new_input(after_snapshot["manifest-list"]))
        for entry in manifest.fetch_manifest_entry(file_io)
    }
    assert committed_data_keys.isdisjoint(slow_data_keys)
    assert len(committed_data_keys) == int(after_snapshot["summary"]["total-data-files"])
    assert committed_data_keys == {key for key in fast_commit_keys if key.endswith(".parquet")}

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.name) for row in rows] == [(1, "base"), (2, "fast")]


def test_rest_catalog_commits_catalog_managed_iceberg_alter(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "alter_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT
        )
        USING iceberg
        """
    )
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]

    spark.sql(
        f"""
        ALTER TABLE {table_fqn}
        SET TBLPROPERTIES ('owner' = 'alice')
        """
    )

    after = _load_table(iceberg_rest_endpoint, table_name)
    assert after["metadata-location"] != before_location
    assert after["metadata"]["table-uuid"] == before["metadata"]["table-uuid"]
    assert after["metadata"]["properties"]["owner"] == "alice"
    assert after["metadata"]["schemas"] == before["metadata"]["schemas"]


@pytest.mark.parametrize("populated", [False, True], ids=["empty", "populated"])
@pytest.mark.parametrize("apply_commit", [False, True], ids=["not_applied", "applied"])
@pytest.mark.parametrize("fault", [503, "disconnect", "malformed"])
def test_unknown_append_preserves_files_without_retrying(
    spark,
    gated_remote,
    commit_gate_proxy,
    iceberg_rest_endpoint,
    seaweedfs_host_endpoint,
    populated,
    apply_commit,
    fault,
    request,
):
    table_name = "unknown_append_" + request.node.callspec.id.replace("-", "_")
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        if populated:
            spark.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        location = before["metadata"]["location"]
        before_keys = _s3_object_keys(seaweedfs_host_endpoint, location)
        _, gate = commit_gate_proxy
        gate.response_fault = fault
        gate.apply_commit = apply_commit
        gate.arm(NAMESPACE, table_name, snapshot_only=False)
        gate.release.set()
        with spark_session_factory(gated_remote) as sessions:
            writer = sessions.create()
            with pytest.raises(PySparkException, match="commit state unknown"):
                writer.sql(f"INSERT INTO {table_fqn} SELECT CAST(id AS INT) FROM range(2, 4, 1, 2)").collect()  # noqa: S608

        assert gate.request_count == 1
        assert gate.request is not None
        after = _load_table(iceberg_rest_endpoint, table_name)
        if apply_commit:
            snapshot = next(
                update["snapshot"] for update in gate.request["updates"] if update["action"] == "add-snapshot"
            )
            assert after["metadata"]["current-snapshot-id"] == snapshot["snapshot-id"]
            assert len(after["metadata"]["snapshots"]) == len(before["metadata"].get("snapshots", [])) + 1
        else:
            assert after["metadata-location"] == before["metadata-location"]

        after_keys = _s3_object_keys(seaweedfs_host_endpoint, location)
        assert before_keys <= after_keys
        new_keys = after_keys - before_keys
        assert any(key.endswith(".parquet") for key in new_keys)
        assert any(key.endswith(".avro") for key in new_keys)
        expected = ([1] if populated else []) + ([2, 3] if apply_commit else [])
        assert [row.id for row in spark.table(table_fqn).orderBy("id").collect()] == expected
        reference = StaticTable.from_metadata(
            after["metadata-location"],
            properties={
                "s3.endpoint": seaweedfs_host_endpoint,
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
            },
        )
        assert sorted(reference.scan().to_arrow().column("id").to_pylist()) == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("apply_commit", [False, True], ids=["not_applied", "applied"])
@pytest.mark.parametrize("fault", [503, "disconnect", "malformed"])
def test_unknown_rewrite_preserves_current_and_historical_files(
    spark,
    gated_remote,
    commit_gate_proxy,
    iceberg_rest_endpoint,
    seaweedfs_host_endpoint,
    version,
    apply_commit,
    fault,
    request,
):
    table_name = "unknown_rewrite_" + request.node.callspec.id.replace("-", "_")
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(
        f"CREATE TABLE {table_fqn} (id INT) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read')"
    )
    properties = {
        "s3.endpoint": seaweedfs_host_endpoint,
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }
    file_io = PyArrowFileIO(properties)
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1), (2)")  # noqa: S608
        spark.sql(f"INSERT INTO {table_fqn} VALUES (3), (4)")  # noqa: S608
        original_snapshot = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"DELETE FROM {table_fqn} WHERE id = 1")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        location = before["metadata"]["location"]
        bucket = urllib.parse.urlparse(location).netloc
        before_keys = _s3_object_keys(seaweedfs_host_endpoint, location)
        original_bytes = {}
        for key in before_keys:
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                original_bytes[key] = source.read()
        _, gate = commit_gate_proxy
        gate.response_fault = fault
        gate.apply_commit = apply_commit
        gate.arm(NAMESPACE, table_name)
        gate.release.set()
        with spark_session_factory(gated_remote) as sessions:
            writer = sessions.create()
            with pytest.raises(PySparkException, match="commit state unknown"):
                writer.sql(
                    f"CALL system.rewrite_data_files('{table_fqn}', options => map('rewrite-all', 'true'))"
                ).collect()

        assert gate.request_count == 1
        assert gate.request is not None
        proposed = next(update["snapshot"] for update in gate.request["updates"] if update["action"] == "add-snapshot")
        assert proposed["summary"]["operation"] == "replace"
        after = _load_table(iceberg_rest_endpoint, table_name)
        if apply_commit:
            assert after["metadata"]["current-snapshot-id"] == proposed["snapshot-id"]
            assert len(after["metadata"]["snapshots"]) == len(before["metadata"]["snapshots"]) + 1
        else:
            assert after["metadata-location"] == before["metadata-location"]
        after_keys = _s3_object_keys(seaweedfs_host_endpoint, location)
        assert before_keys <= after_keys
        assert any(key.endswith(".parquet") for key in after_keys - before_keys)
        assert any(key.endswith(".avro") for key in after_keys - before_keys)
        for key, expected_bytes in original_bytes.items():
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                assert source.read() == expected_bytes
        assert [row.id for row in spark.table(table_fqn).orderBy("id").collect()] == [2, 3, 4]
        history = spark.sql(f"SELECT id FROM {table_fqn} VERSION AS OF {original_snapshot} ORDER BY id")  # noqa: S608
        assert [row.id for row in history.collect()] == [1, 2, 3, 4]
        reference = StaticTable.from_metadata(after["metadata-location"], properties=properties)
        assert sorted(reference.scan().to_arrow().column("id").to_pylist()) == [2, 3, 4]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("apply_commit", [False, True], ids=["not_applied", "applied"])
def test_unknown_alter_does_not_report_success(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, apply_commit
):
    table_name = f"unknown_alter_{apply_commit}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        _, gate = commit_gate_proxy
        gate.response_fault = 503
        gate.apply_commit = apply_commit
        gate.arm(NAMESPACE, table_name, snapshot_only=False)
        gate.release.set()
        with (
            spark_session_factory(gated_remote) as sessions,
            pytest.raises(PySparkException, match="commit state unknown"),
        ):
            sessions.create().sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ('custom'='value')").collect()
        assert gate.request_count == 1
        properties = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["properties"]
        assert properties.get("custom") == ("value" if apply_commit else None)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("spelling", ["absolute", "relative"])
def test_rest_expiration_rejects_escaped_data_alias_without_mutation(
    spark, gated_remote, iceberg_rest_endpoint, seaweedfs_host_endpoint, spelling
):
    from pysail.tests._dev.spark.iceberg.test_iceberg_expire_snapshots import _install_escaped_data_aliases

    name = f"escaped_expiration_{spelling}"
    table_fqn = f"{NAMESPACE}.{name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id BIGINT) USING iceberg")
    try:
        for value in [1, 2]:
            spark.sql(f"INSERT OVERWRITE {table_fqn} VALUES ({value}), ({value + 10})")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, name)
        file_io = PyArrowFileIO(
            {
                "s3.endpoint": seaweedfs_host_endpoint,
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
            }
        )
        expired, shared, content = _install_escaped_data_aliases(before["metadata-location"], file_io, spelling)
        assert sorted(row.value for row in spark.table(table_fqn).selectExpr("id + 100 AS value").collect()) == [
            102,
            112,
        ]
        location = before["metadata"]["location"]
        bucket = urllib.parse.urlparse(location).netloc
        files = {}
        for key in _s3_object_keys(seaweedfs_host_endpoint, location):
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                files[key] = source.read()
        try:
            with pytest.raises(PySparkException, match="ambiguous non-URI path"):
                spark.sql(
                    f"CALL system.expire_snapshots('{table_fqn}', snapshot_ids => array(CAST({expired} AS BIGINT)))"
                ).collect()
        finally:
            with spark_session_factory(gated_remote) as sessions:
                reopened = sessions.create().table(table_fqn)
                assert sorted(row.value for row in reopened.selectExpr("id + 100 AS value").collect()) == [102, 112]
            with file_io.new_input(shared).open() as source:
                assert source.read() == content
        after = _load_table(iceberg_rest_endpoint, name)
        assert after["metadata-location"] == before["metadata-location"]
        assert after["metadata"] == before["metadata"]
        assert files.keys() == _s3_object_keys(seaweedfs_host_endpoint, location)
        for key, expected in files.items():
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                assert source.read() == expected
        with file_io.new_input(shared).open() as source:
            assert source.read() == content
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


def _rest_update(endpoint, table_name, updates):
    before = _load_table(endpoint, table_name)
    response = requests.post(
        f"{endpoint}/v1/namespaces/{NAMESPACE}/tables/{table_name}",
        json={
            "requirements": [{"type": "assert-table-uuid", "uuid": before["metadata"]["table-uuid"]}],
            "updates": updates,
        },
        timeout=30,
    )
    response.raise_for_status()


@pytest.mark.parametrize("change", ["none", "unseen_tag", "main", "observed_tag", "retention"])
def test_rest_expiration_standard_updates_at_publication_gate(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, seaweedfs_host_endpoint, change
):
    table_name = f"expiration_{change}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
        expired = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"INSERT OVERWRITE {table_fqn} VALUES (2)")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        current = before["metadata"]["current-snapshot-id"]
        if change == "observed_tag":
            _rest_update(
                iceberg_rest_endpoint,
                table_name,
                [{"action": "set-snapshot-ref", "ref-name": "hold", "type": "tag", "snapshot-id": current}],
            )
        location = before["metadata"]["location"]
        original_files = _s3_object_keys(seaweedfs_host_endpoint, location)
        _, gate = commit_gate_proxy
        gate.arm(NAMESPACE, table_name)
        with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor(max_workers=1) as executor:
            client = sessions.create()
            future = executor.submit(
                lambda: client.sql(
                    f"CALL system.expire_snapshots('{table_fqn}', older_than => TIMESTAMP '2099-01-01 00:00:00')"
                ).collect()
            )
            try:
                if not gate.arrived.wait(timeout=60):
                    outcome = future.exception(timeout=1) if future.done() else "still running"
                    pytest.fail(f"Expiration did not reach POST gate: {outcome}")
                if change in {"unseen_tag", "observed_tag"}:
                    _rest_update(
                        iceberg_rest_endpoint,
                        table_name,
                        [{"action": "set-snapshot-ref", "ref-name": "hold", "type": "tag", "snapshot-id": expired}],
                    )
                elif change == "main":
                    spark.sql(f"INSERT INTO {table_fqn} VALUES (3)")  # noqa: S608
                elif change == "retention":
                    _rest_update(
                        iceberg_rest_endpoint,
                        table_name,
                        [{"action": "set-properties", "updates": {"history.expire.min-snapshots-to-keep": "10"}}],
                    )
                concurrent = _load_table(iceberg_rest_endpoint, table_name)
                if change in {"unseen_tag", "observed_tag"}:
                    assert concurrent["metadata"]["refs"]["hold"]["snapshot-id"] == expired
            finally:
                gate.release.set()
            result = future.result(timeout=60)
        assert gate.request is not None
        requirements = gate.request["requirements"]
        assert {"type": "assert-table-uuid", "uuid": before["metadata"]["table-uuid"]} in requirements
        assert {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current} in requirements
        after = _load_table(iceberg_rest_endpoint, table_name)
        remaining = {s["snapshot-id"] for s in after["metadata"]["snapshots"]}
        if change == "observed_tag":
            assert gate.response_status == HTTP_CONFLICT
            assert expired in remaining
            assert after["metadata"]["refs"]["hold"]["snapshot-id"] == expired
            assert original_files <= _s3_object_keys(seaweedfs_host_endpoint, location)
            assert sum(result[0]) == 0
        else:
            assert expired not in remaining
            assert result[0].deleted_data_files_count == 1
            assert after["metadata-location"] != concurrent["metadata-location"]
            assert gate.response_status == (HTTP_CONFLICT if change == "main" else HTTPStatus.OK)
            assert gate.request_count == (2 if change == "main" else 1)
            if change == "unseen_tag":
                # Apache Iceberg removes dangling refs when applying remove-snapshots.
                # The standard protocol cannot guard names absent from the prepared metadata.
                assert "hold" not in after["metadata"].get("refs", {})
            if change == "retention":
                # Retention-only updates do not invalidate observed-head requirements.
                assert after["metadata"]["properties"]["history.expire.min-snapshots-to-keep"] == "10"
        assert [row.id for row in spark.table(table_fqn).orderBy("id").collect()] == (
            [2, 3] if change == "main" else [2]
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("clean", [None, False, True], ids=["default", "false", "true"])
def test_rest_expiration_preserves_staged_metadata_at_publication_gate(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, seaweedfs_host_endpoint, clean
):
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog

    table_name = f"expiration_staged_metadata_{clean}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        catalog = RestCatalog(
            "reference",
            uri=iceberg_rest_endpoint,
            **{
                "s3.endpoint": seaweedfs_host_endpoint,
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
            },
        )
        writer = catalog.load_table(table_fqn)
        transaction = writer.transaction()
        transaction.append(pa.table({"id": pa.array([1, 11], type=pa.int32())}), branch=None)
        staged = transaction.table_metadata.snapshots[-1]
        old_schema = writer.schema().schema_id
        old_spec = writer.spec().spec_id
        # The staged transaction has real files but has not published its snapshot.
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMN extra STRING")
        evolving = catalog.load_table(table_fqn)
        with evolving.update_spec() as update:
            update.add_identity("id")
        before = _load_table(iceberg_rest_endpoint, table_name)
        assert before["metadata"]["current-schema-id"] != old_schema
        assert before["metadata"]["default-spec-id"] != old_spec
        location = before["metadata"]["location"]
        bucket = urllib.parse.urlparse(location).netloc
        original_bytes = {}
        for key in _s3_object_keys(seaweedfs_host_endpoint, location):
            with writer.io.new_input(f"s3://{bucket}/{key}").open() as source:
                original_bytes[key] = source.read()
        _, gate = commit_gate_proxy
        gate.arm(NAMESPACE, table_name)
        option = "" if clean is None else f", clean_expired_metadata => {str(clean).lower()}"
        statement = f"CALL system.expire_snapshots('{table_fqn}'{option})"
        with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor(max_workers=1) as executor:
            client = sessions.create()
            future = executor.submit(lambda: client.sql(statement).collect())
            try:
                deadline = time.monotonic() + 60
                while not future.done() and not gate.arrived.wait(timeout=0.05):
                    assert time.monotonic() < deadline, "expiration neither completed nor reached publication"
                # A safe refusal (or default no-op) must not publish anything. On an
                # unsafe implementation, publish the valid staged write before cleanup.
                assert _load_table(iceberg_rest_endpoint, table_name) == before
                transaction.commit_transaction()
                concurrent = _load_table(iceberg_rest_endpoint, table_name)
                for field in ["refs", "current-schema-id", "default-spec-id"]:
                    assert concurrent["metadata"].get(field) == before["metadata"].get(field)
                assert catalog.load_table(table_fqn).scan(snapshot_id=staged.snapshot_id).to_arrow()[
                    "id"
                ].to_pylist() == [
                    1,
                    11,
                ]
            finally:
                gate.release.set()
            # Check authoritative artifacts even if an unsafe implementation reports success.
            error = future.exception(timeout=60)
            after = _load_table(iceberg_rest_endpoint, table_name)
            assert old_schema in {schema["schema-id"] for schema in after["metadata"]["schemas"]}
            assert old_spec in {spec["spec-id"] for spec in after["metadata"]["partition-specs"]}
            assert after == concurrent
            assert gate.table_posts == 0
            assert original_bytes.keys() <= _s3_object_keys(seaweedfs_host_endpoint, location)
            for key, expected in original_bytes.items():
                with writer.io.new_input(f"s3://{bucket}/{key}").open() as source:
                    assert source.read() == expected
            if clean:
                assert isinstance(error, PySparkException)
                assert "clean_expired_metadata=true is not supported for REST tables" in str(error)
            else:
                assert error is None
                assert tuple(future.result()[0]) == (0, 0, 0, 0, 0, 0)
            rows = client.sql(
                f"SELECT id + 100 AS value FROM {table_fqn} VERSION AS OF {staged.snapshot_id} ORDER BY id"  # noqa: S608
            ).collect()
            assert [row.value for row in rows] == [101, 111]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("clean", [None, False, True], ids=["default", "false", "true"])
def test_rest_expiration_metadata_cleanup_option(spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, clean):
    table_name = f"expiration_metadata_option_{clean}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1), (11)")  # noqa: S608
        expired = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMN extra STRING")
        spark.sql(f"INSERT OVERWRITE {table_fqn} VALUES (2, 'new'), (12, 'new')")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        _, gate = commit_gate_proxy
        option = "" if clean is None else f", clean_expired_metadata => {str(clean).lower()}"
        statement = (
            f"CALL system.expire_snapshots('{table_fqn}', snapshot_ids => array(CAST({expired} AS BIGINT)){option})"
        )
        with spark_session_factory(gated_remote) as sessions:
            client = sessions.create()
            if clean:
                with pytest.raises(
                    PySparkException, match="clean_expired_metadata=true is not supported for REST tables"
                ):
                    client.sql(statement).collect()
                assert gate.table_posts == 0
                assert _load_table(iceberg_rest_endpoint, table_name) == before
            else:
                assert client.sql(statement).first().deleted_data_files_count > 0
                after = _load_table(iceberg_rest_endpoint, table_name)
                assert expired not in {snapshot["snapshot-id"] for snapshot in after["metadata"]["snapshots"]}
                assert after["metadata"]["schemas"] == before["metadata"]["schemas"]
            assert sorted(row.value for row in client.table(table_fqn).selectExpr("id + 100 AS value").collect()) == [
                102,
                112,
            ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


def test_rest_expiration_preserves_files_when_gc_disabled_at_publication_gate(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, seaweedfs_host_endpoint
):
    table_name = "expiration_gc_disabled"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg TBLPROPERTIES ('gc.enabled'='true')")
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1), (11)")  # noqa: S608
        expired = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"INSERT OVERWRITE {table_fqn} VALUES (2), (12)")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        location = before["metadata"]["location"]
        file_io = PyArrowFileIO(
            {
                "s3.endpoint": seaweedfs_host_endpoint,
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
            }
        )
        bucket = urllib.parse.urlparse(location).netloc
        original_bytes = {}
        for key in _s3_object_keys(seaweedfs_host_endpoint, location):
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                original_bytes[key] = source.read()
        _, gate = commit_gate_proxy
        gate.arm(NAMESPACE, table_name)
        statement = f"CALL system.expire_snapshots('{table_fqn}', snapshot_ids => array(CAST({expired} AS BIGINT)))"
        with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor(max_workers=1) as executor:
            client = sessions.create()
            future = executor.submit(lambda: client.sql(statement).collect())
            try:
                if not gate.arrived.wait(timeout=60):
                    outcome = future.exception(timeout=1) if future.done() else "still running"
                    pytest.fail(f"Expiration did not reach POST gate: {outcome}")
                _rest_update(
                    iceberg_rest_endpoint,
                    table_name,
                    [{"action": "set-properties", "updates": {"gc.enabled": "false"}}],
                )
                concurrent = _load_table(iceberg_rest_endpoint, table_name)
                assert concurrent["metadata"]["refs"] == before["metadata"]["refs"]
            finally:
                gate.release.set()
            result = future.result(timeout=60)
        after = _load_table(iceberg_rest_endpoint, table_name)
        assert gate.response_status == HTTPStatus.OK
        assert gate.request_count == 1
        assert gate.table_posts == 1
        assert after["metadata-location"] != concurrent["metadata-location"]
        assert expired not in {s["snapshot-id"] for s in after["metadata"]["snapshots"]}
        assert after["metadata"]["properties"]["gc.enabled"] == "false"
        assert original_bytes.keys() <= _s3_object_keys(seaweedfs_host_endpoint, location)
        for key, expected in original_bytes.items():
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                assert source.read() == expected
        assert len(result) == 1
        assert tuple(result[0]) == (0, 0, 0, 0, 0, 0)
        with spark_session_factory(gated_remote) as sessions:
            client = sessions.create()
            assert sorted(row.value for row in client.table(table_fqn).selectExpr("id + 100 AS value").collect()) == [
                102,
                112,
            ]
            with pytest.raises(PySparkException, match="GC is disabled"):
                client.sql(statement).collect()
        assert gate.table_posts == 1
        assert _load_table(iceberg_rest_endpoint, table_name)["metadata-location"] == after["metadata-location"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


@pytest.mark.parametrize("apply_commit", [False, True], ids=["not_applied", "applied"])
@pytest.mark.parametrize(
    "fault", [503, "disconnect", "malformed", "stale_payload", "wrong_uuid", "unapplied_removals", "refresh_denied"]
)
def test_unknown_expiration_preserves_files_without_replay(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, seaweedfs_host_endpoint, apply_commit, fault
):
    table_name = f"unknown_expiration_{apply_commit}_{fault}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
        expired = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"INSERT OVERWRITE {table_fqn} VALUES (2)")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        location = before["metadata"]["location"]
        file_io = PyArrowFileIO(
            {
                "s3.endpoint": seaweedfs_host_endpoint,
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.region": "us-east-1",
            }
        )
        bucket = urllib.parse.urlparse(location).netloc
        original_bytes = {}
        for key in _s3_object_keys(seaweedfs_host_endpoint, location):
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                original_bytes[key] = source.read()
        _, gate = commit_gate_proxy
        gate.response_fault = fault
        gate.apply_commit = apply_commit
        if fault in {"stale_payload", "unapplied_removals"}:
            gate.response_payload = before
        if fault == "refresh_denied":
            gate.response_fault = None if apply_commit else 503
            gate.refresh_status = HTTPStatus.FORBIDDEN
        gate.arm(NAMESPACE, table_name)
        gate.release.set()
        with spark_session_factory(gated_remote) as sessions:
            statement = f"CALL system.expire_snapshots('{table_fqn}', snapshot_ids => array(CAST({expired} AS BIGINT)))"
            if fault == "refresh_denied" and apply_commit:
                assert sum(sessions.create().sql(statement).first()) == 0
            else:
                with pytest.raises(PySparkException, match="commit state unknown"):
                    sessions.create().sql(statement).collect()
        assert gate.request_count == 1
        assert gate.table_posts == 1
        after = _load_table(iceberg_rest_endpoint, table_name)
        assert (expired not in {s["snapshot-id"] for s in after["metadata"]["snapshots"]}) == apply_commit
        assert original_bytes.keys() <= _s3_object_keys(seaweedfs_host_endpoint, location)
        for key, expected in original_bytes.items():
            with file_io.new_input(f"s3://{bucket}/{key}").open() as source:
                assert source.read() == expected
        assert [row.id for row in spark.table(table_fqn).collect()] == [2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


def _rest_access_hints(hint, endpoint):
    if hint == "endpoint":
        return {"config": {"s3.endpoint": endpoint}}
    if hint == "credentials":
        return {
            "storage-credentials": [
                {"prefix": "s3://", "config": {"s3.access-key-id": "admin", "s3.secret-access-key": "password"}}
            ]
        }
    if hint == "signing":
        return {"config": {"s3.remote-signing-enabled": "true"}}
    return {"config": {}}


@pytest.mark.parametrize("hint", ["endpoint", "credentials", "signing"])
def test_rest_procedures_use_configured_access_with_advertised_hints(
    spark, gated_remote, commit_gate_proxy, iceberg_rest_endpoint, seaweedfs_host_endpoint, hint
):
    table_name = f"procedure_access_{hint}"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")
    try:
        spark.sql(f"INSERT INTO {table_fqn} VALUES (1)")  # noqa: S608
        first = _load_table(iceberg_rest_endpoint, table_name)["metadata"]["current-snapshot-id"]
        spark.sql(f"INSERT INTO {table_fqn} VALUES (2)")  # noqa: S608
        before = _load_table(iceberg_rest_endpoint, table_name)
        current = before["metadata"]["current-snapshot-id"]
        _, gate = commit_gate_proxy
        gate.table_hints = _rest_access_hints(hint, seaweedfs_host_endpoint)
        with spark_session_factory(gated_remote) as sessions:
            client = sessions.create()
            ancestors = client.sql(f"CALL system.ancestors_of('{table_fqn}')").collect()
            assert {row.snapshot_id for row in ancestors} == {first, current}
            client.sql(f"CALL system.set_current_snapshot(table => '{table_fqn}', snapshot_id => {first})").collect()
            assert [row.id for row in client.table(table_fqn).collect()] == [1]
        after = _load_table(iceberg_rest_endpoint, table_name)
        assert after["metadata-location"] != before["metadata-location"]
        assert after["metadata"]["current-snapshot-id"] == first
        assert {s["snapshot-id"] for s in after["metadata"]["snapshots"]} == {first, current}
        assert gate.table_posts > 0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")


def test_rest_catalog_rejects_non_iceberg_create_format(
    spark: SparkSession,
) -> None:
    table_name = "delta_bad_t"
    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")

    with pytest.raises(Exception, match=r"(?i)Iceberg REST catalog cannot create 'delta' tables"):
        spark.sql(
            f"""
            CREATE TABLE {NAMESPACE}.{table_name} (
              id INT
            )
            USING DELTA
            """
        )


def test_show_table_properties_reads_external_rest_property_commits(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table = "show_external_properties"
    table_fqn = f"sail.{NAMESPACE}.{table}"
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg TBLPROPERTIES ('custom.show' = 'initial')")
    try:
        assert [tuple(row) for row in spark.sql(f"SHOW TBLPROPERTIES {table_fqn} ('custom.show')").collect()] == [
            ("custom.show", "initial")
        ]
        previous = _load_table(iceberg_rest_endpoint, table)
        for update, expected in [
            ({"action": "set-properties", "updates": {"custom.show": "external"}}, "external"),
            ({"action": "remove-properties", "removals": ["custom.show"]}, None),
        ]:
            response = requests.post(
                f"{iceberg_rest_endpoint}/v1/namespaces/{NAMESPACE}/tables/{table}",
                json={
                    "requirements": [{"type": "assert-table-uuid", "uuid": previous["metadata"]["table-uuid"]}],
                    "updates": [update],
                },
                timeout=30,
            )
            response.raise_for_status()
            committed = _load_table(iceberg_rest_endpoint, table)
            assert committed["metadata-location"] != previous["metadata-location"]
            assert committed["metadata"]["properties"].get("custom.show") == expected
            rows = spark.sql(f"SHOW TBLPROPERTIES {table_fqn} ('custom.show')").collect()
            assert len(rows) == 1
            assert rows[0].key == "custom.show"
            if expected is None:
                assert "does not have property: custom.show" in rows[0].value
            else:
                assert rows[0].value == expected
            previous = committed
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
