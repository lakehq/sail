from __future__ import annotations

import json
import re
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import boto3
import pytest
import requests
from botocore.config import Config

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

    def arm(self, namespace: str, table: str) -> None:
        with self._lock:
            self._target = ([namespace], table)

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
                self._claimed
                or self._target is None
                or not has_snapshot_requirement
                or identifier.get("namespace") != self._target[0]
                or identifier.get("name") != self._target[1]
            ):
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
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length) if length else b""
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

            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in {
                    "connection",
                    "content-encoding",
                    "content-length",
                    "transfer-encoding",
                }:
                    self.send_header(key, value)
            self.send_header("content-length", str(len(response.content)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response.content)

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


def test_delete_advances_rest_catalog_metadata_location_with_equality_delete(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
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
    assert summary["added-equality-delete-files"] == "1"
    assert summary["added-equality-deletes"] == "1"
    assert "deleted-records" not in summary
    assert "added-position-delete-files" not in summary
    assert summary["total-data-files"] == "1"
    assert summary["total-delete-files"] == "1"
    assert summary["total-records"] == "3"

    rows = spark.sql("SELECT id, name, flag FROM iceberg_commit_test.delete_t ORDER BY id").collect()
    assert [(row["id"], row["name"], row["flag"]) for row in rows] == [
        (1, "keep-a", "keep"),
        (3, "keep-c", "keep"),
    ]


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


def test_stale_merge_catalog_conflict_cleans_uncommitted_artifacts(
    spark: SparkSession,
    gated_remote: str,
    commit_gate_proxy: tuple[str, _CommitGate],
    iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
) -> None:
    table_name = "stale_merge_conflict_t"
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

        def stale_merge() -> None:
            slow.sql(
                """
                MERGE INTO iceberg_commit_test.stale_merge_conflict_t AS t
                USING stale_merge_source AS s
                ON t.id = s.id
                WHEN NOT MATCHED THEN
                  INSERT (id, name) VALUES (s.id, s.name)
                """
            ).collect()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(stale_merge)
            try:
                if not gate.arrived.wait(timeout=60):
                    outcome = future.exception(timeout=1) if future.done() else "still running"
                    pytest.fail(f"MERGE did not reach the catalog commit gate: {outcome}")

                blocked_keys = _s3_object_keys(seaweedfs_host_endpoint, table_location)
                slow_created_keys = blocked_keys - before_keys
                assert any(key.endswith(".parquet") for key in slow_created_keys)
                assert any("/metadata/manifest-" in key for key in slow_created_keys)
                assert any("/metadata/snap-" in key for key in slow_created_keys)

                spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'fast')")  # noqa: S608
                fast_commit = _load_table(iceberg_rest_endpoint, table_name)
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
        remaining_slow_keys = slow_created_keys & after_keys
        if not remaining_slow_keys or time.monotonic() >= cleanup_deadline:
            break
        time.sleep(0.05)
    assert not remaining_slow_keys

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_metadata = after["metadata"]
    after_snapshot = _current_snapshot(after_metadata)
    assert after["metadata-location"] == fast_commit["metadata-location"]
    assert after_metadata["metadata-log"][-1]["metadata-file"] == before["metadata-location"]
    assert after_snapshot["parent-snapshot-id"] == before_snapshot_id
    assert proposed_snapshot["snapshot-id"] not in {snapshot["snapshot-id"] for snapshot in after_metadata["snapshots"]}
    assert sum(key.endswith(".parquet") for key in after_keys) == int(after_snapshot["summary"]["total-data-files"])

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.name) for row in rows] == [(1, "base"), (2, "fast")]


def test_rest_catalog_rejects_catalog_managed_iceberg_alter(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "alter_reject_t"
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

    with pytest.raises(Exception, match="catalog-managed Iceberg tables"):
        spark.sql(
            f"""
            ALTER TABLE {table_fqn}
            SET TBLPROPERTIES ('owner' = 'alice')
            """
        )

    after = _load_table(iceberg_rest_endpoint, table_name)
    assert after["metadata-location"] == before_location


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
