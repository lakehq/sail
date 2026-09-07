from __future__ import annotations

import json
import os
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

NAMESPACE = "lakekeeper_access_session_test"
TABLE_NAME = "vended_credentials_t"
TABLE = f"sail.{NAMESPACE}.{TABLE_NAME}"
WRITE_TABLE_NAME = "vended_credentials_write_t"
SEEDED_ROWS = [(1, "a"), (2, "b")]
INVALID_ACCESS_KEY_ID = "sail-invalid-access-key"
INVALID_SECRET_ACCESS_KEY = "sail-invalid-secret-key"  # noqa: S105
INVALID_SESSION_TOKEN = "sail-invalid-session-token"  # noqa: S105


@pytest.fixture(scope="module", params=[False, True], ids=["bootstrap", "refresh"])
def catalog_endpoint(request: pytest.FixtureRequest, lakekeeper_endpoint: str) -> Generator[str, None, None]:
    """Force expired bootstrap credentials while leaving the real refresh API intact."""
    if not request.param:
        yield lakekeeper_endpoint
        return

    refresh_requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def forward(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            response = requests.request(
                self.command,
                f"{lakekeeper_endpoint}{self.path}",
                data=body,
                headers={key: value for key, value in self.headers.items() if key.lower() != "host"},
                timeout=30,
            )
            content = response.content
            if response.ok and "application/json" in response.headers.get("Content-Type", ""):
                payload = response.json()
                if "overrides" in payload:
                    payload["overrides"]["uri"] = f"http://127.0.0.1:{self.server.server_port}/catalog"
                if "metadata" in payload:
                    for credential in payload.get("storage-credentials", []):
                        config = credential["config"]
                        config["s3.access-key-id"] = INVALID_ACCESS_KEY_ID
                        config["expiration-time"] = "0"
                        config["s3.session-token-expires-at-ms"] = "0"
                        endpoint = urllib.parse.urlsplit(config["client.refresh-credentials-endpoint"])
                        config["client.refresh-credentials-endpoint"] = (
                            f"http://127.0.0.1:{self.server.server_port}{endpoint.path}"
                        )
                elif "storage-credentials" in payload:
                    refresh_requests.append(self.path)
                content = json.dumps(payload).encode()
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        do_GET = forward  # noqa: N815
        do_POST = forward  # noqa: N815
        do_DELETE = forward  # noqa: N815

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
        assert refresh_requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def seaweedfs_credentials() -> tuple[str, str]:
    """Use credentials unique to this module so cached ambient credentials cannot work."""
    return "lakekeeper-vending-admin", "lakekeeper-vending-password"


@pytest.fixture(scope="module")
def seeded_lakekeeper_table(
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
    seaweedfs_shared_endpoint: str,
    seaweedfs_credentials: tuple[str, str],
) -> Generator[None, None, None]:
    """Seed the table outside Sail with explicit test-only storage credentials."""
    del lakekeeper_warehouse_id
    access_key_id, secret_access_key = seaweedfs_credentials
    catalog = load_catalog(
        "lakekeeper_seed",
        type="rest",
        uri=f"{lakekeeper_endpoint}/catalog",
        warehouse="demo",
        **{
            "s3.endpoint": seaweedfs_shared_endpoint,
            "s3.access-key-id": access_key_id,
            "s3.secret-access-key": secret_access_key,
            "s3.region": "us-east-1",
            "s3.force-virtual-addressing": "false",
        },
    )
    identifier = (NAMESPACE, TABLE_NAME)
    catalog.create_namespace(NAMESPACE)
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        ),
    )
    table.append(pa.table({"id": pa.array([1, 2], type=pa.int64()), "name": ["a", "b"]}))
    try:
        yield
    finally:
        catalog.drop_table(identifier)
        catalog.drop_namespace(NAMESPACE)


@pytest.fixture(scope="module")
def remote(
    catalog_endpoint: str,
    seeded_lakekeeper_table: None,
    seaweedfs_shared_endpoint: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[str, None, None]:
    """Start a local cluster whose ambient S3 credentials cannot access the table."""
    del seeded_lakekeeper_table
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{catalog_endpoint}/catalog", warehouse="demo"}}]'
    aws_config_dir = tmp_path_factory.mktemp("lakekeeper-empty-aws-config")
    shared_credentials_file = aws_config_dir / "credentials"
    config_file = aws_config_dir / "config"
    shared_credentials_file.write_text("")
    config_file.write_text("")

    inherited_credential_sources = (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    )
    with pytest.MonkeyPatch.context() as monkeypatch:
        for key in inherited_credential_sources:
            monkeypatch.delenv(key, raising=False)
        with spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_CATALOG__LIST": catalog_config,
                "AWS_ACCESS_KEY_ID": INVALID_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": INVALID_SECRET_ACCESS_KEY,
                "AWS_SESSION_TOKEN": INVALID_SESSION_TOKEN,
                "AWS_SHARED_CREDENTIALS_FILE": str(shared_credentials_file),
                "AWS_CONFIG_FILE": str(config_file),
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_REGION": "us-east-1",
                "AWS_ENDPOINT": seaweedfs_shared_endpoint,
                "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
                "AWS_ALLOW_HTTP": "true",
            },
        ) as server:
            assert os.environ["AWS_ACCESS_KEY_ID"] == INVALID_ACCESS_KEY_ID
            assert os.environ["AWS_SECRET_ACCESS_KEY"] == INVALID_SECRET_ACCESS_KEY
            assert os.environ["AWS_SESSION_TOKEN"] == INVALID_SESSION_TOKEN
            yield server.remote


def _load_lakekeeper_table(
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
    table_name: str = TABLE_NAME,
) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(table_name, safe="")
    response = requests.get(
        f"{lakekeeper_endpoint}/catalog/v1/{lakekeeper_warehouse_id}/namespaces/{namespace}/tables/{table}",
        headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _s3_object_keys(
    endpoint: str,
    credentials: tuple[str, str],
    location: str,
) -> set[str]:
    import boto3
    from botocore.config import Config

    parsed = urllib.parse.urlparse(location)
    assert parsed.scheme == "s3"
    prefix = parsed.path.lstrip("/").rstrip("/") + "/"
    access_key_id, secret_access_key = credentials
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc, Prefix=prefix)
    return {item["Key"] for page in pages for item in page.get("Contents", [])}


def _completed_worker_stages(spark: SparkSession) -> int:
    return spark.sql(
        "SELECT count(*) AS count FROM system.execution.stages WHERE placement = 'Worker' AND status = 'INACTIVE'"
    ).first()["count"]


def test_read_uses_lakekeeper_vended_credentials_on_workers(
    spark: SparkSession,
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
    seaweedfs_shared_endpoint: str,
) -> None:
    rows = spark.table(TABLE).orderBy("id").collect()
    assert [(row["id"], row["name"]) for row in rows] == SEEDED_ROWS

    table = _load_lakekeeper_table(lakekeeper_endpoint, lakekeeper_warehouse_id)
    assert table["config"].get("s3.remote-signing-enabled") != "true"
    credentials = table["storage-credentials"]
    assert len(credentials) == 1
    assert credentials[0]["prefix"].startswith("s3://icebergdata/lakekeeper/")
    credential_config = credentials[0]["config"]
    required_keys = {
        "client.refresh-credentials-endpoint",
        "expiration-time",
        "s3.access-key-id",
        "s3.endpoint",
        "s3.secret-access-key",
        "s3.session-token",
        "s3.session-token-expires-at-ms",
    }
    assert not required_keys.difference(credential_config)
    assert urllib.parse.urlsplit(credential_config["s3.endpoint"]).hostname == "s3.localhost"
    assert (
        urllib.parse.urlsplit(credential_config["s3.endpoint"]).port
        == urllib.parse.urlsplit(seaweedfs_shared_endpoint).port
    )
    refreshed = requests.get(
        credential_config["client.refresh-credentials-endpoint"],
        headers={"X-Iceberg-Access-Delegation": "vended-credentials"},
        timeout=30,
    )
    refreshed.raise_for_status()
    assert refreshed.json()["storage-credentials"][0]["prefix"] == credentials[0]["prefix"]

    assert _completed_worker_stages(spark) > 0


def test_append_and_overwrite_use_vended_credentials_on_workers_and_commit_rest_pointer(
    spark: SparkSession,
    catalog_endpoint: str,
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
    seaweedfs_host_endpoint: str,
    seaweedfs_credentials: tuple[str, str],
) -> None:
    table_name = f"{WRITE_TABLE_NAME}_{urllib.parse.urlsplit(catalog_endpoint).port}"
    table = f"sail.{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table} (
              id BIGINT,
              name STRING
            )
            USING iceberg
            """
        )
        before = _load_lakekeeper_table(
            lakekeeper_endpoint,
            lakekeeper_warehouse_id,
            table_name,
        )
        before_metadata = before["metadata"]
        before_metadata_location = before["metadata-location"]
        assert before_metadata_location
        assert before_metadata.get("current-snapshot-id") in (None, -1)
        assert before_metadata.get("snapshots", []) == []
        table_location = before_metadata["location"]
        before_keys = _s3_object_keys(
            seaweedfs_host_endpoint,
            seaweedfs_credentials,
            table_location,
        )
        worker_stages_before = _completed_worker_stages(spark)

        source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        source.writeTo(table).append()

        worker_stages_after = _completed_worker_stages(spark)
        assert worker_stages_after > worker_stages_before
        after = _load_lakekeeper_table(
            lakekeeper_endpoint,
            lakekeeper_warehouse_id,
            table_name,
        )
        after_metadata = after["metadata"]
        assert after["metadata-location"] != before_metadata_location
        assert after_metadata["metadata-log"][-1]["metadata-file"] == before_metadata_location
        current_snapshot_id = after_metadata["current-snapshot-id"]
        assert current_snapshot_id not in (None, -1)
        assert after_metadata["refs"]["main"]["snapshot-id"] == current_snapshot_id
        assert after_metadata["snapshot-log"][-1]["snapshot-id"] == current_snapshot_id
        current_snapshot = next(
            snapshot for snapshot in after_metadata["snapshots"] if snapshot["snapshot-id"] == current_snapshot_id
        )
        summary = current_snapshot["summary"]
        assert summary["operation"] == "append"
        assert int(summary["added-records"]) == len(SEEDED_ROWS)
        assert int(summary["added-data-files"]) >= 1

        after_keys = _s3_object_keys(
            seaweedfs_host_endpoint,
            seaweedfs_credentials,
            table_location,
        )
        created_keys = after_keys - before_keys
        assert any(key.endswith(".parquet") for key in created_keys)
        assert any("/metadata/manifest-" in key and key.endswith(".avro") for key in created_keys)
        assert any("/metadata/snap-" in key and key.endswith(".avro") for key in created_keys)
        assert any(key.endswith(".metadata.json") for key in created_keys)

        rows = spark.table(table).orderBy("id").collect()
        assert [(row["id"], row["name"]) for row in rows] == SEEDED_ROWS

        overwrite_stages_before = _completed_worker_stages(spark)
        spark.sql(f"INSERT OVERWRITE TABLE {table} VALUES (3, 'new'), (4, 'new')")  # noqa: S608
        overwrite_stages_after = _completed_worker_stages(spark)
        assert overwrite_stages_after > overwrite_stages_before

        overwritten = _load_lakekeeper_table(
            lakekeeper_endpoint,
            lakekeeper_warehouse_id,
            table_name,
        )
        assert overwritten["metadata-location"] != after["metadata-location"]
        overwrite_snapshot = next(
            snapshot
            for snapshot in overwritten["metadata"]["snapshots"]
            if snapshot["snapshot-id"] == overwritten["metadata"]["current-snapshot-id"]
        )
        assert overwrite_snapshot["summary"]["operation"] == "overwrite"
        rows = spark.table(table).orderBy("id").collect()
        assert [(row["id"], row["name"]) for row in rows] == [(3, "new"), (4, "new")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_ctas_join_and_delete_keep_each_tables_storage_access(spark: SparkSession, catalog_endpoint: str) -> None:
    table = f"sail.{NAMESPACE}.vended_ctas_t_{urllib.parse.urlsplit(catalog_endpoint).port}"
    try:
        spark.sql(f"EXPLAIN CREATE TABLE {table} USING iceberg AS SELECT * FROM {TABLE}").collect()  # noqa: S608
        assert not spark.catalog.tableExists(table)
        spark.sql(
            f"""
            CREATE TABLE {table} USING iceberg
            TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
            AS SELECT id, name FROM {TABLE}
            """  # noqa: S608
        )
        rows = spark.sql(
            f"SELECT a.id, b.name FROM {TABLE} a JOIN {table} b ON a.id = b.id ORDER BY a.id"  # noqa: S608
        ).collect()
        assert [(row.id, row.name) for row in rows] == SEEDED_ROWS

        spark.sql(f"DELETE FROM {table} WHERE id = CAST(1 AS BIGINT)")  # noqa: S608
        rows = spark.table(table).collect()
        assert [(row.id, row.name) for row in rows] == [(2, "b")]
        assert spark.table(TABLE).count() == len(SEEDED_ROWS)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
