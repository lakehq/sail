# ruff: noqa: S608
"""Unity credential vending across Delta driver and worker I/O."""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from botocore.config import Config
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pysail.testing.containers.unity_defaults import DEFAULT_CATALOG, UNITY_CATALOG_IMAGE
from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.catalog.iceberg_rest import conftest as iceberg_services

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

BUCKET = "deltadata"
SCHEMA = "vending"
SEED = f"sail.{SCHEMA}.seed_replay"
INVALID_KEY = "invalid-ambient-key"
SEED_ROWS = [(1, "a"), (2, "b")]
MIN_REFRESH_REQUESTS = 3

docker_network = iceberg_services.docker_network
seaweedfs_container = iceberg_services.seaweedfs_container
seaweedfs_host_endpoint = iceberg_services.seaweedfs_host_endpoint


@pytest.fixture(scope="module")
def seaweedfs_credentials() -> tuple[str, str]:
    return "delta-vending-admin", "delta-vending-password"


@pytest.fixture(scope="module")
def s3(seaweedfs_host_endpoint: str, seaweedfs_credentials: tuple[str, str]):
    access_key, secret_key = seaweedfs_credentials
    client = boto3.client(
        "s3",
        endpoint_url=seaweedfs_host_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            client.create_bucket(Bucket=BUCKET)
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(1)
        else:
            break
    return client


@pytest.fixture(scope="module")
def vended_credentials(s3, seaweedfs_host_endpoint: str, seaweedfs_credentials: tuple[str, str]) -> dict:
    del s3
    access_key, secret_key = seaweedfs_credentials
    sts = boto3.client(
        "sts",
        endpoint_url=seaweedfs_host_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    return sts.assume_role(RoleArn=iceberg_services.SEAWEEDFS_STS_ROLE_ARN, RoleSessionName="unity-delta-vending")[
        "Credentials"
    ]


@pytest.fixture(scope="module")
def unity_container(
    tmp_path_factory: pytest.TempPathFactory, vended_credentials: dict
) -> Generator[DockerContainer, None, None]:
    config = tmp_path_factory.mktemp("unity-vending") / "server.properties"
    config.write_text(
        "server.env=dev\nserver.authorization=disable\nserver.managed-table.enabled=true\n"
        f"s3.bucketPath.0=s3://{BUCKET}\ns3.region.0=us-east-1\n"
        f"s3.accessKey.0={vended_credentials['AccessKeyId']}\n"
        f"s3.secretKey.0={vended_credentials['SecretAccessKey']}\n"
        f"s3.sessionToken.0={vended_credentials['SessionToken']}\n"
    )
    container = (
        DockerContainer(UNITY_CATALOG_IMAGE)
        .with_exposed_ports(8080)
        .with_volume_mapping(str(config), "/home/unitycatalog/etc/conf/server.properties", "ro")
        .waiting_for(
            LogMessageWaitStrategy(
                "###################################################################"
            ).with_startup_timeout(120)
        )
    )
    with container:
        yield container


@pytest.fixture(scope="module")
def unity_base_url(unity_container: DockerContainer) -> str:
    return f"http://{unity_container.get_container_host_ip()}:{unity_container.get_exposed_port(8080)}/api/2.1/unity-catalog"


@pytest.fixture(scope="module")
def unity_catalog_initialized(unity_base_url: str, s3, seaweedfs_host_endpoint: str) -> None:
    for endpoint, payload in [
        ("catalogs", {"name": DEFAULT_CATALOG, "storage_root": f"s3://{BUCKET}/managed"}),
        ("schemas", {"name": SCHEMA, "catalog_name": DEFAULT_CATALOG}),
    ]:
        response = requests.post(f"{unity_base_url}/{endpoint}", json=payload, timeout=30)
        response.raise_for_status()
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    data = pa.BufferOutputStream()
    pq.write_table(pa.table({"id": pa.array([1, 2], type=pa.int64()), "name": ["a", "b"]}), data)
    parquet = data.getvalue().to_pybytes()
    for name, replay in [("seed_eager", "false"), ("seed_replay", "true")]:
        prefix = f"external/{name}"
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/data.parquet", Body=parquet)
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": str(uuid.uuid4()),
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps(schema),
                    "partitionColumns": [],
                    "configuration": {},
                }
            },
            {
                "add": {
                    "path": "data.parquet",
                    "partitionValues": {},
                    "size": len(parquet),
                    "modificationTime": int(time.time() * 1000),
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": 2}),
                }
            },
        ]
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/_delta_log/{0:020}.json", Body="\n".join(map(json.dumps, actions)))
        response = requests.post(
            f"{unity_base_url}/tables",
            json={
                "name": name,
                "catalog_name": DEFAULT_CATALOG,
                "schema_name": SCHEMA,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": f"s3://{BUCKET}/{prefix}",
                "columns": [
                    {
                        "name": "id",
                        "type_name": "LONG",
                        "type_text": "bigint",
                        "type_json": json.dumps("long"),
                        "nullable": True,
                        "position": 0,
                    },
                    {
                        "name": "name",
                        "type_name": "STRING",
                        "type_text": "string",
                        "type_json": json.dumps("string"),
                        "nullable": True,
                        "position": 1,
                    },
                ],
                "properties": {"option.metadataAsDataRead": replay},
            },
            timeout=30,
        )
        assert response.ok, response.text
        table_id = response.json()["table_id"]
        response = requests.post(
            f"{unity_base_url}/temporary-table-credentials",
            json={"table_id": table_id, "operation": "READ"},
            timeout=30,
        )
        assert response.ok, response.text
        credentials = response.json()["aws_temp_credentials"]
        reader = boto3.client(
            "s3",
            endpoint_url=seaweedfs_host_endpoint,
            region_name="us-east-1",
            aws_access_key_id=credentials["access_key_id"],
            aws_secret_access_key=credentials["secret_access_key"],
            aws_session_token=credentials["session_token"],
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        objects = reader.list_objects_v2(
            Bucket=BUCKET, Prefix=f"{prefix}/_delta_log/", StartAfter=f"{prefix}/_delta_log/{0:020}"
        )
        assert objects["Contents"][0]["Key"] == f"{prefix}/_delta_log/{0:020}.json"


@pytest.fixture(scope="module", params=["bootstrap", "refresh"])
def credential_mode(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(scope="module")
def vending_requests(credential_mode: str) -> list[dict]:
    del credential_mode
    return []


@pytest.fixture(scope="module")
def unity_rest_url(
    unity_base_url: str, credential_mode: str, vending_requests: list[dict]
) -> Generator[str, None, None]:
    attempts: Counter[str] = Counter()
    lock = threading.Lock()
    upstream = unity_base_url.removesuffix("/api/2.1/unity-catalog")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def forward(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            response = requests.request(
                self.command,
                f"{upstream}{self.path}",
                data=body,
                headers={key: value for key, value in self.headers.items() if key.lower() != "host"},
                timeout=30,
            )
            content = response.content
            if self.path.endswith(("/temporary-table-credentials", "/temporary-path-credentials")):
                request = json.loads(body)
                identity = json.dumps(request, sort_keys=True)
                with lock:
                    vending_requests.append(request)
                    attempts[identity] += 1
                    first = attempts[identity] == 1
                if response.ok and credential_mode == "refresh" and first:
                    payload = response.json()
                    payload["aws_temp_credentials"]["access_key_id"] = INVALID_KEY
                    payload["expiration_time"] = 0
                    content = json.dumps(payload).encode()
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        do_GET = forward  # noqa: N815
        do_POST = forward  # noqa: N815
        do_DELETE = forward  # noqa: N815
        do_PATCH = forward  # noqa: N815

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api/2.1/unity-catalog"
        if credential_mode == "refresh":
            assert max(attempts.values()) >= MIN_REFRESH_REQUESTS
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def remote(
    unity_rest_url: str,
    unity_catalog_initialized: None,
    seaweedfs_host_endpoint: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[str, None, None]:
    del unity_catalog_initialized
    config = f'[{{name="sail",type="unity",uri="{unity_rest_url}",default_catalog="{DEFAULT_CATALOG}",token="test-catalog-token"}}]'
    empty_config = tmp_path_factory.mktemp("unity-empty-credentials") / "aws-config"
    empty_config.write_text("")
    with pytest.MonkeyPatch.context() as patch:
        for name in [
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ]:
            patch.delenv(name, raising=False)
        with spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_CATALOG__LIST": config,
                "AWS_ACCESS_KEY_ID": INVALID_KEY,
                "AWS_SECRET_ACCESS_KEY": "invalid-ambient-secret",
                "AWS_SESSION_TOKEN": "invalid-ambient-token",
                "AWS_SHARED_CREDENTIALS_FILE": str(empty_config),
                "AWS_CONFIG_FILE": str(empty_config),
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_ENDPOINT": seaweedfs_host_endpoint,
                "AWS_REGION": "us-east-1",
                "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
                "AWS_ALLOW_HTTP": "true",
                "allow_http": "true",
            }
        ) as server:
            yield server.remote


def _worker_stages(spark: SparkSession) -> int:
    return (
        spark.sql(
            "SELECT count(*) AS n FROM system.execution.stages WHERE placement = 'Worker' AND status = 'INACTIVE'"
        )
        .first()
        .n
    )


def _table_info(endpoint: str, name: str) -> dict:
    response = requests.get(f"{endpoint}/tables/{DEFAULT_CATALOG}.{SCHEMA}.{name}", timeout=30)
    response.raise_for_status()
    return response.json()


def _log_actions(s3, location: str) -> list[dict]:
    prefix = urlsplit(location).path.strip("/") + "/_delta_log/"
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    keys = sorted(item["Key"] for item in objects if re.fullmatch(r"\d{20}\.json", item["Key"].removeprefix(prefix)))
    assert keys
    return [
        json.loads(line) for key in keys for line in s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().splitlines()
    ]


@pytest.fixture(scope="module")
def http_seed_url() -> Generator[str, None, None]:
    data = b"1,a\n2,b\n"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def respond(self, *, include_body: bool) -> None:
            start, end = 0, len(data) - 1
            byte_range = self.headers.get("Range")
            if byte_range:
                match = re.fullmatch(r"bytes=(\d+)-(\d*)", byte_range)
                assert match
                start = int(match[1])
                end = int(match[2]) if match[2] else end
            self.send_response(206 if byte_range else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Content-Type", "text/csv")
            self.send_header("Last-Modified", "Mon, 07 Sep 2026 00:00:00 GMT")
            if byte_range:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            if include_body:
                self.wfile.write(data[start : end + 1])

        def do_HEAD(self) -> None:
            self.respond(include_body=False)

        def do_GET(self) -> None:
            self.respond(include_body=True)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/seed.csv"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_delta_eager_and_replay_scans_use_worker_credentials(spark: SparkSession, vending_requests: list[dict]) -> None:
    before = _worker_stages(spark)
    rows = spark.sql(
        f"SELECT a.id, b.name FROM {SEED} a JOIN sail.{SCHEMA}.seed_eager b ON a.id=b.id ORDER BY a.id"
    ).collect()
    assert [(row.id, row.name) for row in rows] == SEED_ROWS
    assert _worker_stages(spark) > before
    assert any(request["operation"] == "READ" for request in vending_requests)


def test_managed_delta_create_write_and_row_operations(
    spark: SparkSession, unity_rest_url: str, vending_requests: list[dict], s3, http_seed_url: str
) -> None:
    name = "managed_" + uuid.uuid4().hex[:8]
    table = f"sail.{SCHEMA}.{name}"
    before = _worker_stages(spark)
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, name STRING) USING delta TBLPROPERTIES ('delta.enableDeletionVectors'='true')"
        )
        spark.read.schema("id LONG, name STRING").csv(http_seed_url).writeTo(table).append()
        assert [(row.id, row.name) for row in spark.table(table).orderBy("id").collect()] == SEED_ROWS
        spark.sql(f"INSERT OVERWRITE TABLE {table} VALUES (1L, 'a'), (2L, 'b'), (3L, 'c')")
        spark.sql(f"UPDATE {table} SET name='updated' WHERE id=1L")
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT 3L AS id, 'merged' AS name) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET *"
        )
        spark.sql(f"DELETE FROM {table} WHERE id=2L")
        assert [(row.id, row.name) for row in spark.table(table).orderBy("id").collect()] == [
            (1, "updated"),
            (3, "merged"),
        ]
        assert _worker_stages(spark) > before
        info = _table_info(unity_rest_url, name)
        actions = _log_actions(s3, info["storage_location"])
        metadata = next(action["metaData"] for action in actions if "metaData" in action)
        assert metadata["configuration"]["io.unitycatalog.tableId"] == info["table_id"]
        assert any(action.get("add", {}).get("deletionVector") for action in actions)
        assert any(
            request.get("table_id") == info["table_id"] and request["operation"] == "READ_WRITE"
            for request in vending_requests
        )
        response = requests.get(
            f"{unity_rest_url}/delta/preview/commits",
            json={
                "table_id": info["table_id"],
                "table_uri": info["storage_location"],
                "start_version": 1,
            },
            timeout=30,
        )
        response.raise_for_status()
        assert response.json()["commits"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("external", [False, True], ids=["managed", "external"])
def test_delta_ctas_keeps_source_and_target_access(
    spark: SparkSession, unity_rest_url: str, vending_requests: list[dict], s3, *, external: bool
) -> None:
    name = "ctas_" + uuid.uuid4().hex[:8]
    table = f"sail.{SCHEMA}.{name}"
    location = f"s3://{BUCKET}/external/{name}"
    location_clause = f"LOCATION '{location}'" if external else ""
    try:
        spark.sql(f"CREATE TABLE {table} USING delta {location_clause} AS SELECT * FROM {SEED}")
        assert [(row.id, row.name) for row in spark.table(table).orderBy("id").collect()] == SEED_ROWS
        assert spark.table(SEED).count() == len(SEED_ROWS)
        info = _table_info(unity_rest_url, name)
        assert any("add" in action for action in _log_actions(s3, info["storage_location"]))
        if external:
            assert any(
                request.get("url") == location and request["operation"] == "PATH_CREATE_TABLE"
                for request in vending_requests
            )
            before = _log_actions(s3, location)
            with pytest.raises(
                Exception, match="SET/UNSET TBLPROPERTIES is not yet supported for catalog-managed Delta tables"
            ):
                spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('vending.check'='updated')")
            assert _log_actions(s3, location) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
