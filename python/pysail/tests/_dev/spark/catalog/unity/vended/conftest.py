# SPDX-License-Identifier: Apache-2.0

"""Pytest fixtures for Unity Catalog managed Delta tables on storage that only vended credentials reach.

The storage is an S3-compatible object store whose own security token service issues temporary
credentials. Unity Catalog asks that service for the credentials of a table, limited to the table
location and to reading or writing, and Sail gets them from Unity Catalog. Sail has no storage
credentials of its own, so every storage request that succeeds used credentials that the catalog issued.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl

import boto3
import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy

from pysail.testing._dev.containers.unity_defaults import DEFAULT_CATALOG
from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from pyspark.sql import SparkSession

_STORE_IMAGE = "pgsty/silo:RELEASE.2026-09-03T13-18-01Z"
_UNITY_IMAGE = "unitycatalog/unitycatalog:v0.7.0"
_STORE_USER = "storeadmin"
_STORE_PASSWORD = "storepassword"  # noqa: S105
_BUCKET = "unity"
_REGION = "us-east-1"
_HOST = "host.docker.internal"
# The shortest lifetime of the credentials that the object store issues.
_LIFETIME_SECONDS = 900
# The time to wait for the first report of the object store to the test.
_HOOK_TIMEOUT_SECONDS = 30
_ENVIRONMENT_CREDENTIALS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)


@dataclass
class VendedStorage:
    """The object store, the credentials issued for it, and the requests that it received."""

    endpoint: str = ""
    # The address at which the containers reach the test.
    hook: str = ""
    # The lifetime that Unity Catalog is told for the credentials that are issued from now on.
    # The credentials are valid for longer, so that a test can make Sail replace them early.
    reported_lifetime: timedelta = timedelta(seconds=_LIFETIME_SECONDS)
    # Whether the credentials issued for writing from now on allow only reading.
    deny_writes: bool = False
    # The access key, the allowed actions and the allowed resources of each credential issued.
    issued: list[tuple[str, frozenset[str], tuple[str, ...]]] = field(default_factory=list)
    # The operation, the object, the status and the access key of each storage request.
    requests: list[tuple[str, str, int, str]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def client(self, service: str):
        return boto3.client(
            service,
            endpoint_url=self.endpoint,
            aws_access_key_id=_STORE_USER,
            aws_secret_access_key=_STORE_PASSWORD,
            region_name=_REGION,
        )

    def put(self, key: str, body: bytes) -> str:
        """Writes an object with the credentials of the administrator, and returns its location."""
        self.client("s3").put_object(Bucket=_BUCKET, Key=key, Body=body)
        return f"s3://{_BUCKET}/{key}"

    def issue(self, form: dict[str, str]) -> str:
        """Answers an `AssumeRole` request of Unity Catalog with credentials of the object store."""
        policy = json.loads(form["Policy"])
        # The object store does not know the condition of the statement for key management,
        # which Unity Catalog adds for AWS.
        policy["Statement"] = [
            x for x in policy["Statement"] if not any(action.startswith("kms:") for action in x["Action"])
        ]
        if self.deny_writes:
            for statement in policy["Statement"]:
                statement["Action"] = [x for x in statement["Action"] if x in ("s3:GetObject", "s3:ListBucket")]
        credentials = self.client("sts").assume_role(
            RoleArn=form["RoleArn"],
            RoleSessionName=form["RoleSessionName"],
            Policy=json.dumps(policy),
            DurationSeconds=_LIFETIME_SECONDS,
        )["Credentials"]
        actions = frozenset(action for x in policy["Statement"] for action in x["Action"])
        resources = tuple(r for x in policy["Statement"] for r in x["Resource"] if r.endswith("/*"))
        with self.lock:
            self.issued.append((credentials["AccessKeyId"], actions, resources))
        expiration = min(credentials["Expiration"], datetime.now(timezone.utc) + self.reported_lifetime)
        return (
            '<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials>'
            f"<AccessKeyId>{credentials['AccessKeyId']}</AccessKeyId>"
            f"<SecretAccessKey>{credentials['SecretAccessKey']}</SecretAccessKey>"
            f"<SessionToken>{credentials['SessionToken']}</SessionToken>"
            f"<Expiration>{expiration.strftime('%Y-%m-%dT%H:%M:%SZ')}</Expiration>"
            "</Credentials></AssumeRoleResult></AssumeRoleResponse>"
        )

    def record(self, audit: dict) -> None:
        api = audit.get("api", {})
        if api.get("bucket") == _BUCKET:
            with self.lock:
                self.requests.append(
                    (api.get("name"), api.get("object", ""), api.get("statusCode"), audit.get("accessKey"))
                )

    def settled(self) -> tuple[list, list]:
        """Returns the credentials issued and the storage requests, once no more requests are reported."""
        count = -1
        while count != len(self.requests):
            count = len(self.requests)
            time.sleep(0.5)
        with self.lock:
            return list(self.issued), list(self.requests)


def _handler(storage: VendedStorage) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            pass

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            status, answer = 200, b""
            if self.path == "/audit":
                storage.record(json.loads(body))
            else:
                try:
                    answer = storage.issue(dict(parse_qsl(body.decode()))).encode()
                except Exception as error:  # noqa: BLE001
                    status, answer = 500, str(error).encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/xml")
            self.send_header("Content-Length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

    return Handler


def _wait_for(url: str) -> None:
    for _ in range(60):
        try:
            if requests.get(url, timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    pytest.fail(f"no answer from {url}")


@pytest.fixture(scope="module")
def vended_storage() -> Generator[VendedStorage, None, None]:
    """Start the object store, which reports each request to the test."""
    storage = VendedStorage()
    server = ThreadingHTTPServer(("0.0.0.0", 0), _handler(storage))  # noqa: S104
    threading.Thread(target=server.serve_forever, daemon=True).start()
    hook = f"http://{_HOST}:{server.server_address[1]}"
    container = (
        DockerContainer(_STORE_IMAGE)
        .with_exposed_ports(9000)
        .with_env("MINIO_ROOT_USER", _STORE_USER)
        .with_env("MINIO_ROOT_PASSWORD", _STORE_PASSWORD)
        .with_env("MINIO_AUDIT_WEBHOOK_ENABLE_test", "on")
        .with_env("MINIO_AUDIT_WEBHOOK_ENDPOINT_test", f"{hook}/audit")
        .with_command("server /data")
        .with_kwargs(extra_hosts={_HOST: "host-gateway"})
        .waiting_for(HttpWaitStrategy(9000, "/minio/health/ready").with_startup_timeout(120))
    )
    with container:
        host = container.get_container_host_ip()
        host = "127.0.0.1" if host in {"localhost", "::1"} else host
        storage.endpoint = f"http://{host}:{container.get_exposed_port(9000)}"
        storage.client("s3").create_bucket(Bucket=_BUCKET)
        storage.hook = hook
        # The object store reports the request above to the test. Without the report, the
        # containers cannot reach the test, so the storage requests and the credentials
        # cannot be observed, and Unity Catalog cannot get credentials either.
        deadline = time.monotonic() + _HOOK_TIMEOUT_SECONDS
        while not storage.requests and time.monotonic() < deadline:
            time.sleep(0.5)
        if not storage.requests:
            pytest.skip(
                f"the containers cannot reach the test process at {hook} "
                "(host.docker.internal is not routed to this host, or a firewall blocks it)"
            )
        yield storage
    server.shutdown()


@pytest.fixture(scope="module")
def vended_unity_url(
    vended_storage: VendedStorage, tmp_path_factory: pytest.TempPathFactory
) -> Generator[str, None, None]:
    """Start Unity Catalog, which gets the credentials of the bucket from the object store."""
    properties: Path = tmp_path_factory.mktemp("unity_vended") / "server.properties"
    properties.write_text(
        "server.env=dev\nserver.authorization=disable\nserver.managed-table.enabled=true\n"
        f"s3.bucketPath.0=s3://{_BUCKET}\ns3.region.0={_REGION}\n"
        "s3.awsRoleArn.0=arn:aws:iam::000000000000:role/unity\n"
        "s3.accessKey.0=unity\ns3.secretKey.0=unity\n"
    )
    container = (
        DockerContainer(_UNITY_IMAGE)
        .with_exposed_ports(8080)
        .with_env("AWS_ENDPOINT_URL_STS", vended_storage.hook)
        .with_volume_mapping(str(properties), "/home/unitycatalog/etc/conf/server.properties", "ro")
        .with_kwargs(extra_hosts={_HOST: "host-gateway"})
        .waiting_for(
            LogMessageWaitStrategy(
                "###################################################################"
            ).with_startup_timeout(120)
        )
    )
    with container:
        url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8080)}/api/2.1/unity-catalog"
        _wait_for(f"{url}/catalogs")
        for path, payload in (
            ("catalogs", {"name": DEFAULT_CATALOG, "storage_root": f"s3://{_BUCKET}/root"}),
            ("schemas", {"name": "default", "catalog_name": DEFAULT_CATALOG}),
        ):
            requests.post(f"{url}/{path}", json=payload, timeout=30).raise_for_status()
        yield url


@pytest.fixture(scope="module")
def remote(vended_storage: VendedStorage, vended_unity_url: str) -> Generator[str, None, None]:
    """Start Sail server with Unity catalog and without storage credentials in the environment."""
    catalog = f'[{{name="sail", type="unity", uri="{vended_unity_url}", default_catalog="{DEFAULT_CATALOG}"}}]'
    with pytest.MonkeyPatch.context() as mp:
        for name in _ENVIRONMENT_CREDENTIALS:
            mp.delenv(name, raising=False)
        with spark_connect_server(
            envs={
                "SAIL_CATALOG__LIST": catalog,
                "UNITY_ALLOW_HTTP_URL": "true",
                "AWS_ENDPOINT": vended_storage.endpoint,
                "AWS_ALLOW_HTTP": "true",
                "AWS_REGION": _REGION,
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent/credentials",
                "AWS_CONFIG_FILE": "/nonexistent/config",
            },
        ) as server:
            yield server.remote


@pytest.fixture(scope="module", autouse=True)
def _configure_unity_catalog_spark() -> None:
    """Replaces the fixture of the other Unity Catalog tests, which is for local storage."""


@pytest.fixture
def new_session(remote: str) -> Generator[SparkSession, None, None]:
    """A session that has not fetched storage credentials yet."""
    with spark_session_factory(remote) as sessions:
        yield sessions.create()
