from __future__ import annotations

import json
import threading
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest
import requests

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_commit_state_unknown_test"


@dataclass
class CommitFaultState:
    armed: bool = False
    injected: int = 0


def _proxy_handler(upstream: str, state: CommitFaultState):
    class CommitFaultProxy(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self._forward()

        def do_POST(self) -> None:  # noqa: N802
            self._forward()

        def do_DELETE(self) -> None:  # noqa: N802
            self._forward()

        def _forward(self) -> None:
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length) if length else None
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
                data=body,
                headers=headers,
                allow_redirects=False,
                timeout=30,
            )

            if self.command == "POST" and state.armed:
                state.armed = False
                state.injected += 1
                payload = json.dumps(
                    {
                        "error": {
                            "message": "response lost after commit",
                            "type": "CommitStateUnknownException",
                            "code": 504,
                        }
                    }
                ).encode()
                self.send_response(504)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

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
            self.wfile.write(response.content)

        def log_message(self, _format: str, *_args) -> None:
            return

    return CommitFaultProxy


@pytest.fixture(scope="module")
def commit_fault_proxy(
    iceberg_rest_endpoint: str,
) -> Generator[tuple[str, CommitFaultState], None, None]:
    state = CommitFaultState()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _proxy_handler(iceberg_rest_endpoint, state),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def remote(
    commit_fault_proxy: tuple[str, CommitFaultState],
    seaweedfs_host_endpoint: str,
) -> Generator[str, None, None]:
    proxy_endpoint, _ = commit_fault_proxy
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
    response = requests.get(
        f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/tables/{table}",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def test_commit_reconciles_after_the_server_applies_a_request_but_returns_504(
    spark: SparkSession,
    commit_fault_proxy: tuple[str, CommitFaultState],
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "reconciled_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          value STRING
        )
        USING iceberg
        """
    )

    _, fault = commit_fault_proxy
    fault.armed = True
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'committed')")  # noqa: S608

    assert fault.injected == 1
    metadata = _load_table(iceberg_rest_endpoint, table_name)["metadata"]
    assert metadata["current-snapshot-id"] not in (None, -1)
    assert len(metadata["snapshots"]) == 1
    rows = spark.sql(f"SELECT id, value FROM {table_fqn}").collect()  # noqa: S608
    assert [(row.id, row.value) for row in rows] == [(1, "committed")]
