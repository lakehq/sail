# SPDX-License-Identifier: Apache-2.0

"""Reading an Iceberg REST table loads the table once per statement."""

from __future__ import annotations

import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest
import requests

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

NAMESPACE = "load_table_requests"
TABLE_PATH = f"/v1/namespaces/{NAMESPACE}/tables/"
ACCESS_DELEGATION = "X-Iceberg-Access-Delegation"


@pytest.fixture(scope="module")
def recording_catalog(iceberg_rest_endpoint: str) -> Generator[tuple[str, list], None, None]:
    """Forward requests to the REST catalog and record each method, path, and delegation header."""
    records = []

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("content-length", "0")))
            headers = {"Content-Type": "application/json"}
            if ACCESS_DELEGATION in self.headers:
                headers[ACCESS_DELEGATION] = self.headers[ACCESS_DELEGATION]
            response = requests.request(
                self.command,
                iceberg_rest_endpoint + self.path,
                data=body or None,
                headers=headers,
                timeout=30,
            )
            records.append((self.command, urllib.parse.unquote(self.path), self.headers.get(ACCESS_DELEGATION)))
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response.content)

        do_GET = forward  # noqa: N815
        do_POST = forward  # noqa: N815
        do_DELETE = forward  # noqa: N815
        do_HEAD = forward  # noqa: N815

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", records
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.fixture(scope="module")
def remote(recording_catalog, seaweedfs_host_endpoint) -> Generator[str, None, None]:
    endpoint, _ = recording_catalog
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": f'[{{name="sail", type="iceberg-rest", uri="{endpoint}"}}]',
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        }
    ) as server:
        yield server.remote


def _table_loads(records, start):
    return [
        (path.removeprefix(TABLE_PATH), header)
        for method, path, header in records[start:]
        if method == "GET" and path.startswith(TABLE_PATH)
    ]


def test_read_loads_each_table_once_per_statement(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    first = f"{NAMESPACE}.first"
    second = f"{NAMESPACE}.second"
    try:
        spark.sql(f"CREATE TABLE {first} (id BIGINT) USING iceberg")
        spark.sql(f"CREATE TABLE {second} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {first} VALUES (1), (2)")
        spark.sql(f"INSERT INTO {second} VALUES (2), (3)")

        start = len(records)
        assert [row.id for row in spark.read.table(first).orderBy("id").collect()] == [1, 2]
        assert _table_loads(records, start) == [("first", "vended-credentials")]

        start = len(records)
        # Repeated references to a table also load it once.
        rows = (
            spark.read.table(first)
            .alias("a")
            .join(spark.read.table(first).alias("b"), "id")
            .join(spark.read.table(first).alias("c"), "id")
            .join(spark.read.table(second), "id")
            .collect()
        )
        assert [row.id for row in rows] == [2]
        assert sorted(_table_loads(records, start)) == [
            ("first", "vended-credentials"),
            ("second", "vended-credentials"),
        ]

        # Each statement loads the table again and sees its latest version.
        spark.sql(f"INSERT INTO {first} VALUES (3)")
        start = len(records)
        assert [row.id for row in spark.read.table(first).orderBy("id").collect()] == [1, 2, 3]
        assert _table_loads(records, start) == [("first", "vended-credentials")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {first}")
        spark.sql(f"DROP TABLE IF EXISTS {second}")
