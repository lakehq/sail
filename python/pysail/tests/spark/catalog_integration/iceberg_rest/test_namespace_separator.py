from __future__ import annotations

import contextlib
import http.client
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


_UNIT_SEPARATOR_PERCENT = "%1F"
_CLIENT_SEPARATOR = "."
_SERVER_SEPARATOR = "\x1F"


def _proxy_endpoint(server: ThreadingHTTPServer) -> str:
    host, port = server.server_address
    # Bind to 127.0.0.1 explicitly so Spark Connect can reach it reliably.
    return f"http://{host}:{port}"


def _start_iceberg_separator_proxy(
    *,
    target_endpoint: str,
    inject_server_config: bool,
) -> ThreadingHTTPServer:
    """Start a local proxy that enforces a non-default namespace separator.

    - Rejects requests that use the default Iceberg separator (`%1F`) so the test
      fails if Sail ignores the configured separator.
    - Translates `.` into `%1F` when forwarding to the Iceberg REST fixture so we
      don't need the fixture itself to support custom separators.
    - Optionally injects `rest.namespace.separator="."` into `/v1/config` so the
      Sail client learns the separator via the standard config endpoint.
    """

    target = urllib.parse.urlsplit(target_endpoint)
    if target.scheme != "http":
        raise ValueError(f"unsupported scheme for proxy target: {target_endpoint}")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", "0"))
            return self.rfile.read(length) if length else b""

        def _reject_default_separator(self) -> None:
            self.send_response(400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            body = b"namespace separator must not use %1F in this test\n"
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _rewrite(self, path: str) -> str:
            parts = urllib.parse.urlsplit(path)

            if _UNIT_SEPARATOR_PERCENT.lower() in parts.path.lower():
                raise ValueError("default separator present in path")

            if parts.query and _UNIT_SEPARATOR_PERCENT.lower() in parts.query.lower():
                raise ValueError("default separator present in query")

            rewritten_path = parts.path
            if rewritten_path.startswith("/v1/namespaces/"):
                prefix = "/v1/namespaces/"
                remainder = rewritten_path[len(prefix) :]
                namespace_segment, sep, rest = remainder.partition("/")
                # The segment is already percent-encoded by the client. Replace the configured
                # client separator directly in this raw segment so the forwarded request uses
                # the encoded Unit Separator expected by the fixture.
                namespace_segment = namespace_segment.replace(_CLIENT_SEPARATOR, _UNIT_SEPARATOR_PERCENT)
                rewritten_path = f"{prefix}{namespace_segment}{sep}{rest}"

            query_pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
            rewritten_pairs: list[tuple[str, str]] = []
            for key, value in query_pairs:
                if key == "parent":
                    value = value.replace(_CLIENT_SEPARATOR, _SERVER_SEPARATOR)
                rewritten_pairs.append((key, value))
            rewritten_query = urllib.parse.urlencode(rewritten_pairs)

            return urllib.parse.urlunsplit(
                (parts.scheme, parts.netloc, rewritten_path, rewritten_query, parts.fragment)
            )

        def _forward(self) -> None:
            if self.path.startswith("/v1/config") and inject_server_config and self.command == "GET":
                conn = http.client.HTTPConnection(target.hostname, target.port, timeout=30)
                conn.request("GET", "/v1/config")
                resp = conn.getresponse()
                data = resp.read()
                if resp.status != 200:
                    self.send_response(resp.status)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return

                config = json.loads(data.decode("utf-8"))
                config.setdefault("overrides", {})["rest.namespace.separator"] = _CLIENT_SEPARATOR
                encoded = json.dumps(config).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return

            try:
                rewritten = self._rewrite(self.path)
            except ValueError:
                self._reject_default_separator()
                return

            conn = http.client.HTTPConnection(target.hostname, target.port, timeout=30)

            headers = {k: v for k, v in self.headers.items()}
            headers.pop("Host", None)
            body = self._read_body()
            conn.request(self.command, rewritten, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()

            self.send_response(resp.status)
            for key, value in resp.getheaders():
                # Hop-by-hop headers are not valid to forward.
                if key.lower() in {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}:
                    continue
                if key.lower() == "content-length":
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._forward()

        def do_POST(self) -> None:  # noqa: N802
            self._forward()

        def do_DELETE(self) -> None:  # noqa: N802
            self._forward()

        def do_HEAD(self) -> None:  # noqa: N802
            self._forward()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            # Keep integration test output clean.
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


@pytest.fixture(scope="module")
def iceberg_rest_proxy_endpoint_configured(iceberg_rest_endpoint: str) -> Generator[str, None, None]:
    """Proxy endpoint that injects `rest.namespace.separator` via `/v1/config`."""
    server = _start_iceberg_separator_proxy(
        target_endpoint=iceberg_rest_endpoint,
        inject_server_config=True,
    )
    try:
        yield _proxy_endpoint(server)
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="module")
def iceberg_rest_proxy_endpoint_passthrough(iceberg_rest_endpoint: str) -> Generator[str, None, None]:
    """Proxy endpoint that does not modify `/v1/config`."""
    server = _start_iceberg_separator_proxy(
        target_endpoint=iceberg_rest_endpoint,
        inject_server_config=False,
    )
    try:
        yield _proxy_endpoint(server)
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="module")
def iceberg_spark_server_config_separator(
    iceberg_rest_proxy_endpoint_configured: str,
) -> Generator[SparkSession, None, None]:
    catalog_config = (
        f'[{{name="sail", type="iceberg-rest", uri="{iceberg_rest_proxy_endpoint_configured}"}}]'
    )
    server, remote, saved_env = start_sail_server(catalog_list=catalog_config)
    spark = create_spark_session(remote, "iceberg_rest_namespace_separator_server_config")
    try:
        yield spark
    finally:
        with contextlib.suppress(Exception):
            spark.stop()
        stop_sail_server(server, saved_env)


@pytest.fixture(scope="module")
def iceberg_spark_client_config_separator(
    iceberg_rest_proxy_endpoint_passthrough: str,
) -> Generator[SparkSession, None, None]:
    catalog_config = (
        f'[{{name="sail", type="iceberg-rest", uri="{iceberg_rest_proxy_endpoint_passthrough}", '
        f'"rest.namespace.separator"="{_CLIENT_SEPARATOR}"}}]'
    )
    server, remote, saved_env = start_sail_server(catalog_list=catalog_config)
    spark = create_spark_session(remote, "iceberg_rest_namespace_separator_client_config")
    try:
        yield spark
    finally:
        with contextlib.suppress(Exception):
            spark.stop()
        stop_sail_server(server, saved_env)


@pytest.mark.parametrize(
    "spark_fixture",
    [
        pytest.param("iceberg_spark_server_config_separator", id="server_config"),
        pytest.param("iceberg_spark_client_config_separator", id="client_config"),
    ],
)
def test_namespace_separator_allows_multi_part_namespaces(
    request: pytest.FixtureRequest,
    spark_fixture: str,
) -> None:
    spark: SparkSession = request.getfixturevalue(spark_fixture)

    base = f"iceberg_ns_sep_{threading.get_ident()}"
    mid = "mid"
    leaf1 = "leaf1"
    leaf2 = "leaf2"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {base}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {base}.{mid}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {base}.{mid}.{leaf1}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {base}.{mid}.{leaf2}")

    # Exercise a GET /namespaces/{namespace} call where {namespace} is multipart.
    spark.sql(f"DESCRIBE DATABASE {base}.{mid}").collect()

    # Exercise GET /namespaces?parent=... where parent is multipart.
    rows = spark.sql(f"SHOW DATABASES LIKE '{base}.{mid}.%'").collect()
    names = {row.name for row in rows}
    assert f"{base}.{mid}.{leaf1}" in names
    assert f"{base}.{mid}.{leaf2}" in names

    # Exercise CASCADE path where list_tables/list_views/drop_namespace all need the separator.
    spark.sql(f"DROP DATABASE IF EXISTS {base}.{mid}.{leaf1} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {base}.{mid}.{leaf2} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {base}.{mid} CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS {base} CASCADE")
