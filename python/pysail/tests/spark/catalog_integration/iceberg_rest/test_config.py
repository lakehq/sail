from __future__ import annotations

import contextlib
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Self

import pytest

from pysail.tests.spark.catalog_integration.conftest import (
    create_spark_session,
    start_sail_server,
    stop_sail_server,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from types import TracebackType

    from pyspark.sql import SparkSession


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _load_namespace(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
    *,
    prefix: str | None = None,
) -> dict[str, object]:
    namespace = _quote(separator.join(namespace_parts))
    prefix_path = f"/{_quote(prefix)}" if prefix is not None else ""
    url = f"{iceberg_rest_endpoint}/v1{prefix_path}/namespaces/{namespace}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


@contextlib.contextmanager
def _spark_with_catalog(
    catalog_endpoint: str,
    catalog_options: str = "",
    *,
    app_name: str,
) -> Generator[SparkSession, None, None]:
    options = f", {catalog_options}" if catalog_options else ""
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{catalog_endpoint}"{options}}}]'
    server, remote, saved_env = start_sail_server(catalog_list=catalog_config)
    spark = create_spark_session(remote, app_name)
    try:
        yield spark
    finally:
        with contextlib.suppress(Exception):
            spark.stop()
        stop_sail_server(server, saved_env)


def _exercise_namespace(
    spark: SparkSession,
    catalog_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
    prefix: str | None = None,
) -> None:
    parent = f"iceberg_rest_config_{namespace_id}"
    child = "child"
    namespace = f"{parent}.{child}"

    spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")
    try:
        spark.sql(f"CREATE DATABASE {parent}")
        spark.sql(f"CREATE DATABASE {namespace}")

        rows = {row.info_name: row.info_value for row in spark.sql(f"DESCRIBE DATABASE {namespace}").collect()}
        assert rows["Namespace Name"] == namespace
        assert _load_namespace(catalog_endpoint, [parent, child], separator, prefix=prefix)["namespace"] == [
            parent,
            child,
        ]
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")


# The Apache Iceberg REST fixture hardcodes /v1/config to return empty defaults and overrides
# regardless of any CATALOG_* env vars (see RESTCatalogAdapter.java). No off-the-shelf catalog
# exposes per-test defaults/overrides knobs, so this proxy is the only way to drive the full
# merge matrix (server defaults x client config x server overrides) in an integration test.
class _ConfigProxy:
    def __init__(
        self,
        target_endpoint: str,
        *,
        defaults: dict[str, str] | None = None,
        overrides: dict[str, str] | None = None,
        expected_prefix: str | None = None,
        namespace_separator: str | None = None,
        expected_config_warehouse: str | None = None,
        reject_catalog_requests: bool = False,
    ) -> None:
        self._target_endpoint = target_endpoint.rstrip("/")
        self._defaults = defaults or {}
        self._overrides = overrides or {}
        self._expected_prefix = expected_prefix
        self._namespace_separator = namespace_separator
        self._expected_config_warehouse = expected_config_warehouse
        self._reject_catalog_requests = reject_catalog_requests
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        if self._server is None:
            message = "proxy server is not running"
            raise RuntimeError(message)
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> Self:
        target_endpoint = self._target_endpoint
        defaults = self._defaults
        overrides = self._overrides
        expected_prefix = self._expected_prefix
        namespace_separator = self._namespace_separator
        expected_config_warehouse = self._expected_config_warehouse
        reject_catalog_requests = self._reject_catalog_requests

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self._handle()

            def do_POST(self) -> None:
                self._handle()

            def do_DELETE(self) -> None:
                self._handle()

            def do_HEAD(self) -> None:
                self._handle()

            def log_message(self, fmt: str, *args: object) -> None:
                pass

            def _handle(self) -> None:
                parsed = urllib.parse.urlsplit(self.path)
                if parsed.path == "/v1/config":
                    query = urllib.parse.parse_qs(parsed.query)
                    warehouse = query.get("warehouse", [None])[0]
                    if warehouse != expected_config_warehouse:
                        self._send_json(400, {"error": f"unexpected warehouse query: {warehouse!r}"})
                        return
                    self._send_json(200, {"defaults": defaults, "overrides": overrides})
                    return

                if reject_catalog_requests:
                    self._send_json(502, {"error": "catalog requests must use the uri override"})
                    return

                path = parsed.path
                if expected_prefix is not None:
                    prefix_path = f"/v1/{_quote(expected_prefix)}"
                    if not (path == prefix_path or path.startswith(f"{prefix_path}/")):
                        self._send_json(404, {"error": f"missing expected prefix {expected_prefix!r}"})
                        return
                    path = f"/v1{path[len(prefix_path) :]}"

                query = parsed.query
                if namespace_separator is not None:
                    path = self._rewrite_namespace_path(path)
                    query = self._rewrite_parent_query(query)

                forwarded_path = urllib.parse.urlunsplit(("", "", path, query, parsed.fragment))
                self._forward(forwarded_path)

            def _rewrite_namespace_path(self, path: str) -> str:
                namespace_prefix = "/v1/namespaces/"
                if not path.startswith(namespace_prefix):
                    return path

                rest = path.removeprefix(namespace_prefix)
                namespace, separator, suffix = rest.partition("/")
                decoded_namespace = urllib.parse.unquote(namespace)
                rewritten_namespace = self._rewrite_namespace(decoded_namespace)
                return f"{namespace_prefix}{_quote(rewritten_namespace)}{separator}{suffix}"

            def _rewrite_parent_query(self, query: str) -> str:
                params = urllib.parse.parse_qsl(query, keep_blank_values=True)
                rewritten = [
                    (key, self._rewrite_namespace(value) if key == "parent" else value) for key, value in params
                ]
                return urllib.parse.urlencode(rewritten)

            def _rewrite_namespace(self, namespace: str) -> str:
                return namespace.replace(namespace_separator, "\x1f")

            def _forward(self, path: str) -> None:
                body = None
                content_length = self.headers.get("content-length")
                if content_length is not None:
                    body = self.rfile.read(int(content_length))

                request = urllib.request.Request(  # noqa: S310
                    f"{target_endpoint}{path}",
                    data=body,
                    method=self.command,
                )
                for key, value in self.headers.items():
                    if key.lower() not in {"host", "content-length", "connection"}:
                        request.add_header(key, value)

                try:
                    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
                    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                        response_body = response.read()
                        self.send_response(response.status)
                        for key, value in response.headers.items():
                            if key.lower() not in {"transfer-encoding", "connection"}:
                                self.send_header(key, value)
                        self.end_headers()
                        if self.command != "HEAD":
                            self.wfile.write(response_body)
                except urllib.error.HTTPError as error:
                    error_body = error.read()
                    self.send_response(error.code)
                    for key, value in error.headers.items():
                        if key.lower() not in {"transfer-encoding", "connection"}:
                            self.send_header(key, value)
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(error_body)

            def _send_json(self, status: int, value: dict[str, object]) -> None:
                body = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=30)


def test_default_config_without_client_values_or_server_overrides(
    iceberg_rest_endpoint: str,
) -> None:
    with _spark_with_catalog(
        iceberg_rest_endpoint,
        app_name="iceberg_rest_config_defaults",
    ) as spark:
        _exercise_namespace(
            spark,
            iceberg_rest_endpoint,
            namespace_id="defaults",
            separator="\x1f",
        )


def test_client_prefix_config_used_without_server_override(
    iceberg_rest_endpoint: str,
) -> None:
    prefix = "client_prefix"

    with (
        _ConfigProxy(iceberg_rest_endpoint, expected_prefix=prefix) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'prefix="{prefix}"',
            app_name="iceberg_rest_config_client_prefix",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="client_prefix",
            separator="\x1f",
            prefix=prefix,
        )


def test_server_prefix_default_used_without_client_config_or_server_override(
    iceberg_rest_endpoint: str,
) -> None:
    prefix = "default_prefix"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            defaults={"prefix": prefix},
            expected_prefix=prefix,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            app_name="iceberg_rest_config_default_prefix",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="default_prefix",
            separator="\x1f",
            prefix=prefix,
        )


def test_client_prefix_config_wins_over_server_default(
    iceberg_rest_endpoint: str,
) -> None:
    default_prefix = "wrong_default_prefix"
    client_prefix = "client_prefix_over_default"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            defaults={"prefix": default_prefix},
            expected_prefix=client_prefix,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'prefix="{client_prefix}"',
            app_name="iceberg_rest_config_client_prefix_default",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="client_prefix_default",
            separator="\x1f",
            prefix=client_prefix,
        )


def test_server_prefix_override_wins_over_client_config(
    iceberg_rest_endpoint: str,
) -> None:
    client_prefix = "wrong_client_prefix"
    server_prefix = "server_prefix"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            overrides={"prefix": server_prefix},
            expected_prefix=server_prefix,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'prefix="{client_prefix}"',
            app_name="iceberg_rest_config_server_prefix",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="server_prefix",
            separator="\x1f",
            prefix=server_prefix,
        )


def test_server_namespace_separator_default_used_without_client_config_or_server_override(
    iceberg_rest_endpoint: str,
) -> None:
    separator = "::"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            defaults={"namespace-separator": separator},
            namespace_separator=separator,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            app_name="iceberg_rest_config_default_separator",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="default_separator",
            separator=separator,
        )


def test_client_namespace_separator_config_wins_over_server_default(
    iceberg_rest_endpoint: str,
) -> None:
    default_separator = "/"
    client_separator = "::"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            defaults={"namespace-separator": default_separator},
            namespace_separator=client_separator,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'namespace_separator="{client_separator}"',
            app_name="iceberg_rest_config_client_separator_default",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="client_separator_default",
            separator=client_separator,
        )


def test_client_namespace_separator_config_used_without_server_override(
    iceberg_rest_endpoint: str,
) -> None:
    separator = "::"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            namespace_separator=separator,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'namespace_separator="{separator}"',
            app_name="iceberg_rest_config_client_separator",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="client_separator",
            separator=separator,
        )


def test_server_namespace_separator_override_wins_over_client_config(
    iceberg_rest_endpoint: str,
) -> None:
    client_separator = "/"
    server_separator = "::"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            overrides={"namespace-separator": server_separator},
            namespace_separator=server_separator,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'namespace_separator="{client_separator}"',
            app_name="iceberg_rest_config_server_separator",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="server_separator",
            separator=server_separator,
        )


def test_server_uri_override_redirects_catalog_requests(
    iceberg_rest_endpoint: str,
) -> None:
    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            overrides={"uri": iceberg_rest_endpoint},
            reject_catalog_requests=True,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            app_name="iceberg_rest_config_server_uri",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            iceberg_rest_endpoint,
            namespace_id="server_uri",
            separator="\x1f",
        )


@pytest.mark.parametrize(
    ("catalog_options", "expected_warehouse"),
    [
        ("", None),
        ('warehouse="s3://client/warehouse"', "s3://client/warehouse"),
    ],
)
def test_config_request_uses_client_warehouse_query(
    iceberg_rest_endpoint: str,
    catalog_options: str,
    expected_warehouse: str | None,
) -> None:
    namespace_id = "warehouse_default" if expected_warehouse is None else "warehouse_client"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            expected_config_warehouse=expected_warehouse,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            catalog_options,
            app_name=f"iceberg_rest_config_{namespace_id}",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id=namespace_id,
            separator="\x1f",
        )


def test_server_warehouse_override_does_not_change_initial_config_query(
    iceberg_rest_endpoint: str,
) -> None:
    client_warehouse = "s3://client/warehouse"
    server_warehouse = "s3://server/warehouse"

    with (
        _ConfigProxy(
            iceberg_rest_endpoint,
            overrides={"warehouse": server_warehouse},
            expected_config_warehouse=client_warehouse,
        ) as proxy,
        _spark_with_catalog(
            proxy.endpoint,
            f'warehouse="{client_warehouse}"',
            app_name="iceberg_rest_config_server_warehouse",
        ) as spark,
    ):
        _exercise_namespace(
            spark,
            proxy.endpoint,
            namespace_id="server_warehouse",
            separator="\x1f",
        )
