from __future__ import annotations

import contextlib
import json
import threading
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from collections.abc import Generator


class _MockRestCatalogState:
    def __init__(self, table_name: str, metadata_location: str, metadata: dict[str, object]) -> None:
        self.table_name = table_name
        self.load_result = {
            "metadata-location": metadata_location,
            "metadata": metadata,
            "config": {"scan-planning-mode": "server"},
        }
        self.table_requests: list[tuple[str, str, str | None]] = []


def _mock_rest_catalog_handler(state: _MockRestCatalogState) -> type[BaseHTTPRequestHandler]:
    class MockRestCatalogHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            path = urllib.parse.urlsplit(self.path).path
            if path == "/v1/config":
                self._send_json(200, {"defaults": {}, "overrides": {}})
                return

            prefix = "/v1/namespaces/"
            table_separator = "/tables/"
            if path.startswith(prefix) and table_separator in path:
                namespace, table = path.removeprefix(prefix).split(table_separator, maxsplit=1)
                namespace = urllib.parse.unquote(namespace)
                table = urllib.parse.unquote(table)
                delegation = self.headers.get("X-Iceberg-Access-Delegation")
                state.table_requests.append((namespace, table, delegation))
                if namespace == "default" and table == state.table_name:
                    self._send_json(200, state.load_result)
                    return

            self._send_json(
                404,
                {
                    "error": {
                        "message": f"table not found: {path}",
                        "type": "NoSuchTableException",
                        "code": 404,
                    }
                },
            )

        def _send_json(self, status: int, payload: object) -> None:
            encoded = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return MockRestCatalogHandler


@contextlib.contextmanager
def _mock_rest_catalog(
    table_name: str,
    metadata_location: str,
    metadata: dict[str, object],
) -> Generator[tuple[str, _MockRestCatalogState], None, None]:
    state = _MockRestCatalogState(table_name, metadata_location, metadata)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _mock_rest_catalog_handler(state))
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def test_rest_catalog_metadata_table_uses_metadata_only_access(spark, tmp_path):
    table_name = "iceberg_rest_metadata_relation_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608
        metadata_location = spark.sql(
            f"""
            SELECT file
            FROM {table_name}.metadata_log_entries
            ORDER BY timestamp DESC, file DESC
            LIMIT 1
            """  # noqa: S608
        ).first()[0]
        # Safe: the URI comes from the local Iceberg table created by this test.
        with urllib.request.urlopen(metadata_location, timeout=30) as response:  # noqa: S310
            metadata = json.load(response)

        with _mock_rest_catalog(table_name, metadata_location, metadata) as (endpoint, state):
            catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{endpoint}"}}]'
            with (
                spark_connect_server(
                    envs={
                        "SAIL_CATALOG__LIST": catalog_config,
                        "SAIL_CATALOG__DEFAULT_CATALOG": "sail",
                    }
                ) as server,
                spark_session_factory(server.remote) as sessions,
            ):
                rest_spark = sessions.create()
                snapshots = rest_spark.sql(
                    f"SELECT operation FROM {table_name}.snapshots"  # noqa: S608
                ).collect()
                assert [row.operation for row in snapshots] == ["append"]

                with pytest.raises(Exception, match="requires server-side scan planning"):
                    rest_spark.sql(f"SELECT * FROM {table_name}").collect()  # noqa: S608

        requested_tables = [table for _, table, _ in state.table_requests]
        assert "snapshots" in requested_tables
        assert table_name in requested_tables
        assert requested_tables.index("snapshots") < requested_tables.index(table_name)
        assert any(delegation == "vended-credentials" for _, _, delegation in state.table_requests)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
