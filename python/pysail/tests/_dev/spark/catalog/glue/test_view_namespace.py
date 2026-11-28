# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

# The time that the lookup of the table waits, in which the session changes its database.
DELAY = 3.0


class SlowGlue:
    """A proxy in front of the Glue endpoint that delays the next request for one table once armed."""

    def __init__(self, target: str):
        self.armed: tuple[str, str] | None = None
        self.hit = threading.Event()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                if self.headers.get("X-Amz-Target", "").endswith(".GetTable") and proxy.armed is not None:
                    request = json.loads(body or b"{}")
                    if (request.get("DatabaseName"), request.get("Name")) == proxy.armed:
                        proxy.armed = None
                        proxy.hit.set()
                        time.sleep(DELAY)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
                response = requests.post(target + self.path, data=body, headers=headers, timeout=60)
                self.send_response(response.status_code)
                for key, value in response.headers.items():
                    if key.lower() not in ("content-length", "transfer-encoding", "connection", "content-encoding"):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(response.content)))
                self.end_headers()
                self.wfile.write(response.content)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture(scope="module")
def slow_glue(moto_endpoint: str) -> Generator[SlowGlue, None, None]:
    proxy = SlowGlue(moto_endpoint)
    yield proxy
    proxy.server.shutdown()


@pytest.fixture(scope="module")
def remote(slow_glue: SlowGlue) -> Generator[str, None, None]:
    catalog_config = (
        f'[{{name="sail", type="glue", catalog_id="123456789012", region="us-east-1", endpoint_url="{slow_glue.url}"}}]'
    )
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": catalog_config,
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
        },
    ) as server:
        yield server.remote


@pytest.mark.parametrize("kind", ["sql", "dataframe"])
def test_view_reads_the_table_of_the_database_current_when_it_was_created(spark, slow_glue, tmp_path, kind):
    for database, value in [("view_namespace_one", 1), ("view_namespace_two", 2)]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        root = tmp_path / database
        root.mkdir()
        pq.write_table(pa.table({"id": pa.array([value], pa.int64())}), root / "data.parquet")
        spark.sql(f"CREATE TABLE {database}.t (id BIGINT) USING parquet LOCATION '{root.as_uri()}'")
    view = f"namespace_view_{kind}"
    try:
        spark.sql("USE DATABASE view_namespace_one")
        slow_glue.armed = ("view_namespace_one", "t")

        def create():
            if kind == "sql":
                spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT id FROM t")  # noqa: S608
            else:
                spark.table("t").select("id").createOrReplaceTempView(view)

        with ThreadPoolExecutor(max_workers=1) as executor:
            created = executor.submit(create)
            # The session changes its database while the lookup of the table of the view waits.
            assert slow_glue.hit.wait(30)
            spark.sql("USE DATABASE view_namespace_two")
            created.result(60)
        assert spark.table(view).collect() == [(1,)]
        # The view resolves its query again in the database that was current when it was created.
        spark.sql("ALTER TABLE view_namespace_one.t ADD COLUMNS (extra STRING)")
        assert spark.table(view).collect() == [(1,)]
    finally:
        slow_glue.armed = None
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        spark.sql("USE DATABASE test_db")
        for database in ["view_namespace_one", "view_namespace_two"]:
            spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
