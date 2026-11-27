# SPDX-License-Identifier: Apache-2.0

import json
import threading
from datetime import UTC, datetime
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, TimestampType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.tests._dev.spark.iceberg.utils import create_sql_catalog

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail worker Parquet configuration")

STARTUP_METADATA_SIZE_HINT = 8192


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request):
    with spark_connect_server(
        envs={
            "SAIL_MODE": request.param,
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
            "SAIL_PARQUET__METADATA_SIZE_HINT": str(STARTUP_METADATA_SIZE_HINT),
            # Every scan reads the Parquet footers it needs, so that the requests show the hint.
            "SAIL_PARQUET__FILE_METADATA_CACHE__TYPE": "none",
            # The HTTP object store reads its client options from the environment.
            "allow_http": "true",
        }
    ) as server:
        yield server.remote


@pytest.fixture
def http_ranges(tmp_path):
    """Serve `tmp_path` as a read-only WebDAV store and record the byte ranges of `GET` requests."""
    root = tmp_path.resolve()
    ranges = []

    def entry(path):
        href = quote("/" + "/".join(path.relative_to(root).parts))
        modified = formatdate(path.stat().st_mtime, usegmt=True)
        if path.is_dir():
            prop = "<resourcetype><collection/></resourcetype>"
        else:
            prop = f"<resourcetype/><getcontentlength>{path.stat().st_size}</getcontentlength>"
        return (
            f"<response><href>{href}</href><propstat><prop><getlastmodified>{modified}</getlastmodified>"
            f"{prop}</prop><status>HTTP/1.1 200 OK</status></propstat></response>"
        )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def resolve(self):
            path = (root / unquote(urlsplit(self.path).path).lstrip("/")).resolve()
            return path if path.is_relative_to(root) and path.exists() else None

        def reply(self, status, body=b"", headers=()):
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self):
            path = self.resolve()
            if path is None or not path.is_file():
                self.reply(404)
                return
            data = path.read_bytes()
            size = len(data)
            header = self.headers.get("Range")
            if header is None:
                self.reply(200, data)
                return
            first, last = header.removeprefix("bytes=").split("-")
            if first:
                start, end = int(first), min(int(last) + 1, size) if last else size
            else:
                start, end = size - int(last), size
            ranges.append((path.relative_to(root).as_posix(), start, end))
            self.reply(206, data[start:end], [("Content-Range", f"bytes {start}-{end - 1}/{size}")])

        def do_HEAD(self):
            path = self.resolve()
            if path is None or not path.is_file():
                self.reply(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(path.stat().st_size))
            self.send_header("Last-Modified", formatdate(path.stat().st_mtime, usegmt=True))
            self.end_headers()

        def do_PROPFIND(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            path = self.resolve()
            if path is None:
                self.reply(404)
                return
            depth = self.headers.get("Depth", "infinity")
            paths = [path]
            if path.is_dir() and depth != "0":
                paths += sorted(path.rglob("*") if depth == "infinity" else path.iterdir())
            body = '<?xml version="1.0"?><multistatus xmlns="DAV:">' + "".join(map(entry, paths)) + "</multistatus>"
            self.reply(207, body.encode(), [("Content-Type", "application/xml")])

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", ranges
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("table_format", ["delta", "iceberg"])
@pytest.mark.parametrize("metadata_as_data", [False, True])
# The session resolution must also reach Delta and Iceberg scans that workers build.
@pytest.mark.parametrize(("coerce_int96", "fraction"), [(None, "678901"), ("ms", "678")])
def test_lake_int96_timestamp_resolution(remote, tmp_path, table_format, metadata_as_data, coerce_int96, fraction):
    path = tmp_path / "timestamps.parquet"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"}),
            pa.field("ts", pa.timestamp("us"), metadata={"PARQUET:field_id": "2"}),
        ]
    )
    # The year 3000 fits in microseconds but overflows a nanosecond timestamp.
    timestamps = [
        datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        datetime(3000, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        None,
    ]
    data = pa.table({"id": [0, 1, 2], "ts": timestamps}, schema=schema)
    pq.write_table(data, path, use_deprecated_int96_timestamps=True, store_schema=False)
    catalog = None
    if table_format == "delta":
        log = tmp_path / "_delta_log"
        log.mkdir()
        fields = [
            {"name": name, "type": kind, "nullable": True, "metadata": {}}
            for name, kind in [("id", "long"), ("ts", "timestamp")]
        ]
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": "int96",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps({"type": "struct", "fields": fields}),
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0,
                }
            },
            {
                "add": {
                    "path": path.name,
                    "partitionValues": {},
                    "size": path.stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": 3}),
                }
            },
        ]
        (log / "00000000000000000000.json").write_text("".join(json.dumps(action) + "\n" for action in actions))
        location = str(tmp_path)
    else:
        catalog = create_sql_catalog(tmp_path)
        table = catalog.create_table(
            "default.int96",
            schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "ts", TimestampType())),
        )
        with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=path.as_uri(),
                    file_format=FileFormat.PARQUET,
                    partition={},
                    record_count=3,
                    file_size_in_bytes=path.stat().st_size,
                    spec_id=0,
                )
            )
        location = table.location()
    try:
        with spark_session_factory(remote) as sessions:
            spark = sessions.create()
            if coerce_int96 is not None:
                spark.sql(f"SET datafusion.execution.parquet.coerce_int96 = '{coerce_int96}'").collect()
            query = (
                spark.read.format(table_format)
                .option("metadataAsDataRead", str(metadata_as_data).lower())
                .load(location)
            )
            if metadata_as_data:
                node = "DeltaScanByAddsExec" if table_format == "delta" else "IcebergScanByDataFilesExec"
                assert node in query._explain_string()  # noqa: SLF001
            rows = query.orderBy("id").selectExpr("CAST(ts AS STRING) AS ts").collect()
        assert [row.ts for row in rows] == [f"2024-01-02 03:04:05.{fraction}", f"3000-01-02 03:04:05.{fraction}", None]
    finally:
        if catalog is not None:
            catalog.drop_table("default.int96")


# Every Parquet footer read uses the session hint: the data files of each scan path and the Delta log checkpoint.
@pytest.mark.parametrize("source", ["parquet", "delta", "delta-metadata-as-data", "delta-checkpoint-metadata-as-data"])
def test_scans_use_session_metadata_size_hint(remote, tmp_path, http_ranges, source):
    url, ranges = http_ranges
    table = tmp_path / "table"
    checkpoint = source.startswith("delta-checkpoint")
    metadata_as_data = source.endswith("metadata-as-data")
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        # Random doubles keep each data file larger than the startup hint.
        frame = spark.range(0, 8192, 1, 2).selectExpr("id", "rand(0) AS v")
        if source == "parquet":
            frame.write.parquet(str(table))
        else:
            interval = "1" if checkpoint else "10"
            frame.write.format("delta").option("delta.checkpointInterval", interval).save(str(table))
        if checkpoint:
            # Many commits keep the checkpoint larger than the startup hint.
            for _ in range(25):
                append = spark.range(0, 16, 1, 8).selectExpr("id", "rand(1) AS v")
                append.write.format("delta").mode("append").save(str(table))
        files = [*table.glob("*.parquet"), *(table / "_delta_log").glob("*.checkpoint.parquet")]
        sizes = {file.relative_to(tmp_path).as_posix(): file.stat().st_size for file in files}
        assert checkpoint or min(sizes.values()) > STARTUP_METADATA_SIZE_HINT
        spark.sql("SET datafusion.execution.parquet.metadata_size_hint = 128").collect()
        if source == "parquet":
            query = spark.read.parquet(f"{url}/table")
        else:
            reader = spark.read.format("delta").option("metadataAsDataRead", str(metadata_as_data).lower())
            query = reader.load(f"{url}/table")
            if metadata_as_data:
                assert "DeltaScanByAddsExec" in query._explain_string()  # noqa: SLF001
        # The sum needs the data, which statistics cannot answer.
        expected = 8191 * 8192 // 2 + (25 * (15 * 16 // 2) if checkpoint else 0)
        assert query.selectExpr("sum(id) AS s").collect()[0].s == expected
    # The footer is the only read that ends at the end of a file.
    footers = {(name, end - start) for name, start, end in ranges if end == sizes.get(name)}
    assert footers == {(name, 128) for name, _ in footers}
    assert {name for name in sizes if ".checkpoint" not in name} <= {name for name, _ in footers}
    if checkpoint:
        assert max(name for name in sizes if ".checkpoint" in name) in {name for name, _ in footers}
