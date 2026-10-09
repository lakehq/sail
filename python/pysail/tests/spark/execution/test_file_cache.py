import hashlib
import json
import threading
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail file cache configuration")


@pytest.fixture(scope="module")
def object_files():
    # A small S3 read endpoint makes equal paths, sizes, and timestamps deterministic.
    files = {}
    requests = Counter()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            self.respond(head=True)

        def do_GET(self):
            self.respond(head=False)

        def respond(self, *, head):
            url = urlsplit(self.path)
            bucket, _, key = unquote(url.path).lstrip("/").partition("/")
            query = parse_qs(url.query)
            kind = "listing" if "list-type" in query else ("head" if head else "data")
            requests[bucket, kind] += 1
            if "list-type" in query:
                prefix = query.get("prefix", [""])[0]
                entries = "".join(
                    f"<Contents><Key>{escape(name)}</Key>"
                    f"<LastModified>2025-01-01T00:00:00.000Z</LastModified>"
                    f"<ETag>&quot;{hashlib.sha256(data).hexdigest()}&quot;</ETag>"
                    f"<Size>{len(data)}</Size><StorageClass>STANDARD</StorageClass></Contents>"
                    for (store, name), data in sorted(files.items())
                    if store == bucket and name.startswith(prefix)
                )
                body = (
                    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    f"<Name>{bucket}</Name><Prefix>{escape(prefix)}</Prefix>"
                    f"<IsTruncated>false</IsTruncated>{entries}</ListBucketResult>"
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
            elif (body := files.get((bucket, key))) is None:
                body = b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>"
                self.send_response(404)
            else:
                size = len(body)
                etag = hashlib.sha256(body).hexdigest()
                if requested := self.headers.get("Range"):
                    first, _, last = requested.removeprefix("bytes=").partition("-")
                    start = int(first) if first else size - int(last)
                    end = min(int(last) + 1, size) if first and last else size
                    body = body[start:end]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
                else:
                    self.send_response(200)
                self.send_header("ETag", f'"{etag}"')
                self.send_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield files, f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.fixture(scope="module", params=["metadata", "statistics", "listing"])
def cache_kind(request):
    return request.param


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request, cache_kind, object_files):
    _, endpoint, _ = object_files
    envs = {
        "SAIL_MODE": request.param,
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
        "SAIL_PARQUET__FILE_METADATA_CACHE__TYPE": "none",
        "SAIL_PARQUET__FILE_STATISTICS_CACHE__TYPE": "none",
        "SAIL_EXECUTION__FILE_LISTING_CACHE__TYPE": "none",
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_SKIP_SIGNATURE": "true",
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    section = "EXECUTION" if cache_kind == "listing" else "PARQUET"
    envs[f"SAIL_{section}__FILE_{cache_kind.upper()}_CACHE__TYPE"] = "global"
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def parquet_bytes(values):
    output = pa.BufferOutputStream()
    pq.write_table(pa.table({"id": pa.array(values, type=pa.int64())}), output, compression="NONE")
    return output.getvalue().to_pybytes()


def test_caches_separate_buckets(spark, object_files, cache_kind):
    files, _, _ = object_files
    first, second = f"a-{uuid.uuid4().hex}", f"b-{uuid.uuid4().hex}"
    files[first, "data/part.parquet"] = parquet_bytes([1, 2, 3])
    files[second, "data/part.parquet"] = parquet_bytes([7, 8, 9])
    assert len(files[first, "data/part.parquet"]) == len(files[second, "data/part.parquet"])
    expected = [7, 8, 9]
    if cache_kind == "listing":
        files[second, "data/extra.parquet"] = parquet_bytes([10])
        expected.append(10)
    assert [r.id for r in spark.read.parquet(f"s3://{first}/data/").orderBy("id").collect()] == [1, 2, 3]
    data = spark.read.parquet(f"s3://{second}/data/")
    assert [r.id for r in data.orderBy("id").collect()] == expected
    assert [r.id for r in data.filter("id = 8").collect()] == [8]
    assert tuple(data.selectExpr("min(id)", "max(id)").first()) == (7, expected[-1])


def test_cache_rejects_changed_etag(spark, object_files, cache_kind):
    if cache_kind == "listing":
        pytest.skip("listing caches intentionally retain external changes until expiry")
    files, _, _ = object_files
    bucket = f"version-{uuid.uuid4().hex}"
    key = (bucket, "data.parquet")
    files[key] = parquet_bytes([1, 2, 3])
    original_size = len(files[key])
    path = f"s3://{bucket}/data.parquet"
    assert [r.id for r in spark.read.parquet(path).orderBy("id").collect()] == [1, 2, 3]
    files[key] = parquet_bytes([7, 8, 9])
    assert len(files[key]) == original_size
    data = spark.read.parquet(path)
    assert [r.id for r in data.filter("id = 8").collect()] == [8]
    assert tuple(data.selectExpr("min(id)", "max(id)").first()) == (7, 9)


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_listing_cache_observes_sail_writes(spark, tmp_path, cache_kind, mode):
    if cache_kind != "listing":
        pytest.skip("listing invalidation")
    path = str(tmp_path / "data")
    initial_rows = 3
    spark.range(initial_rows).write.parquet(path)
    assert spark.read.parquet(path).count() == initial_rows
    spark.range(3, 5).write.mode(mode).parquet(path)
    expected = list(range(5)) if mode == "append" else [3, 4]
    assert [r.id for r in spark.read.parquet(path).orderBy("id").collect()] == expected


def test_repeated_reads_use_cache(spark, remote, object_files, cache_kind):
    files, _, requests = object_files
    bucket = f"reuse-{uuid.uuid4().hex}"
    files[bucket, "data/part.parquet"] = parquet_bytes([1, 2, 3])
    kind = "listing" if cache_kind == "listing" else "data"

    def read(session):
        return session.read.schema("id BIGINT").parquet(f"s3://{bucket}/data/").filter("id = 100").collect()

    assert read(spark) == []
    first = requests[bucket, kind]
    assert first > 0
    assert read(spark) == []
    second = requests[bucket, kind] - first
    assert second < first
    # Global scope also reuses entries across client sessions on the same server.
    with spark_session_factory(remote) as sessions:
        before = requests[bucket, kind]
        assert read(sessions.create()) == []
        assert requests[bucket, kind] - before <= second


@pytest.mark.parametrize("metadata_as_data", [False, True])
@pytest.mark.parametrize("checkpoint", [False, True])
def test_delta_footers_separate_buckets(spark, object_files, cache_kind, metadata_as_data, checkpoint):
    if cache_kind != "metadata":
        pytest.skip("Delta reads log statistics and lists its transaction log directly")
    files, _, _ = object_files
    buckets = [f"delta-a-{uuid.uuid4().hex}", f"delta-b-{uuid.uuid4().hex}"]
    fields = [{"name": "id", "type": "long", "nullable": True, "metadata": {}}]
    for bucket, values in zip(buckets, [[1, 2, 3], [7, 8, 9]], strict=True):
        data = parquet_bytes(values)
        files[bucket, "table/part.parquet"] = data
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": bucket,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps({"type": "struct", "fields": fields}),
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 0,
                }
            },
            {
                "add": {
                    "path": "part.parquet",
                    "partitionValues": {},
                    "size": len(data),
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": 3}),
                }
            },
        ]
        files[bucket, "table/_delta_log/00000000000000000000.json"] = "".join(
            json.dumps(action) + "\n" for action in actions
        ).encode()
        if checkpoint:
            string_map = pa.map_(pa.string(), pa.string())
            schema = pa.schema(
                [
                    ("protocol", pa.struct([("minReaderVersion", pa.int32()), ("minWriterVersion", pa.int32())])),
                    (
                        "metaData",
                        pa.struct(
                            [
                                ("id", pa.string()),
                                ("format", pa.struct([("provider", pa.string()), ("options", string_map)])),
                                ("schemaString", pa.string()),
                                ("partitionColumns", pa.list_(pa.string())),
                                ("configuration", string_map),
                                ("createdTime", pa.int64()),
                            ]
                        ),
                    ),
                    (
                        "add",
                        pa.struct(
                            [
                                ("path", pa.string()),
                                ("partitionValues", string_map),
                                ("size", pa.int64()),
                                ("modificationTime", pa.int64()),
                                ("dataChange", pa.bool_()),
                                ("stats", pa.string()),
                            ]
                        ),
                    ),
                ]
            )
            output = pa.BufferOutputStream()
            pq.write_table(pa.Table.from_pylist(actions, schema=schema), output, compression="NONE")
            files[bucket, "table/_delta_log/00000000000000000000.checkpoint.parquet"] = output.getvalue().to_pybytes()
            files[bucket, "table/_delta_log/_last_checkpoint"] = json.dumps(
                {"version": 0, "size": len(actions)}
            ).encode()

    def read(bucket):
        return (
            spark.read.format("delta")
            .option("metadataAsDataRead", str(metadata_as_data).lower())
            .load(f"s3://{bucket}/table")
        )

    assert [r.id for r in read(buckets[0]).orderBy("id").collect()] == [1, 2, 3]
    assert [r.id for r in read(buckets[1]).filter("id = 8").collect()] == [8]
