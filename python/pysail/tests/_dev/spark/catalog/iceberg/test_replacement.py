# ruff: noqa: S608
"""REST replacement transactions through public SQL and DataFrame APIs.

[CREDIT]: https://github.com/apache/iceberg/blob/2234c549de60/core/src/test/java/org/apache/iceberg/TestReplaceTransaction.java
"""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from pyspark.errors import AnalysisException, PySparkException, SparkRuntimeException

from pysail.testing.spark.session import spark_session_factory
from pysail.tests._dev.spark.catalog.iceberg import test_commit
from pysail.tests._dev.spark.catalog.iceberg.test_commit import (
    NAMESPACE,
    _assert_uuid_metadata_location,
    _current_schema_field_names,
    _current_snapshot,
    _load_table,
    _s3_object_keys,
)

commit_gate_proxy = test_commit.commit_gate_proxy
gated_remote = test_commit.gated_remote
namespace = test_commit.namespace
SERVER_FIELD_ID_OFFSET = 100


def _replace(spark, name, api, *, empty=False):
    source = spark.sql("SELECT CAST(7 AS BIGINT) AS replacement" + (" WHERE false" if empty else ""))
    if api == "sql":
        spark.sql(
            f"CREATE OR REPLACE TABLE {name} USING iceberg TBLPROPERTIES ('changed'='new') "
            "AS SELECT CAST(7 AS BIGINT) AS replacement" + (" WHERE false" if empty else "")
        ).collect()
    elif api == "save_table":
        source.write.format("iceberg").mode("overwrite").saveAsTable(name)
    else:
        getattr(source.writeTo(name).using("iceberg").tableProperty("changed", "new"), api)()


@pytest.mark.parametrize("api", ["sql", "save_table", "replace", "createOrReplace"])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_rest_replacement_lifecycle(spark, iceberg_rest_endpoint, seaweedfs_host_endpoint, api, existing, empty):
    table = f"replacement_{api.lower()}_{existing}_{empty}".lower()
    name = f"{NAMESPACE}.{table}"
    before = None
    if existing:
        spark.sql(
            f"CREATE TABLE {name} USING iceberg TBLPROPERTIES ('retained'='yes', 'changed'='old') AS SELECT 1 AS old"
        )
        before = _load_table(iceberg_rest_endpoint, table)
    if not existing and api == "replace":
        with pytest.raises(PySparkException):
            _replace(spark, name, api, empty=empty)
        assert not spark.catalog.tableExists(name)
        return
    _replace(spark, name, api, empty=empty)
    after = _load_table(iceberg_rest_endpoint, table)
    metadata = after["metadata"]
    assert _current_schema_field_names(metadata) == ["replacement"]
    assert metadata["location"].startswith("s3://icebergdata/demo/")
    _assert_uuid_metadata_location(after["metadata-location"])
    snapshot = _current_snapshot(metadata)
    assert snapshot.get("parent-snapshot-id") is None
    assert metadata["refs"]["main"] == {"snapshot-id": snapshot["snapshot-id"], "type": "branch"}
    if before:
        original = before["metadata"]
        assert metadata["table-uuid"] == original["table-uuid"]
        assert metadata["location"] == original["location"]
        assert metadata["last-column-id"] > original["last-column-id"]
        assert metadata["properties"]["retained"] == "yes"
        assert all(snapshot in metadata["snapshots"] for snapshot in original["snapshots"])
        assert before["metadata-location"] in {entry["metadata-file"] for entry in metadata["metadata-log"]}
    keys = _s3_object_keys(seaweedfs_host_endpoint, metadata["location"])
    assert not any(key.endswith("version-hint.text") or "/metadata/v1.metadata.json" in key for key in keys)
    expected = [] if empty else [(7,)]
    assert [tuple(row) for row in spark.table(name).collect()] == expected
    assert [tuple(row) for row in spark.newSession().table(name).collect()] == expected
    spark.sql(f"INSERT INTO {name} VALUES (8)")
    assert [tuple(row) for row in spark.table(name).orderBy("replacement").collect()] == [*expected, (8,)]


@pytest.mark.parametrize("existing", [False, True])
def test_rest_replacement_is_invisible_until_one_commit(spark, gated_remote, commit_gate_proxy, existing):
    _, gate = commit_gate_proxy
    table = f"replacement_visibility_{existing}".lower()
    name = f"{NAMESPACE}.{table}"
    if existing:
        spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    gate.arm(NAMESPACE, table, snapshot_only=False)
    with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor() as executor:
        writer = sessions.create()
        future = executor.submit(_replace, writer, name, "createOrReplace")
        try:
            assert gate.arrived.wait(30)
            if existing:
                assert [tuple(row) for row in spark.table(name).collect()] == [(1,)]
            else:
                assert not spark.catalog.tableExists(name)
            requirements = gate.request["requirements"]
            kinds = {requirement["type"] for requirement in requirements}
            if existing:
                assert "assert-table-uuid" in kinds
                assert "assert-last-assigned-field-id" in kinds
                assert {"assert-current-schema-id", "assert-ref-snapshot-id"} <= kinds
                assert "assert-default-spec-id" not in kinds
            else:
                assert kinds == {"assert-create"}
        finally:
            gate.release.set()
        future.result(timeout=30)
    assert gate.request_count == 1
    assert [tuple(row) for row in spark.table(name).collect()] == [(7,)]


@pytest.mark.parametrize("fault", ["disconnect", "malformed", 503])
def test_rest_replacement_unknown_response_preserves_published_files(
    spark, iceberg_rest_endpoint, gated_remote, commit_gate_proxy, fault
):
    _, gate = commit_gate_proxy
    table = f"replacement_unknown_{fault}"
    name = f"{NAMESPACE}.{table}"
    spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    gate.arm(NAMESPACE, table, snapshot_only=False)
    gate.response_fault = fault
    gate.release.set()
    with spark_session_factory(gated_remote) as sessions:
        writer = sessions.create()
        with pytest.raises(SparkRuntimeException, match=r"unknown|may have succeeded"):
            _replace(writer, name, "replace")
    assert gate.request_count == 1
    assert [tuple(row) for row in spark.table(name).collect()] == [(7,)]
    assert _current_schema_field_names(_load_table(iceberg_rest_endpoint, table)["metadata"]) == ["replacement"]


@pytest.mark.parametrize("mutation", ["rename", "append", "two_appends"])
def test_rest_replacement_concurrent_metadata_race(
    spark, iceberg_rest_endpoint, gated_remote, commit_gate_proxy, mutation
):
    _, gate = commit_gate_proxy
    table = f"replacement_race_{mutation}"
    name = f"{NAMESPACE}.{table}"
    spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    gate.arm(NAMESPACE, table, snapshot_only=False)
    with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor() as executor:
        writer = sessions.create()
        future = executor.submit(_replace, writer, name, "replace")
        try:
            assert gate.arrived.wait(30)
            if mutation == "rename":
                spark.sql(f"ALTER TABLE {name} RENAME COLUMN old TO renamed")
            else:
                spark.sql(f"INSERT INTO {name} VALUES (2)")
                if mutation == "two_appends":
                    spark.sql(f"INSERT INTO {name} VALUES (3)")
            concurrent = _load_table(iceberg_rest_endpoint, table)
        finally:
            gate.release.set()
        with pytest.raises(AnalysisException, match="replan"):
            future.result(timeout=30)
    assert gate.request_count == 1
    assert _load_table(iceberg_rest_endpoint, table) == concurrent
    metadata = concurrent["metadata"]
    current = spark.table(name)
    numeric = spark.read.option("snapshot-id", metadata["current-snapshot-id"]).table(name)
    assert sorted(numeric.collect()) == sorted(current.collect())


@pytest.mark.parametrize("api", ["sql", "createOrReplace"])
@pytest.mark.parametrize(
    ("shape", "expression"),
    [
        ("struct", "named_struct('n', 1)"),
        ("list", "array(named_struct('n', 1))"),
        ("map", "map('key', named_struct('n', 1))"),
        ("tinyint", "CAST(7 AS TINYINT)"),
        ("smallint", "CAST(7 AS SMALLINT)"),
        ("small_struct", "named_struct('tiny', CAST(7 AS TINYINT), 'small', CAST(8 AS SMALLINT))"),
        ("small_list", "array(named_struct('n', CAST(7 AS SMALLINT)))"),
        ("small_map", "map(CAST(1 AS SMALLINT), named_struct('n', CAST(7 AS TINYINT)))"),
    ],
)
def test_rest_staged_nested_schema_and_server_field_ids(
    spark, iceberg_rest_endpoint, gated_remote, commit_gate_proxy, seaweedfs_host_endpoint, api, expression, shape
):
    import io
    import json
    import urllib.parse

    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.config import Config

    _, gate = commit_gate_proxy
    table = f"nested_{api.lower()}_{shape}"
    name = f"{NAMESPACE}.{table}"

    def transform(method, path, body, response):
        if method == "POST" and path.endswith("/tables") and json.loads(body).get("stage-create"):

            def remap(value):
                if isinstance(value, dict):
                    for key, item in value.items():
                        if key in {"id", "element-id", "key-id", "value-id"}:
                            value[key] = item + SERVER_FIELD_ID_OFFSET
                        else:
                            remap(item)
                elif isinstance(value, list):
                    for item in value:
                        remap(item)

            remap(response["metadata"]["schemas"])
            response["metadata"]["last-column-id"] += SERVER_FIELD_ID_OFFSET
        return response

    gate.response_transform = transform
    with spark_session_factory(gated_remote) as sessions:
        writer = sessions.create()
        source = writer.sql(f"SELECT {expression} AS nested")
        if api == "sql":
            writer.sql(f"CREATE OR REPLACE TABLE {name} USING iceberg AS SELECT {expression} AS nested").collect()
        else:
            source.writeTo(name).using("iceberg").createOrReplace()
        assert writer.table(name).collect() == source.collect()
    after = _load_table(iceberg_rest_endpoint, table)["metadata"]
    schema = next(schema for schema in after["schemas"] if schema["schema-id"] == after["current-schema-id"])
    assert schema["fields"][0]["id"] == SERVER_FIELD_ID_OFFSET + 1
    keys = _s3_object_keys(seaweedfs_host_endpoint, after["location"])
    s3 = boto3.client(
        "s3",
        endpoint_url=seaweedfs_host_endpoint,
        aws_access_key_id="admin",
        aws_secret_access_key="password",  # noqa: S106
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    bucket = urllib.parse.urlparse(after["location"]).netloc
    for key in keys:
        if key.endswith(".parquet"):
            metadata = pq.read_metadata(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
            field = metadata.schema.to_arrow_schema().field("nested")

            def parquet_ids(field):
                ids = {int(field.metadata[b"PARQUET:field_id"])}
                if pa.types.is_struct(field.type):
                    for child in field.type:
                        ids.update(parquet_ids(child))
                elif pa.types.is_list(field.type):
                    ids.update(parquet_ids(field.type.value_field))
                elif pa.types.is_map(field.type):
                    ids.update(parquet_ids(field.type.key_field))
                    ids.update(parquet_ids(field.type.item_field))
                return ids

            def iceberg_ids(value):
                if isinstance(value, dict):
                    return set().union(
                        *(
                            {item} if key in {"id", "element-id", "key-id", "value-id"} else iceberg_ids(item)
                            for key, item in value.items()
                        )
                    )
                if isinstance(value, list):
                    return set().union(*(iceberg_ids(item) for item in value))
                return set()

            assert parquet_ids(field) == iceberg_ids(schema["fields"])
    assert spark.table(name).count() == 1


@pytest.mark.parametrize("mutation", ["create", "uuid", "field_highwater"])
def test_rest_replacement_definite_conflict_preserves_winner(
    spark, iceberg_rest_endpoint, gated_remote, commit_gate_proxy, mutation
):
    _, gate = commit_gate_proxy
    table = f"replacement_conflict_{mutation}"
    name = f"{NAMESPACE}.{table}"
    if mutation != "create":
        spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    gate.arm(NAMESPACE, table, snapshot_only=False)
    with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor() as executor:
        writer = sessions.create()
        future = executor.submit(_replace, writer, name, "createOrReplace")
        try:
            assert gate.arrived.wait(30)
            if mutation == "uuid":
                spark.sql(f"DROP TABLE {name}")
            if mutation in {"create", "uuid"}:
                location = f"s3://icebergdata/winner/{table}"
                spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{location}' AS SELECT 2 AS winner")
            else:
                spark.sql(f"ALTER TABLE {name} ADD COLUMN additional INT")
            winner = _load_table(iceberg_rest_endpoint, table)
        finally:
            gate.release.set()
        with pytest.raises(AnalysisException, match="replan"):
            future.result(timeout=30)
    assert gate.request_count == 1
    assert _load_table(iceberg_rest_endpoint, table) == winner
    assert spark.table(name).count() == 1


def test_rest_replacement_ignores_unpublished_path_metadata(spark, iceberg_rest_endpoint, seaweedfs_host_endpoint):
    import urllib.parse

    import boto3
    from botocore.config import Config

    table = "replacement_ignores_filesystem_metadata"
    name = f"{NAMESPACE}.{table}"
    spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    location = _load_table(iceberg_rest_endpoint, table)["metadata"]["location"]
    parsed = urllib.parse.urlparse(location)
    client = boto3.client(
        "s3",
        endpoint_url=seaweedfs_host_endpoint,
        aws_access_key_id="admin",
        aws_secret_access_key="password",  # noqa: S106
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    for suffix in ["v999.metadata.json", "v999.gz.metadata.json", "version-hint.text"]:
        client.put_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/") + "/metadata/" + suffix,
            Body=b"invalid unpublished metadata",
        )
    _replace(spark, name, "replace")
    assert [tuple(row) for row in spark.table(name).collect()] == [(7,)]


@pytest.fixture
def no_list_s3_endpoint(seaweedfs_host_endpoint):
    import threading
    from http import HTTPStatus
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import urlsplit

    import requests

    requests_seen = []

    class PermissionHandler(BaseHTTPRequestHandler):
        def _forward(self):
            requests_seen.append((self.command, self.path))
            path = urlsplit(self.path)
            # AWS requires ListBucket to distinguish a missing object from access denial.
            if self.command == "GET" and path.path.rstrip("/") == "/icebergdata":
                self.send_error(HTTPStatus.FORBIDDEN)
                return
            length = int(self.headers.get("content-length", "0"))
            response = requests.request(
                self.command,
                seaweedfs_host_endpoint + self.path,
                data=self.rfile.read(length) if length else None,
                headers=dict(self.headers),
                allow_redirects=False,
                timeout=30,
            )
            if self.command == "HEAD" and response.status_code == HTTPStatus.NOT_FOUND:
                self.send_error(HTTPStatus.FORBIDDEN)
                return
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in {"connection", "transfer-encoding", "content-encoding"}:
                    self.send_header(key, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response.content)

        def do_GET(self):
            self._forward()

        def do_HEAD(self):
            self._forward()

        def do_PUT(self):
            self._forward()

        def do_POST(self):
            self._forward()

        def do_DELETE(self):
            self._forward()

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), PermissionHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        # Establish both denied operations independently of the Sail request path.
        assert requests.get(endpoint + "/icebergdata?list-type=2", timeout=30).status_code == HTTPStatus.FORBIDDEN
        yield endpoint, requests_seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.mark.parametrize("configured", [False, True])
def test_rest_replacement_uses_configured_storage_without_listing(
    spark, seaweedfs_host_endpoint, commit_gate_proxy, configured, no_list_s3_endpoint
):
    import json

    endpoint, gate = commit_gate_proxy
    table = f"replacement_access_{configured}".lower()
    name = f"{NAMESPACE}.{table}"
    staged_locations = []
    storage_endpoint, storage_requests = no_list_s3_endpoint
    storage_requests.clear()

    def transform(method, path, body, response):
        if method == "POST" and path.endswith("/tables") and json.loads(body).get("stage-create"):
            staged_locations.append(response["metadata"]["location"])
            response["config"] = {"s3.remote-signing-enabled": "true"}
            response["storage-credentials"] = [
                {
                    "prefix": response["metadata"]["location"],
                    "config": {"s3.access-key-id": "session-only-key", "s3.secret-access-key": "session-only-secret"},
                }
            ]
        return response

    gate.response_transform = transform
    gate.arm(NAMESPACE, table, snapshot_only=False)
    gate.release.set()
    # AWS credentials are process-global; a fresh process must own the entire server.
    envs = {
        "SAIL_CATALOG__LIST": f'[{{name="sail", type="iceberg-rest", uri="{endpoint}"}}]',
        "AWS_ACCESS_KEY_ID": "admin" if configured else "invalid",
        "AWS_SECRET_ACCESS_KEY": "password" if configured else "invalid",
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT": storage_endpoint,
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "AWS_ALLOW_HTTP": "true",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    script = """
import sys
from pyspark.errors import PySparkException
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
    writer = sessions.create()
    try:
        writer.sql(f"CREATE OR REPLACE TABLE {sys.argv[1]} USING iceberg AS SELECT 7 AS replacement").collect()
    except PySparkException as error:
        assert sys.argv[2] == "False", str(error)
        assert any(code in str(error) for code in ("403", "Forbidden", "AccessDenied", "InvalidAccessKeyId")), str(error)
        assert "session-only-secret" not in str(error)
    else:
        assert sys.argv[2] == "True", "write succeeded without configured storage access"
        assert writer.table(sys.argv[1]).collect() == [(7,)]
        writer.sql(f"REPLACE TABLE {sys.argv[1]} USING iceberg AS SELECT 8 AS replacement").collect()
        assert writer.table(sys.argv[1]).collect() == [(8,)]
"""
    environment = {**os.environ, **envs}
    environment.pop("SPARK_REMOTE", None)
    environment.pop("AWS_SESSION_TOKEN", None)
    result = subprocess.run(
        [sys.executable, "-c", script, name, str(configured)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(staged_locations) == 1
    keys = _s3_object_keys(seaweedfs_host_endpoint, staged_locations[0])
    if configured:
        assert any(key.endswith(".parquet") for key in keys)
        assert spark.table(name).collect() == [(8,)]
        assert any(method == "PUT" for method, _ in storage_requests)
        assert not any("list-type" in path or ".sail-replacement-access" in path for _, path in storage_requests)
    else:
        assert gate.request_count == 0
        assert not keys
        assert not spark.catalog.tableExists(name)


def test_rest_replacement_publication_survives_caller_cancellation(
    spark, iceberg_rest_endpoint, gated_remote, commit_gate_proxy
):
    _, gate = commit_gate_proxy
    table = "replacement_cancelled"
    name = f"{NAMESPACE}.{table}"
    spark.sql(f"CREATE TABLE {name} USING iceberg AS SELECT 1 AS old")
    gate.arm(NAMESPACE, table, snapshot_only=False)
    with spark_session_factory(gated_remote) as sessions, ThreadPoolExecutor() as executor:
        writer = sessions.create()
        future = executor.submit(_replace, writer, name, "replace")
        try:
            assert gate.arrived.wait(30)
            interrupted = writer.interruptAll()
            assert interrupted, "the blocked replacement operation must be interrupted"
            with pytest.raises(PySparkException, match=r"[Cc]ancel|[Ii]nterrupt|aborted|operation not found"):
                future.result(timeout=30)
        finally:
            gate.release.set()
        assert gate.completed.wait(30)
    assert gate.request_count == 1
    assert _current_schema_field_names(_load_table(iceberg_rest_endpoint, table)["metadata"]) == ["replacement"]
    assert [tuple(row) for row in spark.table(name).collect()] == [(7,)]


@pytest.mark.parametrize("mismatch", ["name", "type", "required"])
def test_rest_staged_incompatible_nested_schema_rejects_before_writing(
    spark, seaweedfs_host_endpoint, gated_remote, commit_gate_proxy, mismatch
):
    import json

    _, gate = commit_gate_proxy
    table = f"incompatible_nested_{mismatch}"
    name = f"{NAMESPACE}.{table}"
    locations = []

    def transform(method, path, body, response):
        if method == "POST" and path.endswith("/tables") and json.loads(body).get("stage-create"):
            locations.append(response["metadata"]["location"])
            field = response["metadata"]["schemas"][0]["fields"][0]["type"]["fields"][0]
            field[mismatch] = {"name": "renamed", "type": "string", "required": True}[mismatch]
        return response

    gate.response_transform = transform
    with spark_session_factory(gated_remote) as sessions:
        writer = sessions.create()
        with pytest.raises(PySparkException, match="REST staged schema differs from replacement input"):
            writer.sql(
                f"CREATE OR REPLACE TABLE {name} USING iceberg AS SELECT named_struct('n', CAST(NULL AS INT)) AS nested"
            ).collect()
    assert not spark.catalog.tableExists(name)
    assert len(locations) == 1
    assert not _s3_object_keys(seaweedfs_host_endpoint, locations[0])
