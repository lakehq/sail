"""Snapshot annotations stay operation-scoped across REST publication."""

# ruff: noqa: T201

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.catalog.iceberg_rest.test_commit import _s3_object_keys


@pytest.fixture(scope="module")
def recording_catalog(iceberg_rest_endpoint):
    records = []

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("content-length", "0")))
            response = requests.request(
                self.command,
                iceberg_rest_endpoint + self.path,
                data=body or None,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            records.append((self.command, self.path, json.loads(body) if body else None, response.status_code))
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
def remote(recording_catalog, seaweedfs_host_endpoint):
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


def _load(endpoint, name):
    response = requests.get(f"{endpoint}/v1/namespaces/annotations/tables/{name}", timeout=30)
    response.raise_for_status()
    return response.json()


def _current(metadata):
    return next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])


def _check_metadata(payload, expected, history):
    metadata = payload["metadata"]
    assert payload["metadata-location"].startswith(metadata["location"] + "/metadata/")
    summary = _current(metadata)["summary"]
    assert summary.get("run-id") == expected
    assert not any("snapshot-property." in key for key in metadata.get("properties", {}))
    snapshots = {s["snapshot-id"]: s for s in metadata["snapshots"]}
    for old in history:
        assert snapshots[old["snapshot-id"]] == old
    history.append(_current(metadata))
    print("AUTHORITATIVE_METADATA " + json.dumps(payload, sort_keys=True))
    return summary


def test_rest_annotations_survive_workers_without_catalog_leakage(spark, recording_catalog):
    endpoint, records = recording_catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS annotations")
    table = "annotations.lifecycle"
    history = []
    try:
        spark.range(2).repartition(4).writeTo(table).using("iceberg").option(
            "snapshot-property.run-id", "first"
        ).create()
        summary = _check_metadata(_load(endpoint, "lifecycle"), "first", history)
        assert summary["total-records"] == "2"
        spark.range(2, 3).repartition(4).writeTo(table).append()
        summary = _check_metadata(_load(endpoint, "lifecycle"), None, history)
        assert summary["total-records"] == "3"
        spark.range(3, 4).repartition(4).writeTo(table).option("snapshot-property.run-id", "replacement").overwrite(
            F.lit(True)
        )
        summary = _check_metadata(_load(endpoint, "lifecycle"), "replacement", history)
        assert summary["operation"] == "overwrite"
        assert summary["total-records"] == "1"
        spark.range(4, 5).repartition(4).writeTo(table).append()
        summary = _check_metadata(_load(endpoint, "lifecycle"), None, history)
        assert summary["total-records"] == "2"
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == [3, 4]
        mutations = [r for r in records if r[0] == "POST" and "/tables" in r[1]]
        assert mutations
        assert any(r[2].get("name") == "lifecycle" for r in mutations)
        assert any(u.get("action") == "add-snapshot" for r in mutations for u in r[2].get("updates", []))
        for _, _, body, _ in mutations:
            assert not any("snapshot-property." in k for k in body.get("properties", {}))
            for update in body.get("updates", []):
                if update.get("action") == "set-properties":
                    assert not any("snapshot-property." in k for k in update.get("updates", {}))
        print("CATALOG_MUTATIONS " + json.dumps(mutations, sort_keys=True))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_rest_invalid_annotation_does_not_mutate_catalog_or_storage(spark, recording_catalog, seaweedfs_host_endpoint):
    endpoint, records = recording_catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS annotations")
    table = "annotations.protected"
    spark.range(2).writeTo(table).using("iceberg").create()
    try:
        before = _load(endpoint, "protected")
        keys = _s3_object_keys(seaweedfs_host_endpoint, "s3://icebergdata/demo")
        start = len(records)
        writer = spark.range(10).writeTo(table).using("iceberg").option("snapshot-property.operation", "bad")
        with pytest.raises(Exception, match="reserved"):
            writer.replace()
        assert _load(endpoint, "protected") == before
        assert _s3_object_keys(seaweedfs_host_endpoint, "s3://icebergdata/demo") == keys
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [0, 1]
        writer = (
            spark.range(10).writeTo("annotations.invalid").using("iceberg").option("snapshot-property.operation", "bad")
        )
        with pytest.raises(Exception, match="reserved"):
            writer.create()
        assert not spark.catalog.tableExists("annotations.invalid")
        assert _s3_object_keys(seaweedfs_host_endpoint, "s3://icebergdata/demo") == keys
        assert not [r for r in records[start:] if r[0] in {"POST", "DELETE"}]
        print("REJECTION_PRESERVED " + json.dumps({"catalog": before, "objects": sorted(keys)}))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_persisted_prefixed_properties_never_become_annotations(spark, recording_catalog):
    endpoint, _ = recording_catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS annotations")
    table = "annotations.persisted"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT) USING iceberg "
            "TBLPROPERTIES ('snapshot-property.raw'='stored', 'option.snapshot-property.wrapped'='stored')"
        )
        spark.range(2).repartition(4).writeTo(table).append()
        payload = _load(endpoint, "persisted")
        properties = payload["metadata"]["properties"]
        assert properties["snapshot-property.raw"] == "stored"
        assert properties["option.snapshot-property.wrapped"] == "stored"
        summary = _current(payload["metadata"])["summary"]
        assert "raw" not in summary
        assert "wrapped" not in summary
        assert not any("snapshot-property." in key for key in summary)
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [0, 1]
        print("PERSISTED_PROPERTY_ISOLATION " + json.dumps(payload, sort_keys=True))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
