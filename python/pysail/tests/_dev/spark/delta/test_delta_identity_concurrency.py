"""Concurrent identity allocation must not publish a stale watermark."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow.parquet as pq
import pytest
from pyspark.errors import PySparkException
from pyspark.sql import functions as sf

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(scope="module")
def remote():
    # Preserve SAIL_MODE, but do not automatically reexecute the deliberately stale plan.
    with spark_connect_server(
        {
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": "64",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
        }
    ) as server:
        yield server.remote


def _log(path):
    return {file.name: file.read_bytes() for file in sorted((path / "_delta_log").glob("*.json"))}


def _assert_snapshot(spark, path, expected):
    active = set()
    metadata = None
    for contents in _log(path).values():
        for line in contents.splitlines():
            action = json.loads(line)
            if "add" in action:
                active.add(action["add"]["path"])
            if "remove" in action:
                active.discard(action["remove"]["path"])
            metadata = action.get("metaData", metadata)
    assert metadata is not None
    field = next(field for field in json.loads(metadata["schemaString"])["fields"] if field["name"] == "id")
    assert field["metadata"]["delta.identity.highWaterMark"] == expected[-1][0]
    assert spark.read.format("delta").load(str(path)).orderBy("k").collect() == expected
    disk_rows = [row for name in active for row in pq.ParquetFile(path / name).read().to_pylist()]
    assert sorted((row["id"], row["k"]) for row in disk_rows) == expected
    assert len({row["id"] for row in disk_rows}) == len(expected)
    return active


@pytest.fixture
def merge_gate(tmp_path):
    both = threading.Event()
    release_second = threading.Event()
    arrivals = []
    lock = threading.Lock()
    path = tmp_path / "table"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            writer = int(self.path.strip("/"))
            with lock:
                arrivals.append((writer, sorted(_log(path))))
                if {writer for writer, _ in arrivals} == {1, 2}:
                    both.set()
            permitted = both.wait(40) if writer == 1 else release_second.wait(70)
            self.send_response(200 if permitted else 500)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def source(spark, writer):
        endpoint = f"http://127.0.0.1:{server.server_port}/{writer}"

        def wait_at_gate(value):
            import urllib.request

            with urllib.request.urlopen(endpoint, timeout=75) as response:  # noqa: S310
                response.read()
            return value

        gate = sf.udf(wait_at_gate, "long").asNondeterministic()
        spark.range(writer, writer + 1, numPartitions=1).select(gate(sf.col("id")).alias("k")).createOrReplaceTempView(
            "gated_source"
        )

    try:
        yield source, release_second, arrivals
    finally:
        both.set()
        release_second.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.0.0/spark/src/test/scala/org/apache/spark/sql/delta/IdentityColumnConflictSuite.scala#L280-L300
@pytest.mark.parametrize("isolation", ["SnapshotIsolation", "WriteSerializable", "Serializable"])
def test_concurrent_identity_merge_rejects_stale_allocation(remote, tmp_path, merge_gate, isolation):
    source, release_second, arrivals = merge_gate
    path = tmp_path / "table"
    location = escape_sql_string_literal(str(path))
    query = """
        MERGE INTO identity_concurrency t USING gated_source s ON t.k=s.k
        WHEN NOT MATCHED THEN INSERT (k) VALUES (s.k)
    """
    with spark_session_factory(remote) as sessions:
        first, second = sessions.create(), sessions.create()
        for spark in (first, second):
            spark.sql("SET datafusion.execution.target_partitions=1").collect()
        first.sql(f"""
            CREATE TABLE identity_concurrency (
                id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 3), k BIGINT
            ) USING DELTA LOCATION '{location}'
            TBLPROPERTIES ('delta.isolationLevel'='{isolation}')
        """).collect()
        try:
            first.sql("INSERT INTO identity_concurrency (k) VALUES (0)").collect()
            second.sql(f"CREATE TABLE identity_concurrency USING DELTA LOCATION '{location}'").collect()
            initial = _log(path)
            for writer, spark in enumerate((first, second), start=1):
                source(spark, writer)
            with ThreadPoolExecutor(max_workers=2) as executor:
                winner = executor.submit(lambda: first.sql(query).collect())
                loser = executor.submit(lambda: second.sql(query).collect())
                try:
                    winner.result(timeout=90)
                finally:
                    release_second.set()
                with pytest.raises(PySparkException, match=r"^Failed to commit transaction: 0$"):
                    loser.result(timeout=90)

            assert {writer for writer, _ in arrivals} == {1, 2}
            assert all(versions == sorted(initial) for _, versions in arrivals)
            after = _log(path)
            assert len(after) == len(initial) + 1
            assert all(after[name] == contents for name, contents in initial.items())
            actions = [json.loads(line) for line in list(after.values())[-1].splitlines()]
            info = next(action["commitInfo"] for action in actions if "commitInfo" in action)
            assert info["operation"] == "MERGE"
            assert info["isBlindAppend"] is False
            metadata = next(action["metaData"] for action in actions if "metaData" in action)
            assert metadata["configuration"]["delta.isolationLevel"] == isolation
            active = _assert_snapshot(first, path, [(10, 0), (13, 1)])
            aborted = {str(file.relative_to(path)) for file in path.rglob("*.parquet")} - active
            assert len(aborted) == 1
            orphan = pq.ParquetFile(path / next(iter(aborted))).read().to_pylist()
            assert orphan == [{"id": 13, "k": 2}]

            # A new statement must replan against the committed watermark, not reuse ID 13.
            second.sql(query).collect()
            final = _log(path)
            assert len(final) == len(after) + 1
            assert all(final[name] == contents for name, contents in after.items())
            active = _assert_snapshot(first, path, [(10, 0), (13, 1), (16, 2)])
            assert aborted.isdisjoint(active)
        finally:
            for spark in (first, second):
                spark.sql("DROP TABLE IF EXISTS identity_concurrency").collect()
