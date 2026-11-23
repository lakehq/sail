# SPDX-License-Identifier: Apache-2.0

import os
import re
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager

import pytest

pytestmark = [
    pytest.mark.skipif(os.name != "posix", reason="requires Unix process signals"),
    pytest.mark.timeout(30),
]


@contextmanager
def _server(protocol, mode, **env_vars):
    command = [sys.executable, "-m", "pysail", protocol, "server", "--port", "0"]
    env = {**os.environ, "SAIL_MODE": mode, "RUST_LOG": "sail_cli=info", **env_vars}
    with subprocess.Popen(command, env=env, stderr=subprocess.PIPE, text=True) as server:
        try:
            for line in server.stderr:
                if match := re.search(r"Starting the .* server on (127\.0\.0\.1:\d+)", line):
                    yield server, match[1]
                    return
            pytest.fail(f"{protocol} server exited during startup")
        finally:
            if server.poll() is None:
                server.kill()


@pytest.mark.parametrize("mode", ["local", "local-cluster"])
def test_flight_sigterm_cancels_running_query(mode):
    from adbc_driver_flightsql import dbapi

    with _server("flight", mode) as (server, address), dbapi.connect(f"grpc://{address}") as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM range(0, 1000000000, 1, 1)")
        reader = cur.fetch_record_batch()
        assert reader.read_next_batch().num_rows > 0
        server.send_signal(signal.SIGTERM)
        with pytest.raises(OSError, match="query cancelled"):
            for _ in reader:
                pass
        assert server.wait(timeout=10) == 0


@pytest.mark.parametrize("mode", ["local", "local-cluster"])
def test_spark_sigterm_cancels_running_query(mode):
    import grpc
    from pyspark.sql.connect.proto import ExecutePlanRequest
    from pyspark.sql.connect.proto.base_pb2_grpc import SparkConnectServiceStub

    with _server("spark", mode) as (server, address), grpc.insecure_channel(address) as channel:
        request = ExecutePlanRequest(session_id=str(uuid.uuid4()))
        request.user_context.user_id = "test"
        request.plan.root.range.end = 1000000000
        request.plan.root.range.step = 1
        request.plan.root.range.num_partitions = 1
        responses = SparkConnectServiceStub(channel).ExecutePlan(request, timeout=20, wait_for_ready=True)
        assert any(response.arrow_batch.row_count > 0 for response in responses)
        server.send_signal(signal.SIGTERM)
        with pytest.raises(grpc.RpcError, match="query cancelled"):
            for _ in responses:
                pass
        assert server.wait(timeout=10) == 0


def test_sigterm_closes_stalled_flight_connection():
    from adbc_driver_flightsql import dbapi

    with _server("flight", "local") as (server, addr), dbapi.connect(f"grpc://{addr}") as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM range(0, 1000000000, 1, 1)")
        reader = cur.fetch_record_batch()
        assert reader.read_next_batch().num_rows > 0
        server.send_signal(signal.SIGTERM)
        # Keep the RPC open without consuming further batches until the server exits.
        assert server.wait(timeout=12) == 0


@pytest.mark.parametrize("kind", ["scalar", "iterator"])
def test_spark_cancels_query_while_python_udf_is_blocked(kind, tmp_path):
    import grpc
    from pyspark.sql import SparkSession, functions
    from pyspark.sql.connect.proto import ExecutePlanRequest
    from pyspark.sql.connect.proto.base_pb2_grpc import SparkConnectServiceStub
    from pyspark.sql.types import LongType

    released = tmp_path / "client_received_error"
    timed_out = tmp_path / "udf_timed_out"
    with _server("spark", "local", TOKIO_WORKER_THREADS="1") as (server, address):
        server_pid = server.pid

        def wait_for_client():
            assert os.getpid() == server_pid
            os.kill(server_pid, signal.SIGTERM)
            deadline = time.monotonic() + 10
            while not released.exists():
                if time.monotonic() > deadline:
                    timed_out.touch()
                    break
                time.sleep(0.025)

        def scalar(value):
            wait_for_client()
            return value

        def iterator(batches):
            for batch in batches:
                wait_for_client()
                yield batch

        spark = SparkSession.builder.remote(f"sc://{address}").create()
        try:
            df = spark.range(0, 1, 1, 1)
            if kind == "scalar":
                df = df.select(functions.udf(scalar, LongType())("id"))
            else:
                df = df.mapInPandas(iterator, "id long")
            # Use a non-reattachable RPC so client retries cannot hide a truncated result.
            request = ExecutePlanRequest(session_id=str(uuid.uuid4()))
            request.user_context.user_id = "test"
            request.plan.CopyFrom(df._plan.to_proto(spark.client))  # noqa: SLF001
        finally:
            spark.stop()

        with grpc.insecure_channel(address) as channel:
            responses = SparkConnectServiceStub(channel).ExecutePlan(request, timeout=20, wait_for_ready=True)
            try:
                with pytest.raises(grpc.RpcError, match="query cancelled"):
                    for _ in responses:
                        pass
            finally:
                released.touch()
        assert server.wait(timeout=10) == 0
        assert not timed_out.exists()
