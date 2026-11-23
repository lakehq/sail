# SPDX-License-Identifier: Apache-2.0

import os
import re
import signal
import subprocess
import sys
import uuid
from contextlib import contextmanager

import pytest

pytestmark = [
    pytest.mark.skipif(os.name != "posix", reason="requires Unix process signals"),
    pytest.mark.timeout(30),
]


@contextmanager
def _server(protocol, mode):
    command = [sys.executable, "-m", "pysail", protocol, "server", "--port", "0"]
    env = {**os.environ, "SAIL_MODE": mode, "RUST_LOG": "sail_cli=info"}
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
