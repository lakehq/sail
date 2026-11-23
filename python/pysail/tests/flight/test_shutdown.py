# SPDX-License-Identifier: Apache-2.0

import os
import re
import signal
import subprocess
import sys

import pytest
from adbc_driver_flightsql import dbapi


@pytest.mark.skipif(os.name != "posix", reason="requires Unix process signals")
@pytest.mark.timeout(30)
@pytest.mark.parametrize("mode", ["local", "local-cluster"])
def test_sigterm_cancels_running_query(mode):
    command = [sys.executable, "-m", "pysail", "flight", "server", "--port", "0"]
    env = {**os.environ, "SAIL_MODE": mode, "RUST_LOG": "sail_cli=info"}
    with subprocess.Popen(command, env=env, stderr=subprocess.PIPE, text=True) as server:
        try:
            for line in server.stderr:
                if match := re.search(r"Starting the Flight SQL server on (127\.0\.0\.1:\d+)", line):
                    break
            else:
                pytest.fail("Flight SQL server exited during startup")

            with dbapi.connect(f"grpc://{match[1]}") as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM range(0, 1000000000, 1, 1)")
                reader = cur.fetch_record_batch()
                assert reader.read_next_batch().num_rows > 0
                server.send_signal(signal.SIGTERM)
                with pytest.raises(OSError, match="query cancelled"):
                    for _ in reader:
                        pass
                assert server.wait(timeout=10) == 0
        finally:
            if server.poll() is None:
                server.kill()
