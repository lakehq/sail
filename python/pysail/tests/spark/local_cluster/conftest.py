from __future__ import annotations

import os
from typing import Generator

import pytest


@pytest.fixture(scope="session")
def remote() -> Generator[str, None, None]:
    """Start a Spark Connect server in Sail local-cluster mode for this test subtree."""
    original_mode = os.environ.get("SAIL_MODE")
    os.environ["SAIL_MODE"] = "local-cluster"

    try:
        # Import lazily to avoid importing `pysail._native` before
        # `pysail/tests/conftest.py::pytest_configure` sets up the environment.
        from pysail.spark import SparkConnectServer

        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        if original_mode is None:
            os.environ.pop("SAIL_MODE", None)
        else:
            os.environ["SAIL_MODE"] = original_mode

