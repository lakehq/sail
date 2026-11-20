# SPDX-License-Identifier: Apache-2.0

"""Pytest fixtures for reading paths in more than one object store, using Moto for S3."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope="module")
def remote(moto_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server that reads S3 from Moto."""
    with spark_connect_server(
        envs={
            "AWS_ENDPOINT": moto_endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_REGION": "us-east-1",
        },
    ) as server:
        yield server.remote
