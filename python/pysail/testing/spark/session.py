from __future__ import annotations

import contextlib
import importlib
import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


@dataclass(frozen=True)
class _ServerHandle:
    remote: str


class _SparkSessionFactory:
    def __init__(self, remote: str) -> None:
        self._remote = remote
        self._sessions: list[SparkSession] = []

    def create(self) -> SparkSession:
        app = f"test-{uuid.uuid4().hex}"
        session = SparkSession.builder.appName(app).remote(self._remote).create()
        configure_spark_session(session)
        patch_spark_connect_session(session)
        self._sessions.append(session)
        return session

    def stop(self) -> None:
        for session in self._sessions:
            # Best-effort cleanup: ignore errors while stopping Spark sessions during test teardown.
            with contextlib.suppress(Exception):
                session.stop()


@contextlib.contextmanager
def spark_connect_server(envs: Mapping[str, str] | None = None) -> Iterator[_ServerHandle]:
    """Create a Spark Connect server unless `SPARK_REMOTE` already points to one."""
    if remote := os.environ.get("SPARK_REMOTE"):
        if envs is not None:
            pytest.skip("the server from `SPARK_REMOTE` may not be compatible with the custom environment variables")
        yield _ServerHandle(remote=remote)
        return

    envs = envs or {}
    with pytest.MonkeyPatch.context() as mp:
        for key, value in envs.items():
            mp.setenv(key, value)

        spark_connect_server_type = importlib.import_module("pysail.spark").SparkConnectServer
        server = spark_connect_server_type("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        try:
            yield _ServerHandle(remote=f"sc://localhost:{port}")
        finally:
            server.stop()


@contextlib.contextmanager
def spark_session_factory(remote: str) -> Iterator[_SparkSessionFactory]:
    """Create Spark sessions connected to the given Spark Connect remote."""
    sessions = _SparkSessionFactory(remote)
    try:
        yield sessions
    finally:
        sessions.stop()


def configure_spark_session(session: SparkSession) -> None:
    # Set the Spark session time zone to UTC by default.
    # Some test data (e.g. TPC-DS data) may generate timestamps that is invalid
    # in some local time zones. This would result in `pytz.exceptions.NonExistentTimeError`
    # when converting such timestamps from the local time zone to UTC.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    # Pin ANSI mode so plan snapshots are stable across PySpark 3.x and 4.x test environments.
    session.conf.set("spark.sql.ansi.enabled", "true")
    # Enable Arrow to avoid data type errors when creating Spark DataFrame from Pandas.
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def patch_spark_connect_session(session: SparkSession) -> None:
    """Patch the Spark Connect session to avoid deadlock when closing the session."""
    f = session._client.close  # noqa: SLF001

    def close():
        if session._client._closed:  # noqa: SLF001
            return None
        return f()

    session._client.close = close  # noqa: SLF001
