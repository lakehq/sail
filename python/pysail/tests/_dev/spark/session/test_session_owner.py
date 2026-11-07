from uuid import uuid4

import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import SparkSession

from pysail.testing.spark.session import patch_spark_connect_session

pytestmark = [
    pytest.mark.spark(version=">=4", reason="Server-side session tracking requires PySpark 4+"),
    pytest.mark.spark(skip_if_jvm=True, reason="PySpark allows only one remote per process in local mode"),
]


def test_same_user_same_session_id(remote: str) -> None:
    sid = str(uuid4())
    spark1 = _create_session(_url(remote, "user1", sid))
    spark2 = _create_session(_url(remote, "user1", sid))
    try:
        spark1.conf.set("key", "1")
        assert spark1.conf.get("key") == "1"
        spark2.conf.set("key", "2")
        assert spark2.conf.get("key") == "2"
        assert spark1.conf.get("key") == "2"

        assert spark1.session_id == sid
        assert spark2.session_id == sid
        assert spark1._client._server_session_id is not None  # noqa: SLF001
        assert spark2._client._server_session_id is not None  # noqa: SLF001
        assert spark1._client._server_session_id != sid  # noqa: SLF001
        assert spark2._client._server_session_id != sid  # noqa: SLF001
        assert spark1._client._server_session_id == spark2._client._server_session_id  # noqa: SLF001

        spark2.stop()
        with pytest.raises(SparkConnectGrpcException, match="SESSION_CLOSED"):
            spark1.sql("SELECT 1 V").first()
    finally:
        spark2.stop()
        spark1.stop()


def test_different_users_same_session_id(remote: str) -> None:
    sid = str(uuid4())
    spark1 = _create_session(_url(remote, "user1", sid))
    spark2 = _create_session(_url(remote, "user2", sid))
    try:
        spark1.conf.set("key", "1")
        assert spark1.conf.get("key") == "1"
        spark2.conf.set("key", "2")
        assert spark2.conf.get("key") == "2"
        assert spark1.conf.get("key") == "1"

        assert spark1.session_id == sid
        assert spark2.session_id == sid
        assert spark1._client._server_session_id is not None  # noqa: SLF001
        assert spark2._client._server_session_id is not None  # noqa: SLF001
        assert spark1._client._server_session_id != sid  # noqa: SLF001
        assert spark2._client._server_session_id != sid  # noqa: SLF001
        assert spark1._client._server_session_id != spark2._client._server_session_id  # noqa: SLF001

        spark2.stop()
        assert spark1.sql("SELECT 1 VAL").first()["VAL"] == 1
    finally:
        spark2.stop()
        spark1.stop()


def test_server_restart_detection(remote: str) -> None:
    spark = _create_session(remote)
    try:
        assert spark.sql("SELECT 1 VAL").first()["VAL"] == 1
        # simulate a server restart: the server now has a different ID than the client observed
        spark._client._server_session_id = str(uuid4())  # noqa: SLF001
        with pytest.raises(SparkConnectGrpcException, match="SESSION_CHANGED") as e:
            spark.sql("SELECT 2 VAL").first()
        assert e.value.getCondition() == "INVALID_HANDLE.SESSION_CHANGED"
        assert e.value.getMessageParameters() == {"handle": spark.session_id}
        assert spark._client._closed  # noqa: SLF001
    finally:
        spark.stop()


def _create_session(remote: str) -> SparkSession:
    session = SparkSession.builder.remote(remote).create()
    # These tests need custom user/session IDs, but still need the same cleanup
    # guard as sessions created by the shared test factory.
    patch_spark_connect_session(session)
    return session


def _url(remote: str, user_id: str, session_id: str) -> str:
    sep = ";" if remote.endswith("/") or "/;" in remote else "/;"
    return f"{remote}{sep}user_id={user_id};session_id={session_id}"
