"""Tests for artifact handling via SparkSession.addArtifact."""
# ruff: noqa: SLF001

import zipfile
import zlib
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import pyspark_version

EXPECTED_ARTIFACT_VALUE = 42


def _artifact_store_files(path):
    return [entry for entry in path.rglob("*") if entry.is_file()]


def _make_zip(path, module_name, code):
    """Create a zip archive containing a single Python module."""
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


def _artifact_statuses(spark, names):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager
    request = proto.ArtifactStatusesRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        names=names,
    )
    return manager._stub.ArtifactStatus(request, metadata=manager._metadata)


def test_add_artifact_zip_as_pyfile(spark, tmp_path):
    """Adding a zip archive as a pyfile artifact should succeed without error.

    The module must also be importable when a Python UDF is deserialized and
    executed by the Sail server.
    """
    zip_path = tmp_path / "sail_test_module.zip"
    _make_zip(zip_path, "sail_test_module.py", f"VALUE = {EXPECTED_ARTIFACT_VALUE}\n")

    spark.addArtifact(str(zip_path), pyfile=True)

    @udf(IntegerType())
    def read_artifact_value(_):
        import sail_test_module

        return sail_test_module.VALUE

    rows = spark.range(1).select(read_artifact_value("id").alias("value")).collect()
    assert rows[0].value == EXPECTED_ARTIFACT_VALUE


def test_add_artifact_pyfile_status_and_duplicate_contract(spark, tmp_path):
    module_a = tmp_path / "a" / "sail_status_module.py"
    module_a.parent.mkdir()
    module_a.write_text("VALUE = 7\n", encoding="utf-8")

    spark.addArtifact(str(module_a), pyfile=True)
    spark.addArtifact(str(module_a), pyfile=True)

    statuses = _artifact_statuses(
        spark,
        [
            "pyfiles/sail_status_module.py",
            "pyfiles/sail_missing_module.py",
        ],
    ).statuses
    assert statuses["pyfiles/sail_status_module.py"].exists
    assert not statuses["pyfiles/sail_missing_module.py"].exists

    module_b = tmp_path / "b" / "sail_status_module.py"
    module_b.parent.mkdir()
    module_b.write_text("VALUE = 8\n", encoding="utf-8")
    with pytest.raises(Exception, match=r"already exists|different content|ARTIFACT_ALREADY_EXISTS"):
        spark.addArtifact(str(module_b), pyfile=True)


def test_add_multiple_artifacts_as_pyfiles(spark, tmp_path):
    """Multiple zip artifacts can be added as pyfiles."""
    for i in range(3):
        zip_path = tmp_path / f"sail_multi_module_{i}.zip"
        _make_zip(zip_path, f"sail_multi_module_{i}.py", f"V = {i}\n")
        spark.addArtifact(str(zip_path), pyfile=True)


def test_add_artifacts_crc_failure_is_reported_without_storing(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"crc payload"
    manager = spark._client._artifact_manager
    bad_crc = (zlib.crc32(data) + 1) & 0xFFFFFFFF
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="files/sail_bad_crc.txt",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=bad_crc),
                )
            ]
        ),
    )

    response = manager._retrieve_responses(iter([request]))

    assert response.artifacts[0].name == "files/sail_bad_crc.txt"
    assert not response.artifacts[0].is_crc_successful
    statuses = _artifact_statuses(spark, ["files/sail_bad_crc.txt"]).statuses
    assert not statuses["files/sail_bad_crc.txt"].exists


def test_large_artifact_requires_object_store_when_not_inline(tmp_path):
    module_path = tmp_path / "sail_requires_store.py"
    module_path.write_text("VALUE = " + repr("x" * 64) + "\n", encoding="utf-8")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        with pytest.raises(Exception, match="artifact_store_uri"):
            session.addArtifact(str(module_path), pyfile=True)


@pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="PySpark 3.x Connect session.stop() does not release the server session",
)
def test_session_stop_cleans_object_store_artifacts(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_path = tmp_path / "sail_cleanup_module.py"
    module_path.write_text("VALUE = " + repr("x" * 64) + "\n", encoding="utf-8")

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_path), pyfile=True)
        assert _artifact_store_files(artifact_store)

        session.stop()

        assert not _artifact_store_files(artifact_store)
