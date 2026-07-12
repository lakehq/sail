"""Artifact handling tests for Sail local-cluster execution."""

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")
requires_server_artifact_root_control = pytest.mark.skipif(
    bool(os.environ.get("SPARK_REMOTE")),
    reason="test must remove the configured server artifact root",
)


@pytest.fixture(scope="module")
def artifact_root():
    artifact_root = tempfile.mkdtemp(prefix="sail-artifact-root-")
    try:
        yield artifact_root
    finally:
        shutil.rmtree(artifact_root, ignore_errors=True)


@pytest.fixture(scope="module")
def artifact_store():
    artifact_store = tempfile.mkdtemp(prefix="sail-artifact-store-")
    try:
        yield artifact_store
    finally:
        shutil.rmtree(artifact_store, ignore_errors=True)


@pytest.fixture(scope="module")
def remote(artifact_root, artifact_store):
    """Override the global remote fixture to use local-cluster mode for this module."""
    if os.environ.get("SPARK_REMOTE"):
        with spark_connect_server() as server:
            yield server.remote
        return

    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_SPARK__ARTIFACT_ROOT": artifact_root,
            "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "32",
            "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
        }
    ) as server:
        yield server.remote


def _make_zip(path, module_name, code):
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


@requires_server_artifact_root_control
def test_add_artifact_zip_materializes_when_shared_path_is_missing(spark, artifact_root, tmp_path):
    zip_path = tmp_path / "sail_cluster_artifact.zip"
    _make_zip(zip_path, "sail_cluster_artifact.py", "VALUE = 123\n")

    spark.addArtifact(str(zip_path), pyfile=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    @udf(IntegerType())
    def read_artifact_value(_):
        import sail_cluster_artifact

        return sail_cluster_artifact.VALUE

    rows = spark.range(16).repartition(4).select(read_artifact_value("id").alias("value")).collect()
    assert {row.value for row in rows} == {123}


@requires_server_artifact_root_control
def test_add_artifact_file_is_available_via_spark_files(spark, artifact_root, tmp_path):
    file_path = tmp_path / "sail_file_artifact.txt"
    payload = "file artifact payload " * 4
    file_path.write_text(payload, encoding="utf-8")

    spark.addArtifact(str(file_path), file=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    @udf(StringType())
    def read_file_artifact(_):
        from pyspark import SparkFiles

        with open(SparkFiles.get("sail_file_artifact.txt"), encoding="utf-8") as file:
            return file.read()

    rows = spark.range(8).repartition(4).select(read_file_artifact("id").alias("value")).collect()
    assert {row.value for row in rows} == {payload}


@requires_server_artifact_root_control
def test_map_in_pandas_keeps_artifact_context_for_iterator(spark, artifact_root, tmp_path):
    file_path = tmp_path / "sail_map_iter_artifact.txt"
    payload = "map iterator artifact payload"
    file_path.write_text(payload, encoding="utf-8")

    spark.addArtifact(str(file_path), file=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    def read_file_artifact(iterator):
        from pyspark import SparkFiles

        for pdf in iterator:
            with open(SparkFiles.get("sail_map_iter_artifact.txt"), encoding="utf-8") as file:
                pdf["value"] = file.read()
            yield pdf[["value"]]

    rows = spark.range(8).repartition(4).mapInPandas(read_file_artifact, "value string").collect()
    assert {row.value for row in rows} == {payload}


@pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="Python UDTF artifact test requires PySpark 4+",
)
@requires_server_artifact_root_control
def test_udtf_keeps_artifact_context_for_iterator(spark, artifact_root, tmp_path):
    from pyspark.sql.functions import lit, udtf

    file_path = tmp_path / "sail_udtf_artifact.txt"
    payload = "udtf artifact payload"
    file_path.write_text(payload, encoding="utf-8")

    spark.addArtifact(str(file_path), file=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    @udtf(returnType="value: string")
    class ReadArtifactUDTF:
        def eval(self, _):
            from pyspark import SparkFiles

            with open(SparkFiles.get("sail_udtf_artifact.txt"), encoding="utf-8") as file:
                yield (file.read(),)

    rows = ReadArtifactUDTF(lit(1)).collect()
    assert {row.value for row in rows} == {payload}


@requires_server_artifact_root_control
def test_add_artifact_archive_is_unpacked_under_spark_files_root(spark, artifact_root, tmp_path):
    archive_dir = tmp_path / "archive_payload"
    archive_dir.mkdir()
    (archive_dir / "payload.txt").write_text("archive artifact payload", encoding="utf-8")
    archive_path = shutil.make_archive(
        str(tmp_path / "sail_archive_artifact"),
        "zip",
        tmp_path,
        "archive_payload",
    )

    spark.addArtifact(f"{archive_path}#sail_archive", archive=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    @udf(StringType())
    def read_archive_artifact(_):
        import os

        from pyspark import SparkFiles

        path = os.path.join(
            SparkFiles.getRootDirectory(),
            "sail_archive",
            "archive_payload",
            "payload.txt",
        )
        with open(path, encoding="utf-8") as file:
            return file.read()

    rows = spark.range(8).repartition(4).select(read_archive_artifact("id").alias("value")).collect()
    assert {row.value for row in rows} == {"archive artifact payload"}


@requires_server_artifact_root_control
def test_add_artifact_archive_uses_default_directory_without_fragment(spark, artifact_root, tmp_path):
    archive_dir = tmp_path / "default_archive_payload"
    archive_dir.mkdir()
    (archive_dir / "payload.txt").write_text("default archive payload", encoding="utf-8")
    archive_path = shutil.make_archive(
        str(tmp_path / "sail_default_archive"),
        "zip",
        tmp_path,
        "default_archive_payload",
    )

    spark.addArtifact(archive_path, archive=True)
    shutil.rmtree(artifact_root, ignore_errors=True)

    @udf(StringType())
    def read_default_archive_artifact(_):
        import os

        from pyspark import SparkFiles

        path = os.path.join(
            SparkFiles.getRootDirectory(),
            "sail_default_archive.zip",
            "default_archive_payload",
            "payload.txt",
        )
        with open(path, encoding="utf-8") as file:
            return file.read()

    rows = spark.range(8).repartition(4).select(read_default_archive_artifact("id").alias("value")).collect()
    assert {row.value for row in rows} == {"default archive payload"}


def test_artifact_store_hash_mismatch_fails_closed(tmp_path):
    if os.environ.get("SPARK_REMOTE"):
        pytest.skip("external Spark Connect endpoint owns the artifact store")

    artifact_root = tempfile.mkdtemp(prefix="sail-corrupt-artifact-root-")
    artifact_store = tempfile.mkdtemp(prefix="sail-corrupt-artifact-store-")
    try:
        with (
            spark_connect_server(
                envs={
                    "SAIL_MODE": "local-cluster",
                    "SAIL_SPARK__ARTIFACT_ROOT": artifact_root,
                    "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "32",
                    "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
                }
            ) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            file_path = tmp_path / "sail_corrupt_artifact.txt"
            file_path.write_text("corruptible artifact payload " * 4, encoding="utf-8")

            existing_files = {path for path in Path(artifact_store).rglob("*") if path.is_file()}
            session.addArtifact(str(file_path), file=True)
            stored_files = [
                path for path in Path(artifact_store).rglob("*") if path.is_file() and path not in existing_files
            ]
            assert stored_files
            stored_files[0].write_text("corrupted", encoding="utf-8")
            shutil.rmtree(artifact_root, ignore_errors=True)

            @udf(StringType())
            def read_corrupt_artifact(_):
                from pyspark import SparkFiles

                with open(SparkFiles.get("sail_corrupt_artifact.txt"), encoding="utf-8") as file:
                    return file.read()

            with pytest.raises(Exception, match=r"SHA-256 mismatch|size mismatch"):
                session.range(1).select(read_corrupt_artifact("id").alias("value")).collect()
    finally:
        shutil.rmtree(artifact_root, ignore_errors=True)
        shutil.rmtree(artifact_store, ignore_errors=True)


def test_add_artifact_rejects_unsafe_archive_member(spark, tmp_path):
    archive_path = tmp_path / "sail_unsafe_archive.zip"
    with zipfile.ZipFile(str(archive_path), "w") as zf:
        zf.writestr("../escape.txt", "unsafe")

    spark.addArtifact(f"{archive_path}#sail_unsafe", archive=True)

    @udf(StringType())
    def trigger_archive_unpack(_):
        from pyspark import SparkFiles

        return SparkFiles.getRootDirectory()

    with pytest.raises(Exception, match=r"unsafe archive member path|safe relative path"):
        spark.range(1).select(trigger_archive_unpack("id").alias("value")).collect()
