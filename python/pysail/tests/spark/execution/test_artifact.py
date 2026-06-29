"""Artifact handling tests for Sail local-cluster execution."""

import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")

_ARTIFACT_S3_BUCKET = "sail-artifact-store"
_ARTIFACT_S3_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z"
_ARTIFACT_S3_PORT = 9000
_ARTIFACT_S3_USER = "admin"
_ARTIFACT_S3_PASSWORD = "password"  # noqa: S105


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
def artifact_s3_store():
    boto3 = pytest.importorskip("boto3")
    container_module = pytest.importorskip("testcontainers.core.container")
    wait_strategy_module = pytest.importorskip("testcontainers.core.wait_strategies")

    container = container_module.DockerContainer(_ARTIFACT_S3_IMAGE)
    container.with_exposed_ports(_ARTIFACT_S3_PORT)
    container.with_env("MINIO_ROOT_USER", _ARTIFACT_S3_USER)
    container.with_env("MINIO_ROOT_PASSWORD", _ARTIFACT_S3_PASSWORD)
    container.with_command(["server", "/data", "--console-address", ":9001"])
    container.waiting_for(
        wait_strategy_module.LogMessageWaitStrategy("MinIO Object Storage Server").with_startup_timeout(120)
    )
    container.start()

    try:
        endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(_ARTIFACT_S3_PORT)}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=_ARTIFACT_S3_USER,
            aws_secret_access_key=_ARTIFACT_S3_PASSWORD,
            region_name="us-east-1",
        )
        _create_s3_bucket(client)
        yield {
            "client": client,
            "env": {
                "AWS_ACCESS_KEY_ID": _ARTIFACT_S3_USER,
                "AWS_SECRET_ACCESS_KEY": _ARTIFACT_S3_PASSWORD,
                "AWS_REGION": "us-east-1",
                "AWS_ENDPOINT": endpoint,
                "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
                "AWS_ALLOW_HTTP": "true",
            },
            "uri": f"s3://{_ARTIFACT_S3_BUCKET}/artifacts",
        }
    finally:
        container.stop()


def _create_s3_bucket(client):
    deadline = time.time() + 60
    while True:
        try:
            client.create_bucket(Bucket=_ARTIFACT_S3_BUCKET)
        except Exception:
            if time.time() >= deadline:
                raise
            time.sleep(1)
        else:
            return


def _s3_keys(client):
    response = client.list_objects_v2(Bucket=_ARTIFACT_S3_BUCKET)
    return sorted(item["Key"] for item in response.get("Contents", []))


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

        from pyspark.core.files import SparkFiles

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

        from pyspark.core.files import SparkFiles

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
                from pyspark.core.files import SparkFiles

                with open(SparkFiles.get("sail_corrupt_artifact.txt"), encoding="utf-8") as file:
                    return file.read()

            with pytest.raises(Exception, match=r"SHA-256 mismatch|size mismatch"):
                session.range(1).select(read_corrupt_artifact("id").alias("value")).collect()
    finally:
        shutil.rmtree(artifact_root, ignore_errors=True)
        shutil.rmtree(artifact_store, ignore_errors=True)


@pytest.mark.integration
def test_add_artifact_file_uses_s3_object_store(artifact_s3_store, tmp_path):
    if os.environ.get("SPARK_REMOTE"):
        pytest.skip("external Spark Connect endpoint owns the artifact store")

    artifact_root = tempfile.mkdtemp(prefix="sail-s3-artifact-root-")
    try:
        with (
            spark_connect_server(
                envs={
                    "SAIL_MODE": "local-cluster",
                    "SAIL_SPARK__ARTIFACT_ROOT": artifact_root,
                    "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                    "SAIL_SPARK__ARTIFACT_STORE_URI": artifact_s3_store["uri"],
                    **artifact_s3_store["env"],
                }
            ) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            file_path = tmp_path / "sail_s3_artifact.txt"
            payload = "s3 artifact payload " * 8
            file_path.write_text(payload, encoding="utf-8")

            session.addArtifact(str(file_path), file=True)
            stored_keys = _s3_keys(artifact_s3_store["client"])
            assert any(key.startswith("artifacts/sail-artifacts/sessions/") for key in stored_keys)
            shutil.rmtree(artifact_root, ignore_errors=True)

            @udf(StringType())
            def read_s3_artifact(_):
                from pyspark.core.files import SparkFiles

                with open(SparkFiles.get("sail_s3_artifact.txt"), encoding="utf-8") as file:
                    return file.read()

            rows = session.range(8).repartition(4).select(read_s3_artifact("id").alias("value")).collect()
            assert {row.value for row in rows} == {payload}

            session.stop()
            assert not _s3_keys(artifact_s3_store["client"])
    finally:
        shutil.rmtree(artifact_root, ignore_errors=True)


def test_add_artifact_rejects_unsafe_archive_member(spark, tmp_path):
    archive_path = tmp_path / "sail_unsafe_archive.zip"
    with zipfile.ZipFile(str(archive_path), "w") as zf:
        zf.writestr("../escape.txt", "unsafe")

    spark.addArtifact(f"{archive_path}#sail_unsafe", archive=True)

    @udf(StringType())
    def trigger_archive_unpack(_):
        from pyspark.core.files import SparkFiles

        return SparkFiles.getRootDirectory()

    with pytest.raises(Exception, match="unsafe archive member path"):
        spark.range(1).select(trigger_archive_unpack("id").alias("value")).collect()
