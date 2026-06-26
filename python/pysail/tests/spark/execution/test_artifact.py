"""Artifact handling tests for Sail local-cluster execution."""

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


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


def test_add_artifact_file_is_available_via_spark_files(spark, tmp_path):
    file_path = tmp_path / "sail_file_artifact.txt"
    file_path.write_text("file artifact payload", encoding="utf-8")

    spark.addArtifact(str(file_path), file=True)

    @udf(StringType())
    def read_file_artifact(_):
        from pyspark.core.files import SparkFiles

        with open(SparkFiles.get("sail_file_artifact.txt"), encoding="utf-8") as file:
            return file.read()

    rows = spark.range(8).repartition(4).select(read_file_artifact("id").alias("value")).collect()
    assert {row.value for row in rows} == {"file artifact payload"}


def test_add_artifact_archive_is_unpacked_under_spark_files_root(spark, tmp_path):
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
