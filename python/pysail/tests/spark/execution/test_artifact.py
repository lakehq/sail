"""Artifact handling tests for Sail local-cluster execution."""

import os
import shutil
import tempfile
import zipfile

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

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
def remote(artifact_root):
    """Override the global remote fixture to use local-cluster mode for this module."""
    if os.environ.get("SPARK_REMOTE"):
        with spark_connect_server() as server:
            yield server.remote
        return

    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_SPARK__ARTIFACT_ROOT": artifact_root,
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
