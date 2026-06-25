"""Artifact handling tests for Sail local-cluster execution."""

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
def remote():
    """Override the global remote fixture to use local-cluster mode for this module."""
    artifact_root = tempfile.mkdtemp(prefix="sail-artifact-root-")
    try:
        with spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_SPARK__ARTIFACT_ROOT": artifact_root,
            }
        ) as server:
            yield server.remote
    finally:
        shutil.rmtree(artifact_root, ignore_errors=True)


def _make_zip(path, module_name, code):
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


def test_add_artifact_zip_as_pyfile_on_worker(spark, tmp_path):
    zip_path = tmp_path / "sail_cluster_artifact.zip"
    _make_zip(zip_path, "sail_cluster_artifact.py", "VALUE = 123\n")

    spark.addArtifact(str(zip_path), pyfile=True)

    @udf(IntegerType())
    def read_artifact_value(_):
        import sail_cluster_artifact

        return sail_cluster_artifact.VALUE

    rows = spark.range(16).repartition(4).select(read_artifact_value("id").alias("value")).collect()
    assert {row.value for row in rows} == {123}
