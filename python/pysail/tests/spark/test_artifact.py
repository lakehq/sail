"""Tests for artifact handling via SparkSession.addArtifact."""

import zipfile

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

EXPECTED_ARTIFACT_VALUE = 42


def _make_zip(path, module_name, code):
    """Create a zip archive containing a single Python module."""
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


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


def test_add_multiple_artifacts_as_pyfiles(spark, tmp_path):
    """Multiple zip artifacts can be added as pyfiles."""
    for i in range(3):
        zip_path = tmp_path / f"sail_multi_module_{i}.zip"
        _make_zip(zip_path, f"sail_multi_module_{i}.py", f"V = {i}\n")
        spark.addArtifact(str(zip_path), pyfile=True)
