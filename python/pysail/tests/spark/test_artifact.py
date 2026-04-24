"""Tests for artifact handling via SparkSession.addArtifact."""

import zipfile


def _make_zip(path, module_name, code):
    """Create a zip archive containing a single Python module."""
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


def test_add_artifact_zip_as_pyfile(spark, tmp_path):
    """Adding a zip archive as a pyfile artifact should succeed without error.

    PySpark Connect's addArtifact only accepts .zip/.egg/.whl when pyfile=True;
    raw .py files or plain .zip files are rejected by the client.
    """
    zip_path = tmp_path / "sail_test_module.zip"
    _make_zip(zip_path, "sail_test_module.py", "VALUE = 42\n")

    # Should not raise
    spark.addArtifact(str(zip_path), pyfile=True)


def test_add_multiple_artifacts_as_pyfiles(spark, tmp_path):
    """Multiple zip artifacts can be added as pyfiles."""
    for i in range(3):
        zip_path = tmp_path / f"sail_multi_module_{i}.zip"
        _make_zip(zip_path, f"sail_multi_module_{i}.py", f"V = {i}\n")
        spark.addArtifact(str(zip_path), pyfile=True)
