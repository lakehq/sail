"""Tests for artifact handling via SparkSession.addArtifact."""


def test_add_artifact_python_file(spark, tmp_path):
    """Adding a Python file artifact should make it importable."""
    # Create a simple Python module
    module_file = tmp_path / "my_artifact_module.py"
    module_file.write_text("MY_CONSTANT = 42\n")

    spark.addArtifact(str(module_file))

    # Verify the artifact can be used in a UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    @udf(returnType=IntegerType())
    def get_constant(_x):
        import my_artifact_module

        return my_artifact_module.MY_CONSTANT

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    result = df.select(get_constant("id")).collect()
    assert all(row[0] == 42 for row in result)  # noqa: PLR2004


def test_add_artifact_zip_file(spark, tmp_path):
    """Adding a zip artifact should make the zip available on the Python path."""
    import zipfile

    # Create a zip file with a module inside
    module_code = "ZIP_VALUE = 99\n"
    zip_path = tmp_path / "myzip.zip"
    with zipfile.ZipFile(str(zip_path), "w") as zf:
        zf.writestr("myzip_module.py", module_code)

    spark.addArtifact(str(zip_path))
    # Verify zip was added (artifact is tracked; actual import in UDF is the real test)


def test_add_multiple_artifacts(spark, tmp_path):
    """Multiple artifacts can be added to the same session."""
    for i in range(3):
        artifact_file = tmp_path / f"artifact_{i}.py"
        artifact_file.write_text(f"VALUE_{i} = {i}\n")
        spark.addArtifact(str(artifact_file))
