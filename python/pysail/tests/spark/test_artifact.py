import sys
import zipfile

import pytest
from pyspark import SparkFiles
from pyspark.sql.functions import udf

from pysail.testing.spark.session import spark_connect_server


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server() as server:
        yield server.remote


def test_runtime_artifacts_survive_source_removal(spark, tmp_path):
    plain_module = tmp_path / "sail_artifact_plain.py"
    plain_module.write_text("VALUE = 'plain'\n")

    package = tmp_path / "sail_artifact_package.zip"
    with zipfile.ZipFile(package, "w") as archive:
        archive.writestr("sail_artifact_zipped.py", "VALUE = 'zipped'\n")

    text_file = tmp_path / "sail_artifact_file.txt"
    text_file.write_text("file")

    archive_file = tmp_path / "sail_artifact_archive.zip"
    with zipfile.ZipFile(archive_file, "w") as archive:
        archive.writestr("nested/value.txt", "archive")

    threshold_key = "spark.sql.session.localRelationCacheThreshold"
    previous_threshold = spark.conf.get(threshold_key)
    previous_spark_files_root = getattr(SparkFiles, "_root_directory")
    previous_spark_files_worker = getattr(SparkFiles, "_is_running_on_worker")
    package_path = str(package)
    try:
        spark.conf.set(threshold_key, "0")
        data = spark.createDataFrame([(2,), (1,)], ["id"])

        spark.addArtifact(str(plain_module), pyfile=True)
        spark.addArtifact(package_path, pyfile=True)
        spark.addArtifact(str(text_file), file=True)
        spark.addArtifact(f"{archive_file}#expanded", archive=True)

        plain_module.unlink()
        package.unlink()
        text_file.unlink()
        archive_file.unlink()

        @udf("string")
        def read_artifacts(value):
            from pathlib import Path

            from pyspark import SparkFiles
            from sail_artifact_plain import VALUE as plain_value
            from sail_artifact_zipped import VALUE as zipped_value

            file_value = Path(SparkFiles.get("sail_artifact_file.txt")).read_text()
            archive_value = (Path(SparkFiles.get("expanded")) / "nested" / "value.txt").read_text()
            return f"{value}:{plain_value}:{zipped_value}:{file_value}:{archive_value}"

        result = data.repartition(4).select("id", read_artifacts("id").alias("value")).orderBy("id").collect()
        assert [row.value for row in result] == [
            "1:plain:zipped:file:archive",
            "2:plain:zipped:file:archive",
        ]
        assert getattr(SparkFiles, "_root_directory") == previous_spark_files_root
        assert getattr(SparkFiles, "_is_running_on_worker") == previous_spark_files_worker
    finally:
        spark.conf.set(threshold_key, previous_threshold)
        while package_path in sys.path:
            sys.path.remove(package_path)
