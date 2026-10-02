import uuid
import zipfile

import grpc
import pytest
from pyspark.errors.exceptions.connect import UnsupportedOperationException
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def artifact_sessions(spark, remote):
    endpoint = f"sc://localhost:{ChannelBuilder.default_port()}" if is_jvm_spark() else remote
    second = SparkSession.builder.remote(endpoint).create()
    try:
        yield spark, second
    finally:
        second.stop()


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/client/test_artifact.py,
# ArtifactTests.test_add_file and test_add_archive; resolve files in workers without JVM helpers.
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Spark Connect AddArtifacts is not implemented",
    raises=(grpc.RpcError, UnsupportedOperationException),
    strict=True,
)
@pytest.mark.parametrize("archive", [False, True], ids=["file", "archive-alias"])
def test_artifact_files_are_available_in_workers_and_isolated_by_session(artifact_sessions, tmp_path, archive):
    name = f"artifact_{uuid.uuid4().hex}"
    worker_path = f"{name}/nested/value.txt" if archive else f"{name}.txt"
    for index, session in enumerate(artifact_sessions):
        directory = tmp_path / str(index)
        directory.mkdir()
        contents = f"session-{index}"
        if archive:
            path = directory / f"{name}.zip"
            with zipfile.ZipFile(path, "w") as output:
                output.writestr("nested/value.txt", contents)
            session.addArtifacts(f"{path}#{name}", archive=True)
        else:
            path = directory / worker_path
            path.write_text(contents)
            session.addArtifacts(str(path), file=True)

    @F.udf("string")
    def read_artifact(_):
        from pathlib import Path

        from pyspark import SparkFiles

        return Path(SparkFiles.get(worker_path)).read_text()

    # Revisit the first session after the second has used the same artifact name.
    for index in (0, 1, 0):
        session = artifact_sessions[index]
        result = session.range(0, 8, 1, 4).select(read_artifact("id").alias("value"))
        assert result.collect() == [Row(value=f"session-{index}")] * 8
