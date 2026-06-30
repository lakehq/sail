"""Tests for artifact handling via SparkSession.addArtifact."""
# ruff: noqa: SLF001

import hashlib
import tempfile
import threading
import time
import zipfile
import zlib
from pathlib import Path

import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import pyspark_version

EXPECTED_ARTIFACT_VALUE = 42
EXPECTED_MULTI_ARTIFACT_VALUE = 3
EXPECTED_CHUNKED_ARTIFACT_VALUE = 123
EXPECTED_COMMITTED_OBJECT_STORE_VALUE = 314
EXPECTED_CONTEXT_FILE_VALUE = 37
EXPECTED_CONTEXT_PLAIN_VALUE = 11
EXPECTED_MULTIPART_OBJECT_STORE_VALUE = 509
MULTIPART_ARTIFACT_PADDING_SIZE = 9 * 1024 * 1024


def _artifact_store_files(path):
    return [entry for entry in path.rglob("*") if entry.is_file()]


def _make_zip(path, module_name, code):
    """Create a zip archive containing a single Python module."""
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


def _artifact_statuses(spark, names):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager
    request = proto.ArtifactStatusesRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        names=names,
    )
    return manager._stub.ArtifactStatus(request, metadata=manager._metadata)


def _session_artifact_dir(spark):
    session_id = spark._client._artifact_manager._session_id
    session_hash = hashlib.sha256(session_id.encode()).hexdigest()
    return Path(tempfile.gettempdir()) / "sail-artifacts" / session_hash


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


def test_add_artifact_pyfile_status_and_duplicate_contract(spark, tmp_path):
    module_a = tmp_path / "a" / "sail_status_module.py"
    module_a.parent.mkdir()
    module_a.write_text("VALUE = 7\n", encoding="utf-8")

    spark.addArtifact(str(module_a), pyfile=True)
    spark.addArtifact(str(module_a), pyfile=True)

    statuses = _artifact_statuses(
        spark,
        [
            "pyfiles/sail_status_module.py",
            "pyfiles/sail_missing_module.py",
        ],
    ).statuses
    assert not statuses["pyfiles/sail_status_module.py"].exists
    assert not statuses["pyfiles/sail_missing_module.py"].exists

    module_b = tmp_path / "b" / "sail_status_module.py"
    module_b.parent.mkdir()
    module_b.write_text("VALUE = 8\n", encoding="utf-8")
    with pytest.raises(Exception, match=r"already exists|different content|ARTIFACT_ALREADY_EXISTS"):
        spark.addArtifact(str(module_b), pyfile=True)


def test_cache_artifact_status_tracks_cache_namespace(spark):
    payload = b"cache artifact payload"
    artifact_hash = spark._client.cache_artifact(payload)

    statuses = _artifact_statuses(
        spark,
        [
            f"cache/{artifact_hash}",
            "cache/missing",
            "files/not-a-cache-artifact",
        ],
    ).statuses

    assert statuses[f"cache/{artifact_hash}"].exists
    assert not statuses["cache/missing"].exists
    assert not statuses["files/not-a-cache-artifact"].exists


def test_cache_artifact_rejects_name_that_does_not_match_content_hash(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"cached local relation payload"
    wrong_hash = "0" * 64
    manager = spark._client._artifact_manager
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name=f"cache/{wrong_hash}",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
                )
            ]
        ),
    )

    with pytest.raises(Exception, match=r"content hash|SHA-256|INVALID"):
        manager._retrieve_responses(iter([request]))

    statuses = _artifact_statuses(spark, [f"cache/{wrong_hash}"]).statuses
    assert not statuses[f"cache/{wrong_hash}"].exists


def test_create_dataframe_uses_chunked_cached_local_relation_when_threshold_is_low(spark):
    spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")

    df = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        schema="id long, value string",
    )
    plan = df._plan.to_proto(spark._client)
    assert plan.root.HasField("chunked_cached_local_relation")

    rows = df.orderBy("id").collect()
    assert [(row.id, row.value) for row in rows] == [(1, "a"), (2, "b")]


@pytest.mark.parametrize(
    ("config_key", "other_config_key", "error"),
    [
        (
            "spark.sql.session.localRelationChunkSizeLimit",
            "spark.sql.session.localRelationSizeLimit",
            r"cached local relation chunks exceeded the limit",
        ),
        (
            "spark.sql.session.localRelationSizeLimit",
            "spark.sql.session.localRelationChunkSizeLimit",
            r"Cached local relation size .* exceeds the limit",
        ),
    ],
)
def test_chunked_cached_local_relation_respects_server_size_limits(spark, config_key, other_config_key, error):
    spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")
    spark.conf.set(config_key, "1")
    spark.conf.set(other_config_key, "10MB")

    df = spark.createDataFrame(
        [(1, "value")],
        schema="id long, value string",
    )
    plan = df._plan.to_proto(spark._client)
    assert plan.root.HasField("chunked_cached_local_relation")

    with pytest.raises(Exception, match=error):
        df.collect()


def test_add_multiple_artifacts_as_pyfiles(spark, tmp_path):
    """Multiple zip artifacts can be added and used as pyfiles."""
    for i in range(3):
        zip_path = tmp_path / f"sail_multi_module_{i}.zip"
        _make_zip(zip_path, f"sail_multi_module_{i}.py", f"V = {i}\n")
        spark.addArtifact(str(zip_path), pyfile=True)

    @udf(IntegerType())
    def read_multi_artifact_values(_):
        import importlib

        return sum(importlib.import_module(f"sail_multi_module_{i}").V for i in range(3))

    rows = spark.range(1).select(read_multi_artifact_values("id").alias("value")).collect()
    assert rows[0].value == EXPECTED_MULTI_ARTIFACT_VALUE


def test_python_artifact_context_is_held_during_udf_execution(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    file_path = tmp_path / "sail_context_file.txt"
    ready_path = tmp_path / "artifact-udf-ready"
    file_path.write_text(str(EXPECTED_CONTEXT_FILE_VALUE), encoding="utf-8")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root)}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        artifact_session = sessions.create()
        plain_session = sessions.create()
        artifact_session.addArtifact(str(file_path), file=True)

        @udf(IntegerType())
        def read_file_after_delay(_):
            import time
            from pathlib import Path

            from pyspark import SparkFiles

            Path(str(ready_path)).write_text("ready", encoding="utf-8")
            time.sleep(0.2)
            with open(SparkFiles.get("sail_context_file.txt"), encoding="utf-8") as file:
                return int(file.read())

        @udf(IntegerType())
        def read_plain_value(_):
            return EXPECTED_CONTEXT_PLAIN_VALUE

        artifact_values = []
        artifact_errors = []

        def collect_artifact_values():
            try:
                rows = artifact_session.range(1).select(read_file_after_delay("id").alias("value")).collect()
                artifact_values.append(rows[0].value)
            except BaseException as error:  # noqa: BLE001
                artifact_errors.append(error)

        thread = threading.Thread(target=collect_artifact_values)
        thread.start()

        deadline = time.monotonic() + 10
        while not ready_path.exists() and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not ready_path.exists():
            thread.join(timeout=1)
            if artifact_errors:
                raise artifact_errors[0]
            pytest.fail("artifact UDF did not start before timeout")

        rows = plain_session.range(1).select(read_plain_value("id").alias("value")).collect()
        assert rows[0].value == EXPECTED_CONTEXT_PLAIN_VALUE

        thread.join(timeout=10)
        if thread.is_alive():
            pytest.fail("artifact UDF did not finish before timeout")
        if artifact_errors:
            raise artifact_errors[0]
        assert artifact_values == [EXPECTED_CONTEXT_FILE_VALUE]


def test_add_artifacts_crc_failure_is_reported_without_storing(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"crc payload"
    manager = spark._client._artifact_manager
    bad_crc = (zlib.crc32(data) + 1) & 0xFFFFFFFF
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="files/sail_bad_crc.txt",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=bad_crc),
                )
            ]
        ),
    )

    response = manager._retrieve_responses(iter([request]))

    assert response.artifacts[0].name == "files/sail_bad_crc.txt"
    assert not response.artifacts[0].is_crc_successful
    statuses = _artifact_statuses(spark, ["files/sail_bad_crc.txt"]).statuses
    assert not statuses["files/sail_bad_crc.txt"].exists


def test_chunked_pyfile_artifact_is_importable(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = f"VALUE = {EXPECTED_CHUNKED_ARTIFACT_VALUE}\n".encode()
    first = data[:5]
    second = data[5:]
    manager = spark._client._artifact_manager
    requests = [
        proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name="pyfiles/sail_chunked_module.py",
                total_bytes=len(data),
                num_chunks=2,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=first, crc=zlib.crc32(first)),
            ),
        ),
        proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            chunk=proto.AddArtifactsRequest.ArtifactChunk(data=second, crc=zlib.crc32(second)),
        ),
    ]

    response = manager._retrieve_responses(iter(requests))
    assert response.artifacts[0].name == "pyfiles/sail_chunked_module.py"
    assert response.artifacts[0].is_crc_successful

    @udf(IntegerType())
    def read_chunked_value(_):
        import sail_chunked_module

        return sail_chunked_module.VALUE

    rows = spark.range(1).select(read_chunked_value("id").alias("value")).collect()
    assert rows[0].value == EXPECTED_CHUNKED_ARTIFACT_VALUE


def test_add_artifacts_batch_failure_does_not_commit_prior_pyfile(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    module_data = b"VALUE = 99\n"
    invalid_data = b"x"
    manager = spark._client._artifact_manager
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="pyfiles/sail_partial_commit_module.py",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=module_data, crc=zlib.crc32(module_data)),
                ),
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="pyfiles/../sail_invalid.py",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=invalid_data, crc=zlib.crc32(invalid_data)),
                ),
            ]
        ),
    )

    with pytest.raises(Exception, match=r"relative path|\\.\\.|invalid"):
        manager._retrieve_responses(iter([request]))

    @udf(IntegerType())
    def read_uncommitted_value(_):
        module = __import__("sail_partial_commit_module")
        return module.VALUE

    with pytest.raises(Exception, match="sail_partial_commit_module"):
        spark.range(1).select(read_uncommitted_value("id").alias("value")).collect()


@pytest.mark.parametrize("name", ["jars/sail_test.jar", "classes/sail/Test.class"])
def test_add_artifacts_rejects_unsupported_jvm_artifacts(spark, name):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"jvm artifact payload"
    manager = spark._client._artifact_manager
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name=name,
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
                )
            ]
        ),
    )

    with pytest.raises(Exception, match=r"JVM artifact is not supported|UNSUPPORTED"):
        manager._retrieve_responses(iter([request]))


def test_chunked_artifact_rejects_unsupported_jvm_artifact_without_committing(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"chunked jvm artifact payload"
    first = data[:5]
    second = data[5:]
    manager = spark._client._artifact_manager
    requests = [
        proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name="jars/sail_chunked.jar",
                total_bytes=len(data),
                num_chunks=2,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=first, crc=zlib.crc32(first)),
            ),
        ),
        proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            chunk=proto.AddArtifactsRequest.ArtifactChunk(data=second, crc=zlib.crc32(second)),
        ),
    ]

    with pytest.raises(Exception, match=r"JVM artifact is not supported|UNSUPPORTED"):
        manager._retrieve_responses(iter(requests))

    assert not (_session_artifact_dir(spark) / "jars" / "sail_chunked.jar").exists()


def test_chunked_artifact_rejects_incomplete_declared_size_without_large_allocation(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"x"
    manager = spark._client._artifact_manager
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
            name="files/sail_declared_huge.txt",
            total_bytes=10_000_000_000,
            num_chunks=1,
            initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
        ),
    )

    with pytest.raises(Exception, match=r"missing data chunks|expected 1 chunks|10000000000"):
        manager._retrieve_responses(iter([request]))


def test_copy_from_local_to_fs_rejects_local_destination_by_default(tmp_path):
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("payload", encoding="utf-8")

    with (
        spark_connect_server() as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        with pytest.raises(
            Exception,
            match=r"local file(?:system| system)|copyFromLocalToFs.allowDestLocal|UNSUPPORTED",
        ):
            session.copyFromLocalToFs(str(source), str(destination))

    assert not destination.exists()


@pytest.mark.parametrize(
    "config_key",
    [
        "spark.sql.artifact.copyFromLocalToFs.allowDestLocal",
        "spark.connect.copyFromLocalToFs.allowDestLocal",
    ],
)
def test_copy_from_local_to_fs_allows_local_destination_when_enabled(tmp_path, config_key):
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("payload", encoding="utf-8")

    with (
        spark_connect_server() as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.conf.set(config_key, "true")
        session.copyFromLocalToFs(str(source), str(destination))

    assert destination.read_text(encoding="utf-8") == "payload"


def test_large_artifact_requires_object_store_when_not_inline(tmp_path):
    module_path = tmp_path / "sail_requires_store.py"
    module_path.write_text("VALUE = " + repr("x" * 64) + "\n", encoding="utf-8")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        with pytest.raises(Exception, match="artifact_store_uri"):
            session.addArtifact(str(module_path), pyfile=True)


def test_failed_batch_does_not_delete_committed_object_store_artifact(tmp_path):
    from pyspark.sql.connect.proto import base_pb2 as proto

    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_path = tmp_path / "sail_committed_store_module.py"
    module_data = f"VALUE = {EXPECTED_COMMITTED_OBJECT_STORE_VALUE}\n".encode()
    module_path.write_bytes(module_data)
    invalid_data = b"x"

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_path), pyfile=True)
        committed_files = set(_artifact_store_files(artifact_store))
        assert committed_files

        manager = session._client._artifact_manager
        request = proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="pyfiles/sail_same_content_alias.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(data=module_data, crc=zlib.crc32(module_data)),
                    ),
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="pyfiles/../sail_invalid.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(data=invalid_data, crc=zlib.crc32(invalid_data)),
                    ),
                ]
            ),
        )

        with pytest.raises(Exception, match=r"relative path|\\.\\.|invalid"):
            manager._retrieve_responses(iter([request]))

        assert committed_files <= set(_artifact_store_files(artifact_store))
        assert not (artifact_root / "pyfiles" / "sail_same_content_alias.py").exists()

        @udf(IntegerType())
        def read_committed_value(_):
            import sail_committed_store_module

            return sail_committed_store_module.VALUE

        rows = session.range(1).select(read_committed_value("id").alias("value")).collect()
        assert rows[0].value == EXPECTED_COMMITTED_OBJECT_STORE_VALUE


def test_duplicate_object_store_artifact_reuses_committed_object(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_a = tmp_path / "sail_duplicate_store_a.py"
    module_b = tmp_path / "sail_duplicate_store_b.py"
    module_data = b"VALUE = 271\n"
    module_a.write_bytes(module_data)
    module_b.write_bytes(module_data)

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_a), pyfile=True)

        files = _artifact_store_files(artifact_store)
        assert len(files) == 1
        object_path = files[0]
        object_stat = object_path.stat()

        time.sleep(0.01)
        session.addArtifact(str(module_b), pyfile=True)

        assert _artifact_store_files(artifact_store) == [object_path]
        stat_after_duplicate = object_path.stat()
        assert stat_after_duplicate.st_size == object_stat.st_size
        assert stat_after_duplicate.st_mtime_ns == object_stat.st_mtime_ns


def test_large_object_store_artifact_can_be_materialized_without_local_source(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_path = tmp_path / "sail_large_store_module.py"
    module_path.write_text(
        f"VALUE = {EXPECTED_MULTIPART_OBJECT_STORE_VALUE}\nPADDING = {('x' * MULTIPART_ARTIFACT_PADDING_SIZE)!r}\n",
        encoding="utf-8",
    )

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_path), pyfile=True)

        files = _artifact_store_files(artifact_store)
        assert len(files) == 1
        assert files[0].stat().st_size > MULTIPART_ARTIFACT_PADDING_SIZE

        local_sources = list(artifact_root.rglob(f"pyfiles/{module_path.name}"))
        assert len(local_sources) == 1
        local_source = local_sources[0]
        local_source.unlink()

        @udf(IntegerType())
        def read_large_store_value(_):
            import sail_large_store_module

            return sail_large_store_module.VALUE

        rows = session.range(1).select(read_large_store_value("id").alias("value")).collect()
        assert rows[0].value == EXPECTED_MULTIPART_OBJECT_STORE_VALUE


@pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="PySpark 3.x Connect session.stop() does not release the server session",
)
def test_session_stop_cleans_object_store_artifacts(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_path = tmp_path / "sail_cleanup_module.py"
    module_path.write_text("VALUE = " + repr("x" * 64) + "\n", encoding="utf-8")

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
                "SAIL_SPARK__ARTIFACT_STORE_URI": Path(artifact_store).as_uri(),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_path), pyfile=True)
        assert _artifact_store_files(artifact_store)

        session.stop()

        assert not _artifact_store_files(artifact_store)
