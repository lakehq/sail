"""Tests for artifact handling via SparkSession.addArtifact."""

import hashlib
import os
import tempfile
import threading
import time
import zipfile
import zlib
from pathlib import Path

import pytest
from pyspark.sql.functions import length, udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import IntegerType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import pyspark_version

MULTIPART_ARTIFACT_PADDING_SIZE = 9 * 1024 * 1024
requires_managed_sail_server = pytest.mark.skipif(
    bool(os.environ.get("SPARK_REMOTE")),
    reason="test requires a locally managed Sail server",
)
requires_server_artifact_root_control = pytest.mark.skipif(
    bool(os.environ.get("SPARK_REMOTE")),
    reason="test requires control of the server artifact root",
)


def _artifact_store_files(path):
    return [entry for entry in path.rglob("*") if entry.is_file()]


def _make_zip(path, module_name, code):
    """Create a zip archive containing a single Python module."""
    with zipfile.ZipFile(str(path), "w") as zf:
        zf.writestr(module_name, code)


def _artifact_statuses(spark, names):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.ArtifactStatusesRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
        names=names,
    )
    return manager._stub.ArtifactStatus(request, metadata=manager._metadata)  # noqa: SLF001


def _session_artifact_dir(spark, artifact_root=None):
    manager = spark._client._artifact_manager  # noqa: SLF001
    session_id = manager._session_id  # noqa: SLF001
    user_id = manager._user_context.user_id  # noqa: SLF001
    identity = hashlib.sha256()
    identity.update(len(user_id.encode()).to_bytes(8, "big"))
    identity.update(user_id.encode())
    identity.update(len(session_id.encode()).to_bytes(8, "big"))
    identity.update(session_id.encode())
    session_hash = identity.hexdigest()
    if artifact_root is not None:
        root = Path(artifact_root)
        matches = list(root.glob(f"*/{session_hash}"))
        return matches[0] if len(matches) == 1 else root / session_hash
    root = Path(tempfile.gettempdir()) / "sail-artifacts"
    matches = list(root.glob(f"process-*/*/{session_hash}"))
    if len(matches) == 1:
        return matches[0]
    return root / session_hash


def _assert_artifact_staging_empty(spark, artifact_root=None):
    staging = _session_artifact_dir(spark, artifact_root) / ".staging"
    assert not staging.exists() or not any(staging.iterdir())


def test_add_artifact_zip_as_pyfile(spark, tmp_path):
    """Adding a zip archive as a pyfile artifact should succeed without error.

    The module must also be importable when a Python UDF is deserialized and
    executed by the Sail server.
    """
    zip_path = tmp_path / "sail_test_module.zip"
    _make_zip(zip_path, "sail_test_module.py", "VALUE = 42\n")

    spark.addArtifact(str(zip_path), pyfile=True)

    @udf(IntegerType())
    def read_artifact_value(_):
        import sail_test_module

        return sail_test_module.VALUE

    rows = spark.range(1).select(read_artifact_value("id").alias("value")).collect()
    assert rows[0].value == 42  # noqa: PLR2004


@requires_server_artifact_root_control
def test_add_artifact_rejects_jvm_jar(spark, tmp_path):
    jar_path = tmp_path / "sail_test_udf.jar"
    _make_zip(jar_path, "placeholder.class", b"not-a-class")

    with pytest.raises(Exception, match=r"JVM artifact is not supported|UNSUPPORTED"):
        spark.addArtifact(str(jar_path))

    assert not (_session_artifact_dir(spark) / "jars" / jar_path.name).exists()


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


def test_artifact_session_isolates_same_session_id_for_different_users(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.ArtifactStatusesRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=proto.UserContext(user_id="different-user"),
        names=["cache/" + ("0" * 64)],
    )
    response = manager._stub.ArtifactStatus(request, metadata=manager._metadata)  # noqa: SLF001
    assert not response.statuses[request.names[0]].exists
    assert spark.range(1).count() == 1


@pytest.mark.skipif(pyspark_version() < (4,), reason="ReleaseSession requires PySpark 4+")
def test_release_session_for_other_user_does_not_delete_original(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager  # noqa: SLF001
    other_user = proto.UserContext(user_id="different-user")
    manager._stub.ArtifactStatus(  # noqa: SLF001
        proto.ArtifactStatusesRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=other_user,
            names=[],
        ),
        metadata=manager._metadata,  # noqa: SLF001
    )
    request = proto.ReleaseSessionRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=other_user,
    )
    manager._stub.ReleaseSession(request, metadata=manager._metadata)  # noqa: SLF001

    assert spark.range(3).count() == 3  # noqa: PLR2004


@requires_server_artifact_root_control
def test_artifact_rpc_enforces_artifact_count_limit():
    from pyspark.sql.connect.proto import base_pb2 as proto

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_RPC_MAX_ARTIFACTS": "1"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        manager = session._client._artifact_manager  # noqa: SLF001
        first_data = b"VALUE = 1\n"
        second_data = b"VALUE = 2\n"
        request = proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="pyfiles/sail_quota_first.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(
                            data=first_data,
                            crc=zlib.crc32(first_data),
                        ),
                    ),
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="pyfiles/sail_quota_second.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(
                            data=second_data,
                            crc=zlib.crc32(second_data),
                        ),
                    ),
                ]
            ),
        )
        with pytest.raises(Exception, match=r"limit of 1 artifacts"):
            manager._retrieve_responses(iter([request]))  # noqa: SLF001

        assert not (_session_artifact_dir(session) / "pyfiles" / "sail_quota_first.py").exists()
        assert not (_session_artifact_dir(session) / "pyfiles" / "sail_quota_second.py").exists()


@requires_server_artifact_root_control
def test_artifact_session_enforces_distinct_artifact_limit(tmp_path):
    first = tmp_path / "sail_session_quota_first.py"
    second = tmp_path / "sail_session_quota_second.py"
    first.write_text("VALUE = 1\n", encoding="utf-8")
    second.write_text("VALUE = 2\n", encoding="utf-8")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_SESSION_MAX_ARTIFACTS": "1"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(first), pyfile=True)
        with pytest.raises(Exception, match=r"limit of 1 artifacts"):
            session.addArtifact(str(second), pyfile=True)

        assert (_session_artifact_dir(session) / "pyfiles" / first.name).exists()
        assert not (_session_artifact_dir(session) / "pyfiles" / second.name).exists()


@requires_server_artifact_root_control
def test_artifact_rpc_enforces_byte_limit():
    from pyspark.sql.connect.proto import base_pb2 as proto

    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_RPC_MAX_BYTES": "3",
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        manager = session._client._artifact_manager  # noqa: SLF001
        data = b"four"
        request = proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="files/sail_rpc_bytes.txt",
                        data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
                    )
                ]
            ),
        )
        with pytest.raises(Exception, match=r"limit of 3 bytes"):
            manager._retrieve_responses(iter([request]))  # noqa: SLF001

        assert not (_session_artifact_dir(session) / "files" / "sail_rpc_bytes.txt").exists()


@requires_server_artifact_root_control
def test_artifact_rpc_enforces_chunk_limit():
    from pyspark.sql.connect.proto import base_pb2 as proto

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_RPC_MAX_CHUNKS": "1"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        manager = session._client._artifact_manager  # noqa: SLF001
        requests = [
            proto.AddArtifactsRequest(
                session_id=manager._session_id,  # noqa: SLF001
                user_context=manager._user_context,  # noqa: SLF001
                begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                    name="files/sail_rpc_chunks.txt",
                    total_bytes=2,
                    num_chunks=2,
                    initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=b"1", crc=zlib.crc32(b"1")),
                ),
            ),
            proto.AddArtifactsRequest(
                session_id=manager._session_id,  # noqa: SLF001
                user_context=manager._user_context,  # noqa: SLF001
                chunk=proto.AddArtifactsRequest.ArtifactChunk(data=b"2", crc=zlib.crc32(b"2")),
            ),
        ]
        with pytest.raises(Exception, match=r"limit of 1 chunks"):
            manager._retrieve_responses(iter(requests))  # noqa: SLF001

        assert not (_session_artifact_dir(session) / "files" / "sail_rpc_chunks.txt").exists()
        _assert_artifact_staging_empty(session)


@requires_server_artifact_root_control
def test_artifact_session_enforces_byte_limit(tmp_path):
    first = tmp_path / "sail_session_bytes_first.py"
    second = tmp_path / "sail_session_bytes_second.py"
    first.write_bytes(b"123")
    second.write_bytes(b"4")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_SESSION_MAX_BYTES": "3"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(first), pyfile=True)
        with pytest.raises(Exception, match=r"limit of 3 bytes"):
            session.addArtifact(str(second), pyfile=True)

        assert (_session_artifact_dir(session) / "pyfiles" / first.name).exists()
        assert not (_session_artifact_dir(session) / "pyfiles" / second.name).exists()


@requires_server_artifact_root_control
def test_artifact_chunk_idle_timeout_is_reported_and_cleans_staging(tmp_path):
    from pyspark.sql.connect.proto import base_pb2 as proto

    artifact_root = tmp_path / "artifact-root"
    with (
        spark_connect_server(
            envs={
                "SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root),
                "SAIL_SPARK__ARTIFACT_CHUNK_TIMEOUT_SECS": "1",
                "SAIL_SPARK__ARTIFACT_RPC_TIMEOUT_SECS": "5",
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        manager = session._client._artifact_manager  # noqa: SLF001

        def requests():
            yield proto.AddArtifactsRequest(
                session_id=manager._session_id,  # noqa: SLF001
                user_context=manager._user_context,  # noqa: SLF001
                begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                    name="files/sail_timeout.txt",
                    total_bytes=2,
                    num_chunks=2,
                    initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=b"1", crc=zlib.crc32(b"1")),
                ),
            )
            time.sleep(2)
            yield proto.AddArtifactsRequest(
                session_id=manager._session_id,  # noqa: SLF001
                user_context=manager._user_context,  # noqa: SLF001
                chunk=proto.AddArtifactsRequest.ArtifactChunk(data=b"2", crc=zlib.crc32(b"2")),
            )

        with pytest.raises(Exception, match=r"DEADLINE_EXCEEDED|timed out"):
            manager._retrieve_responses(requests())  # noqa: SLF001
        assert not (_session_artifact_dir(session, artifact_root) / "files" / "sail_timeout.txt").exists()
        _assert_artifact_staging_empty(session, artifact_root)


def test_cache_artifact_status_tracks_cache_namespace(spark):
    payload = b"cache artifact payload"
    artifact_hash = spark._client.cache_artifact(payload)  # noqa: SLF001

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


@requires_server_artifact_root_control
def test_cache_artifact_rejects_name_that_does_not_match_content_hash(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"cached local relation payload"
    wrong_hash = "0" * 64
    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
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
        manager._retrieve_responses(iter([request]))  # noqa: SLF001

    statuses = _artifact_statuses(spark, [f"cache/{wrong_hash}"]).statuses
    assert not statuses[f"cache/{wrong_hash}"].exists
    session_dir = _session_artifact_dir(spark)
    assert not (session_dir / "cache" / wrong_hash).exists()
    _assert_artifact_staging_empty(spark)


def test_create_dataframe_uses_cached_local_relation_when_threshold_is_low(spark):
    spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")

    df = spark.createDataFrame(
        [(1, "a"), (2, "b")],
        schema="id long, value string",
    )
    plan = df._plan.to_proto(spark._client)  # noqa: SLF001
    relation = "chunked_cached_local_relation" if pyspark_version() >= (4, 1) else "cached_local_relation"
    assert plan.root.HasField(relation)

    rows = df.orderBy("id").collect()
    assert [(row.id, row.value) for row in rows] == [(1, "a"), (2, "b")]


@pytest.mark.parametrize(
    ("analyzer_name", "analyzer_call"),
    [
        ("is-local", lambda df: df.isLocal()),
        ("is-streaming", lambda df: df.isStreaming()),
    ],
)
@pytest.mark.skipif(pyspark_version() < (4, 1), reason="chunked cached relations require PySpark 4.1+")
@requires_server_artifact_root_control
def test_analyzer_resolves_missing_chunked_cached_relation_blocks(spark, analyzer_name, analyzer_call):
    spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")
    df = spark.createDataFrame([(1, analyzer_name)], schema="id long, value string")
    relation = df._plan.to_proto(spark._client).root.chunked_cached_local_relation  # noqa: SLF001
    block_hash = relation.dataHashes[0]
    (_session_artifact_dir(spark) / "cache" / block_hash).unlink()

    with pytest.raises(Exception, match=r"not found|missing|cached local relation"):
        analyzer_call(df)


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
@pytest.mark.skipif(pyspark_version() < (4, 1), reason="chunked cached relations require PySpark 4.1+")
def test_chunked_cached_local_relation_respects_server_size_limits(spark, config_key, other_config_key, error):
    spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")
    spark.conf.set(config_key, "1")
    spark.conf.set(other_config_key, "10MB")

    df = spark.createDataFrame(
        [(1, config_key)],
        schema="id long, value string",
    )
    plan = df._plan.to_proto(spark._client)  # noqa: SLF001
    assert plan.root.HasField("chunked_cached_local_relation")

    with pytest.raises(Exception, match=error):
        df.collect()


def test_add_multiple_artifacts_as_pyfiles(spark, tmp_path):
    """Multiple zip artifacts can be added and used as pyfiles."""
    paths = []
    for i in range(3):
        zip_path = tmp_path / f"sail_multi_module_{i}.zip"
        _make_zip(zip_path, f"sail_multi_module_{i}.py", f"V = {i}\n")
        paths.append(str(zip_path))
    spark.addArtifacts(*paths, pyfile=True)

    @udf(IntegerType())
    def read_multi_artifact_values(_):
        import importlib

        return sum(importlib.import_module(f"sail_multi_module_{i}").V for i in range(3))

    rows = spark.range(1).select(read_multi_artifact_values("id").alias("value")).collect()
    assert rows[0].value == 3  # noqa: PLR2004


def test_python_artifact_modules_are_isolated_between_sessions(tmp_path):
    module_name = "sail_session_isolation_module"
    first_module = tmp_path / "first" / f"{module_name}.py"
    second_module = tmp_path / "second" / f"{module_name}.py"
    first_module.parent.mkdir()
    second_module.parent.mkdir()
    first_module.write_text("VALUE = 101\n", encoding="utf-8")
    second_module.write_text("VALUE = 202\n", encoding="utf-8")

    with (
        spark_connect_server() as server,
        spark_session_factory(server.remote) as sessions,
    ):
        first_session = sessions.create()
        second_session = sessions.create()
        first_session.addArtifact(str(first_module), pyfile=True)
        second_session.addArtifact(str(second_module), pyfile=True)

        @udf(IntegerType())
        def read_module_value(_):
            import importlib

            return importlib.import_module(module_name).VALUE

        first_rows = first_session.range(1).select(read_module_value("id").alias("value")).collect()
        second_rows = second_session.range(1).select(read_module_value("id").alias("value")).collect()

        assert first_rows[0].value == 101  # noqa: PLR2004
        assert second_rows[0].value == 202  # noqa: PLR2004


def test_python_artifact_context_is_held_during_udf_execution(tmp_path):
    artifact_root = tmp_path / "artifact-root"
    file_path = tmp_path / "sail_context_file.txt"
    ready_path = tmp_path / "artifact-udf-ready"
    release_path = tmp_path / "artifact-udf-release"
    plain_entered_path = tmp_path / "plain-udf-entered"
    file_path.write_text("37", encoding="utf-8")

    with (
        spark_connect_server(envs={"SAIL_SPARK__ARTIFACT_ROOT": str(artifact_root)}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        artifact_session = sessions.create()
        plain_session = sessions.create()
        artifact_session.addArtifact(str(file_path), file=True)

        @udf(IntegerType())
        def read_file_after_barrier(_):
            import time
            from pathlib import Path

            from pyspark import SparkFiles

            Path(str(ready_path)).write_text("ready", encoding="utf-8")
            deadline = time.monotonic() + 10
            while not Path(str(release_path)).exists():
                if time.monotonic() >= deadline:
                    message = "artifact UDF release barrier timed out"
                    raise TimeoutError(message)
                time.sleep(0.01)
            with open(SparkFiles.get("sail_context_file.txt"), encoding="utf-8") as file:
                return int(file.read())

        @udf(IntegerType())
        def read_plain_value(_):
            from pathlib import Path

            Path(str(plain_entered_path)).write_text("entered", encoding="utf-8")
            return 11

        artifact_values = []
        artifact_errors = []
        plain_values = []
        plain_errors = []

        def collect_artifact_values():
            try:
                rows = artifact_session.range(1).select(read_file_after_barrier("id").alias("value")).collect()
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

        def collect_plain_values():
            try:
                rows = plain_session.range(1).select(read_plain_value("id").alias("value")).collect()
                plain_values.append(rows[0].value)
            except BaseException as error:  # noqa: BLE001
                plain_errors.append(error)

        plain_thread = threading.Thread(target=collect_plain_values)
        try:
            plain_thread.start()
            plain_thread.join(timeout=1)
            assert plain_thread.is_alive(), "plain UDF was not blocked by the active artifact context"
            assert not plain_entered_path.exists()
        finally:
            release_path.write_text("release", encoding="utf-8")
            thread.join(timeout=10)
            if plain_thread.ident is not None:
                plain_thread.join(timeout=10)

        if thread.is_alive():
            pytest.fail("artifact UDF did not finish before timeout")
        if artifact_errors:
            raise artifact_errors[0]
        if plain_thread.is_alive():
            pytest.fail("plain UDF did not finish after releasing the artifact context")
        if plain_errors:
            raise plain_errors[0]
        assert artifact_values == [37]
        assert plain_values == [11]
        assert plain_entered_path.exists()


@requires_server_artifact_root_control
def test_add_artifacts_crc_failure_is_reported_without_storing(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"crc payload"
    manager = spark._client._artifact_manager  # noqa: SLF001
    bad_crc = (zlib.crc32(data) + 1) & 0xFFFFFFFF
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
        batch=proto.AddArtifactsRequest.Batch(
            artifacts=[
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="files/sail_bad_crc.txt",
                    data=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=bad_crc),
                )
            ]
        ),
    )

    response = manager._retrieve_responses(iter([request]))  # noqa: SLF001

    assert response.artifacts[0].name == "files/sail_bad_crc.txt"
    assert not response.artifacts[0].is_crc_successful
    assert not (_session_artifact_dir(spark) / "files" / "sail_bad_crc.txt").exists()


def test_chunked_pyfile_artifact_is_importable(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"VALUE = 123\n"
    first = data[:5]
    second = data[5:]
    manager = spark._client._artifact_manager  # noqa: SLF001
    requests = [
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name="pyfiles/sail_chunked_module.py",
                total_bytes=len(data),
                num_chunks=2,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=first, crc=zlib.crc32(first)),
            ),
        ),
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            chunk=proto.AddArtifactsRequest.ArtifactChunk(data=second, crc=zlib.crc32(second)),
        ),
    ]

    response = manager._retrieve_responses(iter(requests))  # noqa: SLF001
    assert response.artifacts[0].name == "pyfiles/sail_chunked_module.py"
    assert response.artifacts[0].is_crc_successful

    @udf(IntegerType())
    def read_chunked_value(_):
        import sail_chunked_module

        return sail_chunked_module.VALUE

    rows = spark.range(1).select(read_chunked_value("id").alias("value")).collect()
    assert rows[0].value == 123  # noqa: PLR2004


@pytest.mark.parametrize("changed_identity", ["session", "user"])
@requires_server_artifact_root_control
def test_chunked_artifact_rejects_identity_change_and_cleans_staging(spark, changed_identity):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager  # noqa: SLF001
    data = b"identity"
    first = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
        begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
            name="files/sail_identity_change.txt",
            total_bytes=len(data),
            num_chunks=2,
            initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(
                data=data[:4],
                crc=zlib.crc32(data[:4]),
            ),
        ),
    )
    second = proto.AddArtifactsRequest(
        session_id="different-session" if changed_identity == "session" else manager._session_id,  # noqa: SLF001
        user_context=(
            proto.UserContext(user_id="different-user") if changed_identity == "user" else manager._user_context  # noqa: SLF001
        ),
        chunk=proto.AddArtifactsRequest.ArtifactChunk(
            data=data[4:],
            crc=zlib.crc32(data[4:]),
        ),
    )

    with pytest.raises(Exception, match=rf"{changed_identity} ID must be consistent|INVALID"):
        manager._retrieve_responses(iter([first, second]))  # noqa: SLF001
    assert not (_session_artifact_dir(spark) / "files" / "sail_identity_change.txt").exists()
    _assert_artifact_staging_empty(spark)


@requires_server_artifact_root_control
def test_chunked_artifact_crc_failure_cleans_staging(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    manager = spark._client._artifact_manager  # noqa: SLF001
    data = b"chunked crc"
    requests = [
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name="files/sail_chunked_bad_crc.txt",
                total_bytes=len(data),
                num_chunks=2,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(
                    data=data[:5],
                    crc=zlib.crc32(data[:5]),
                ),
            ),
        ),
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            chunk=proto.AddArtifactsRequest.ArtifactChunk(
                data=data[5:],
                crc=(zlib.crc32(data[5:]) + 1) & 0xFFFFFFFF,
            ),
        ),
    ]

    response = manager._retrieve_responses(iter(requests))  # noqa: SLF001
    assert not response.artifacts[0].is_crc_successful
    assert not (_session_artifact_dir(spark) / "files" / "sail_chunked_bad_crc.txt").exists()
    _assert_artifact_staging_empty(spark)


def test_add_artifacts_batch_failure_keeps_prior_pyfile(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    module_data = b"VALUE = 99\n"
    invalid_data = b"x"
    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
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
                proto.AddArtifactsRequest.SingleChunkArtifact(
                    name="pyfiles/sail_after_failure_module.py",
                    data=proto.AddArtifactsRequest.ArtifactChunk(
                        data=b"VALUE = 1\n",
                        crc=zlib.crc32(b"VALUE = 1\n"),
                    ),
                ),
            ]
        ),
    )

    with pytest.raises(Exception, match=r"relative path|\\.\\.|invalid"):
        manager._retrieve_responses(iter([request]))  # noqa: SLF001

    @udf(IntegerType())
    def read_committed_value(_):
        first = __import__("sail_partial_commit_module")
        following = __import__("sail_after_failure_module")
        return first.VALUE + following.VALUE

    rows = spark.range(1).select(read_committed_value("id").alias("value")).collect()
    assert rows[0].value == 100  # noqa: PLR2004


@pytest.mark.parametrize("name", ["jars/sail_test.jar", "classes/sail/Test.class"])
@requires_managed_sail_server
def test_add_artifacts_rejects_unsupported_jvm_artifacts(spark, name):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"jvm artifact payload"
    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
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
        manager._retrieve_responses(iter([request]))  # noqa: SLF001


@requires_server_artifact_root_control
def test_chunked_artifact_rejects_unsupported_jvm_artifact_without_committing(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"chunked jvm artifact payload"
    first = data[:5]
    second = data[5:]
    manager = spark._client._artifact_manager  # noqa: SLF001
    requests = [
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name="jars/sail_chunked.jar",
                total_bytes=len(data),
                num_chunks=2,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=first, crc=zlib.crc32(first)),
            ),
        ),
        proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
            chunk=proto.AddArtifactsRequest.ArtifactChunk(data=second, crc=zlib.crc32(second)),
        ),
    ]

    with pytest.raises(Exception, match=r"JVM artifact is not supported|UNSUPPORTED"):
        manager._retrieve_responses(iter(requests))  # noqa: SLF001

    assert not (_session_artifact_dir(spark) / "jars" / "sail_chunked.jar").exists()


@requires_managed_sail_server
def test_chunked_artifact_rejects_oversized_declaration_without_large_allocation(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"x"
    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
        begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
            name="files/sail_declared_huge.txt",
            total_bytes=10_000_000_000,
            num_chunks=1,
            initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
        ),
    )

    with pytest.raises(Exception, match=r"exceeded the limit|67108864"):
        manager._retrieve_responses(iter([request]))  # noqa: SLF001


@requires_server_artifact_root_control
def test_chunked_artifact_rejects_incomplete_within_limit(spark):
    from pyspark.sql.connect.proto import base_pb2 as proto

    data = b"x"
    manager = spark._client._artifact_manager  # noqa: SLF001
    request = proto.AddArtifactsRequest(
        session_id=manager._session_id,  # noqa: SLF001
        user_context=manager._user_context,  # noqa: SLF001
        begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
            name="files/sail_incomplete.txt",
            total_bytes=2,
            num_chunks=1,
            initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(data=data, crc=zlib.crc32(data)),
        ),
    )

    with pytest.raises(Exception, match=r"size mismatch|expected 2|incomplete"):
        manager._retrieve_responses(iter([request]))  # noqa: SLF001
    session_dir = _session_artifact_dir(spark)
    assert not (session_dir / "files" / "sail_incomplete.txt").exists()
    _assert_artifact_staging_empty(spark)


@requires_managed_sail_server
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
@requires_managed_sail_server
def test_copy_from_local_to_fs_rejects_client_side_enablement(tmp_path, config_key):
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("payload", encoding="utf-8")

    with (
        spark_connect_server() as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.conf.set(config_key, "true")
        with pytest.raises(Exception, match=r"local file(?:system| system)|not supported|UNSUPPORTED"):
            session.copyFromLocalToFs(str(source), str(destination))

    assert not destination.exists()


def test_default_inline_limit_accepts_two_megabyte_pyfile(tmp_path):
    module_path = tmp_path / "sail_two_megabyte_module.py"
    module_path.write_text(
        f"VALUE = 211\nPADDING = {('x' * (2 * 1024 * 1024))!r}\n",
        encoding="utf-8",
    )

    with (
        spark_connect_server() as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.addArtifact(str(module_path), pyfile=True)

        @udf(IntegerType())
        def read_two_megabyte_value(_):
            import sail_two_megabyte_module

            return sail_two_megabyte_module.VALUE

        rows = session.range(1).select(read_two_megabyte_value("id").alias("value")).collect()
        assert rows[0].value == 211  # noqa: PLR2004


def test_default_inline_limit_accepts_distributed_two_megabyte_local_relation():
    with (
        spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": str(1024 * 1024),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        session = sessions.create()
        session.conf.set("spark.sql.session.localRelationCacheThreshold", "1")
        rows = [
            (
                index,
                "".join(hashlib.sha256(f"{index}:{chunk}".encode()).hexdigest() for chunk in range(16)),
            )
            for index in range(2048)
        ]
        frame = session.createDataFrame(rows, schema="id long, payload string")

        result = frame.select(spark_sum(length("payload")).alias("payload_bytes")).first()
        assert result.payload_bytes == 2 * 1024 * 1024


@requires_managed_sail_server
def test_local_auto_artifact_store_spills_non_inline_pyfile_and_cleans_up(tmp_path):
    module_path = tmp_path / "sail_auto_store_module.py"
    module_path.write_text("VALUE = 613\n", encoding="utf-8")
    server_temp = tmp_path / "server-temp"
    server_temp.mkdir()

    with spark_connect_server(
        envs={
            "SAIL_SPARK__ARTIFACT_INLINE_MAX_BYTES": "0",
            "TMPDIR": str(server_temp),
        }
    ) as server:
        automatic_stores = list(server_temp.glob("sail-artifact-store-*"))
        assert len(automatic_stores) == 1
        automatic_store = automatic_stores[0]

        with spark_session_factory(server.remote) as sessions:
            session = sessions.create()
            session.addArtifact(str(module_path), pyfile=True)

            @udf(IntegerType())
            def read_auto_store_value(_):
                import sail_auto_store_module

                return sail_auto_store_module.VALUE

            rows = session.range(1).select(read_auto_store_value("id").alias("value")).collect()
            assert rows[0].value == 613  # noqa: PLR2004
            assert _artifact_store_files(automatic_store)

        assert automatic_store.exists()

    assert not automatic_store.exists()


def test_failed_batch_keeps_committed_object_store_artifacts(tmp_path):
    from pyspark.sql.connect.proto import base_pb2 as proto

    artifact_root = tmp_path / "artifact-root"
    artifact_store = tmp_path / "artifact-store"
    module_path = tmp_path / "sail_committed_store_module.py"
    module_data = b"VALUE = 314\n"
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

        manager = session._client._artifact_manager  # noqa: SLF001
        request = proto.AddArtifactsRequest(
            session_id=manager._session_id,  # noqa: SLF001
            user_context=manager._user_context,  # noqa: SLF001
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
            manager._retrieve_responses(iter([request]))  # noqa: SLF001

        assert committed_files <= set(_artifact_store_files(artifact_store))
        alias = _session_artifact_dir(session, artifact_root) / "pyfiles" / "sail_same_content_alias.py"
        assert alias.exists()

        @udf(IntegerType())
        def read_committed_value(_):
            import sail_same_content_alias

            return sail_same_content_alias.VALUE

        rows = session.range(1).select(read_committed_value("id").alias("value")).collect()
        assert rows[0].value == 314  # noqa: PLR2004


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
        f"VALUE = 509\nPADDING = {('x' * MULTIPART_ARTIFACT_PADDING_SIZE)!r}\n",
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
        assert rows[0].value == 509  # noqa: PLR2004


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

        deadline = time.monotonic() + 10
        while _artifact_store_files(artifact_store) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _artifact_store_files(artifact_store)
