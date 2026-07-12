#!/usr/bin/env python3

import hashlib
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
import zipfile
import zlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.connect.proto import base_pb2 as proto
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15051")
RUN_ID = os.environ.get("SAIL_K8S_ARTIFACT_RUN_ID", uuid.uuid4().hex[:8])
NAMESPACE = os.environ.get("SAIL_K8S_NAMESPACE", "sail")
SERVER_DEPLOYMENT = os.environ.get("SAIL_K8S_SERVER_DEPLOYMENT", "sail-spark-server")
KUBECTL_CONTEXT = os.environ.get("SAIL_K8S_CONTEXT")

failures: list[str] = []


def log(message: str) -> None:
    print(message, flush=True)  # noqa: T201


def kubectl_exec(script: str, check: bool = True) -> subprocess.CompletedProcess[str]:  # noqa: FBT001, FBT002
    command = ["kubectl"]
    if KUBECTL_CONTEXT:
        command.extend(["--context", KUBECTL_CONTEXT])
    command.extend(
        [
            "-n",
            NAMESPACE,
            "exec",
            f"deployment/{SERVER_DEPLOYMENT}",
            "--",
            "sh",
            "-lc",
            script,
        ]
    )
    return subprocess.run(
        command,
        check=check,
        text=True,
        capture_output=True,
    )


def artifact_store_file_count() -> int:
    out = kubectl_exec("find /tmp/sail/artifact-store -type f 2>/dev/null | wc -l").stdout
    return int(out.strip() or "0")


def clear_artifact_root() -> None:
    kubectl_exec("rm -rf /tmp/sail/artifact-root/* && mkdir -p /tmp/sail/artifact-root")


def server_artifact_exists(spark: SparkSession, relative_path: str) -> bool:
    manager = spark._client._artifact_manager
    session_id = manager._session_id
    user_id = manager._user_context.user_id
    identity = hashlib.sha256()
    identity.update(len(user_id.encode()).to_bytes(8, "big"))
    identity.update(user_id.encode())
    identity.update(len(session_id.encode()).to_bytes(8, "big"))
    identity.update(session_id.encode())
    session_hash = identity.hexdigest()
    pattern = Path("*") / session_hash / relative_path
    script = (
        "find /tmp/sail/artifact-root -path "
        f"{shlex.quote(str(Path('/tmp/sail/artifact-root') / pattern))} -print -quit | grep -q ."  # noqa: S108
    )
    return kubectl_exec(script, check=False).returncode == 0


def make_zip(path: Path, entries: dict[str, str | bytes]) -> None:
    with zipfile.ZipFile(str(path), "w") as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)


def expect_raises(label: str, fn, pattern: str | None = None) -> str:
    try:
        fn()
    except Exception as exc:
        message = str(exc)
        if pattern is not None and re.search(pattern, message, flags=re.IGNORECASE) is None:
            raise AssertionError(f"{label} raised unexpected error: {message}") from exc  # noqa: EM102, TRY003
        return message
    raise AssertionError(f"{label} did not raise")  # noqa: EM102, TRY003


def new_spark(case_name: str) -> SparkSession:
    return SparkSession.builder.remote(REMOTE).appName(f"k8s-artifacts-{RUN_ID}-{case_name}").getOrCreate()


def stop_spark(spark: SparkSession | None) -> None:
    if spark is not None:
        try:
            spark.stop()
        finally:
            time.sleep(1)


def run_case(name: str, fn) -> None:
    log(f"[RUN] {name}")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        failures.append(name)
        log(f"[FAIL] {name}: {exc}")
        traceback.print_exc()
    else:
        log(f"[PASS] {name}")


def module_value_udf(module_name: str):
    @udf(IntegerType())
    def read_value(_):
        import importlib

        return int(importlib.import_module(module_name).VALUE)

    return read_value


def module_text_len_udf(module_name: str):
    @udf(IntegerType())
    def read_len(_):
        import importlib

        return len(importlib.import_module(module_name).TEXT)

    return read_len


def spark_file_text_udf(file_name: str):
    @udf(StringType())
    def read_file(_):
        from pyspark import SparkFiles

        with open(SparkFiles.get(file_name), encoding="utf-8") as handle:
            return handle.read()

    return read_file


def archive_text_udf(*parts: str):
    @udf(StringType())
    def read_archive(_):
        import os

        from pyspark import SparkFiles

        path = os.path.join(SparkFiles.getRootDirectory(), *parts)
        with open(path, encoding="utf-8") as handle:
            return handle.read()

    return read_archive


def artifact_statuses(spark: SparkSession, names: list[str]):
    manager = spark._client._artifact_manager
    request = proto.ArtifactStatusesRequest(
        session_id=manager._session_id,
        user_context=manager._user_context,
        names=names,
    )
    return manager._stub.ArtifactStatus(request, metadata=manager._metadata).statuses


def case_basic_k8s_execution() -> None:
    spark = None
    try:
        spark = new_spark("basic")
        server_pod = kubectl_exec("hostname").stdout.strip()

        @udf(StringType())
        def worker_pod_name(_):
            return os.environ["SAIL_K8S_WORKER_POD_NAME"]

        rows = spark.range(20).repartition(4).select("id", worker_pod_name("id").alias("worker_pod")).collect()
        assert sum(row.id for row in rows) == 190  # noqa: PLR2004, S101
        worker_pods = {row.worker_pod for row in rows}
        assert worker_pods  # noqa: S101
        assert server_pod not in worker_pods  # noqa: S101
    finally:
        stop_spark(spark)


def case_cache_artifacts_and_local_relations() -> None:
    spark = None
    try:
        spark = new_spark("cache")

        small_hash = spark._client.cache_artifact(b"cache-small-payload")
        large_blob = (b"cache-large-payload-" * 4096) + b"tail"
        large_hash = spark._client.cache_artifact(large_blob)
        statuses = artifact_statuses(
            spark,
            [
                f"cache/{small_hash}",
                f"cache/{large_hash}",
                "cache/missing",
                "files/not-cache",
            ],
        )
        assert statuses[f"cache/{small_hash}"].exists  # noqa: S101
        assert statuses[f"cache/{large_hash}"].exists  # noqa: S101
        assert not statuses["cache/missing"].exists  # noqa: S101
        assert not statuses["files/not-cache"].exists  # noqa: S101

        wrong_hash = "0" * 64
        data = b"cached local relation payload"
        manager = spark._client._artifact_manager
        bad_request = proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name=f"cache/{wrong_hash}",
                        data=proto.AddArtifactsRequest.ArtifactChunk(
                            data=data,
                            crc=zlib.crc32(data),
                        ),
                    )
                ]
            ),
        )
        expect_raises(
            "cache artifact hash mismatch",
            lambda: manager._retrieve_responses(iter([bad_request])),
            r"content hash|SHA-256|INVALID",
        )
        assert not artifact_statuses(spark, [f"cache/{wrong_hash}"])[f"cache/{wrong_hash}"].exists  # noqa: S101
        assert not server_artifact_exists(spark, f"cache/{wrong_hash}")  # noqa: S101

        spark.conf.set("spark.sql.session.localRelationCacheThreshold", str(1 << 30))
        inline_df = spark.createDataFrame([(1, "a"), (2, "b")], schema="id long, value string")
        inline_plan = inline_df._plan.to_proto(spark._client)
        assert inline_plan.root.HasField("local_relation")  # noqa: S101
        assert [(row.id, row.value) for row in inline_df.orderBy("id").collect()] == [  # noqa: S101
            (1, "a"),
            (2, "b"),
        ]

        spark.conf.set("spark.sql.session.localRelationCacheThreshold", "1")
        cached_df = spark.createDataFrame(
            [(1, "a", True, 1.25), (2, "b", False, 2.5)],
            schema="id long, value string, flag boolean, score double",
        )
        cached_plan = cached_df._plan.to_proto(spark._client)
        assert cached_plan.root.HasField("chunked_cached_local_relation")  # noqa: S101
        assert len(cached_plan.root.chunked_cached_local_relation.dataHashes) >= 1  # noqa: S101
        rows = cached_df.orderBy("id").collect()
        assert [(r.id, r.value, r.flag, r.score) for r in rows] == [  # noqa: S101
            (1, "a", True, 1.25),
            (2, "b", False, 2.5),
        ]

        spark.conf.set("spark.sql.session.localRelationChunkSizeRows", "2")
        spark.conf.set("spark.sql.session.localRelationChunkSizeBytes", "1024")
        spark.conf.set("spark.sql.session.localRelationBatchOfChunksSizeBytes", "2048")
        multi_chunk_df = spark.createDataFrame(
            [(i, f"{i}-" + ("x" * 2048)) for i in range(9)],
            schema="id long, payload string",
        )
        multi_chunk_plan = multi_chunk_df._plan.to_proto(spark._client)
        relation = multi_chunk_plan.root.chunked_cached_local_relation
        assert multi_chunk_plan.root.HasField("chunked_cached_local_relation")  # noqa: S101
        assert len(relation.dataHashes) >= 2, list(relation.dataHashes)  # noqa: PLR2004, S101
        assert [row.id for row in multi_chunk_df.orderBy("id").collect()] == list(range(9))  # noqa: S101
    finally:
        stop_spark(spark)


def case_pyfile_file_archive_and_jar_artifacts() -> None:
    spark = None
    before_store_count = artifact_store_file_count()
    try:
        spark = new_spark("artifacts")
        with tempfile.TemporaryDirectory(prefix=f"sail-k8s-artifacts-{RUN_ID}-") as raw_tmp:
            tmp = Path(raw_tmp)

            small_module = f"k8s_small_module_{RUN_ID}"
            small_py = tmp / f"{small_module}.py"
            small_py.write_text("VALUE = 7\n", encoding="utf-8")
            spark.addArtifact(str(small_py), pyfile=True)
            rows = spark.range(8).repartition(4).select(module_value_udf(small_module)("id").alias("v")).collect()
            assert {row.v for row in rows} == {7}  # noqa: S101

            medium_module = f"k8s_medium_module_{RUN_ID}"
            medium_py = tmp / f"{medium_module}.py"
            medium_py.write_text("TEXT = " + repr("m" * 256) + "\nVALUE = 8\n", encoding="utf-8")
            spark.addArtifact(str(medium_py), pyfile=True)
            clear_artifact_root()
            rows = spark.range(8).repartition(4).select(module_text_len_udf(medium_module)("id").alias("v")).collect()
            assert {row.v for row in rows} == {256}  # noqa: S101

            chunk_module = f"k8s_chunk_module_{RUN_ID}"
            chunk_py = tmp / f"{chunk_module}.py"
            chunk_py.write_text("TEXT = " + repr("c" * 40000) + "\nVALUE = 9\n", encoding="utf-8")
            spark.addArtifact(str(chunk_py), pyfile=True)
            clear_artifact_root()
            rows = spark.range(8).repartition(4).select(module_text_len_udf(chunk_module)("id").alias("v")).collect()
            assert {row.v for row in rows} == {40000}  # noqa: S101

            zip_module = f"k8s_zip_module_{RUN_ID}"
            zip_path = tmp / f"{zip_module}.zip"
            make_zip(zip_path, {f"{zip_module}.py": "VALUE = 10\n"})
            spark.addArtifact(str(zip_path), pyfile=True)
            clear_artifact_root()
            rows = spark.range(4).repartition(2).select(module_value_udf(zip_module)("id").alias("v")).collect()
            assert {row.v for row in rows} == {10}  # noqa: S101

            total_expected = 0
            for i in range(3):
                module = f"k8s_multi_module_{RUN_ID}_{i}"
                total_expected += i + 1
                path = tmp / f"{module}.zip"
                make_zip(path, {f"{module}.py": f"VALUE = {i + 1}\n"})
                spark.addArtifact(str(path), pyfile=True)

            @udf(IntegerType())
            def read_multi(_):
                import importlib

                return sum(int(importlib.import_module(f"k8s_multi_module_{RUN_ID}_{i}").VALUE) for i in range(3))

            rows = spark.range(4).repartition(2).select(read_multi("id").alias("v")).collect()
            assert {row.v for row in rows} == {total_expected}  # noqa: S101

            file_path = tmp / f"k8s_file_{RUN_ID}.txt"
            file_payload = "file artifact payload " * 8
            file_path.write_text(file_payload, encoding="utf-8")
            spark.addArtifact(str(file_path), file=True)
            clear_artifact_root()
            rows = spark.range(8).repartition(4).select(spark_file_text_udf(file_path.name)("id").alias("v")).collect()
            assert {row.v for row in rows} == {file_payload}  # noqa: S101

            large_file = tmp / f"k8s_large_file_{RUN_ID}.txt"
            large_payload = "L" * 70000
            large_file.write_text(large_payload, encoding="utf-8")
            spark.addArtifact(str(large_file), file=True)
            clear_artifact_root()
            rows = spark.range(8).repartition(4).select(spark_file_text_udf(large_file.name)("id").alias("v")).collect()
            assert {len(row.v) for row in rows} == {70000}  # noqa: S101

            archive_dir = tmp / f"k8s_archive_payload_{RUN_ID}"
            archive_dir.mkdir()
            (archive_dir / "payload.txt").write_text("archive with fragment", encoding="utf-8")
            archive_path = shutil.make_archive(str(tmp / f"k8s_archive_{RUN_ID}"), "zip", tmp, archive_dir.name)
            spark.addArtifact(f"{archive_path}#k8s_fragment_{RUN_ID}", archive=True)
            clear_artifact_root()
            rows = (
                spark.range(4)
                .repartition(2)
                .select(
                    archive_text_udf(
                        f"k8s_fragment_{RUN_ID}",
                        archive_dir.name,
                        "payload.txt",
                    )("id").alias("v")
                )
                .collect()
            )
            assert {row.v for row in rows} == {"archive with fragment"}  # noqa: S101

            default_dir = tmp / f"k8s_default_archive_payload_{RUN_ID}"
            default_dir.mkdir()
            (default_dir / "payload.txt").write_text("archive default dir", encoding="utf-8")
            default_archive = shutil.make_archive(
                str(tmp / f"k8s_default_archive_{RUN_ID}"), "zip", tmp, default_dir.name
            )
            spark.addArtifact(default_archive, archive=True)
            clear_artifact_root()
            rows = (
                spark.range(4)
                .repartition(2)
                .select(
                    archive_text_udf(
                        Path(default_archive).name,
                        default_dir.name,
                        "payload.txt",
                    )("id").alias("v")
                )
                .collect()
            )
            assert {row.v for row in rows} == {"archive default dir"}  # noqa: S101

            unsafe_archive = tmp / f"k8s_unsafe_archive_{RUN_ID}.zip"
            make_zip(unsafe_archive, {"../escape.txt": "unsafe"})
            spark.addArtifact(f"{unsafe_archive}#k8s_unsafe_{RUN_ID}", archive=True)
            expect_raises(
                "unsafe archive member",
                lambda: (
                    spark.range(1)
                    .select(archive_text_udf(f"k8s_unsafe_{RUN_ID}", "escape.txt")("id").alias("v"))
                    .collect()
                ),
                r"unsafe archive member path|safe relative path",
            )

            jar_path = tmp / f"k8s_dummy_{RUN_ID}.jar"
            make_zip(jar_path, {"META-INF/MANIFEST.MF": "Manifest-Version: 1.0\n"})
            expect_raises(
                "unsupported JVM jar artifact",
                lambda: spark.addArtifact(str(jar_path)),
                r"JVM artifact is not supported|UNSUPPORTED",
            )

        after_store_count = artifact_store_file_count()
        assert after_store_count > before_store_count, (before_store_count, after_store_count)  # noqa: S101
    finally:
        stop_spark(spark)
        deadline = time.time() + 20
        while time.time() < deadline:
            if artifact_store_file_count() <= before_store_count:
                break
            time.sleep(1)
        assert artifact_store_file_count() <= before_store_count  # noqa: S101


def case_protocol_errors_and_transactionality() -> None:
    spark = None
    try:
        spark = new_spark("protocol")
        manager = spark._client._artifact_manager

        bad_crc_data = b"crc payload"
        bad_crc = (zlib.crc32(bad_crc_data) + 1) & 0xFFFFFFFF
        crc_request = proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name=f"files/k8s_bad_crc_{RUN_ID}.txt",
                        data=proto.AddArtifactsRequest.ArtifactChunk(data=bad_crc_data, crc=bad_crc),
                    )
                ]
            ),
        )
        crc_response = manager._retrieve_responses(iter([crc_request]))
        assert crc_response.artifacts[0].name == f"files/k8s_bad_crc_{RUN_ID}.txt"  # noqa: S101
        assert not crc_response.artifacts[0].is_crc_successful  # noqa: S101
        assert not server_artifact_exists(spark, f"files/k8s_bad_crc_{RUN_ID}.txt")  # noqa: S101

        direct_module = f"k8s_direct_chunk_module_{RUN_ID}"
        direct_data = f"VALUE = 321\nTEXT = {repr('d' * 128)}\n".encode()  # noqa: RUF010
        first = direct_data[:7]
        second = direct_data[7:]
        direct_requests = [
            proto.AddArtifactsRequest(
                session_id=manager._session_id,
                user_context=manager._user_context,
                begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                    name=f"pyfiles/{direct_module}.py",
                    total_bytes=len(direct_data),
                    num_chunks=2,
                    initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(
                        data=first,
                        crc=zlib.crc32(first),
                    ),
                ),
            ),
            proto.AddArtifactsRequest(
                session_id=manager._session_id,
                user_context=manager._user_context,
                chunk=proto.AddArtifactsRequest.ArtifactChunk(
                    data=second,
                    crc=zlib.crc32(second),
                ),
            ),
        ]
        direct_response = manager._retrieve_responses(iter(direct_requests))
        assert direct_response.artifacts[0].is_crc_successful  # noqa: S101
        rows = spark.range(4).repartition(2).select(module_value_udf(direct_module)("id").alias("v")).collect()
        assert {row.v for row in rows} == {321}  # noqa: S101

        incomplete = b"x"
        incomplete_request = proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            begin_chunk=proto.AddArtifactsRequest.BeginChunkedArtifact(
                name=f"files/k8s_incomplete_{RUN_ID}.txt",
                total_bytes=10_000_000_000,
                num_chunks=1,
                initial_chunk=proto.AddArtifactsRequest.ArtifactChunk(
                    data=incomplete,
                    crc=zlib.crc32(incomplete),
                ),
            ),
        )
        expect_raises(
            "incomplete declared chunked artifact",
            lambda: manager._retrieve_responses(iter([incomplete_request])),
            r"exceeded the limit|67108864",
        )

        module_name = f"k8s_uncommitted_module_{RUN_ID}"
        module_data = b"VALUE = 99\n"
        invalid_data = b"x"
        batch_request = proto.AddArtifactsRequest(
            session_id=manager._session_id,
            user_context=manager._user_context,
            batch=proto.AddArtifactsRequest.Batch(
                artifacts=[
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name=f"pyfiles/{module_name}.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(
                            data=module_data,
                            crc=zlib.crc32(module_data),
                        ),
                    ),
                    proto.AddArtifactsRequest.SingleChunkArtifact(
                        name="pyfiles/../k8s_invalid.py",
                        data=proto.AddArtifactsRequest.ArtifactChunk(
                            data=invalid_data,
                            crc=zlib.crc32(invalid_data),
                        ),
                    ),
                ]
            ),
        )
        expect_raises(
            "batch path traversal failure",
            lambda: manager._retrieve_responses(iter([batch_request])),
            r"relative path|\\.\\.|invalid",
        )
        rows = spark.range(1).select(module_value_udf(module_name)("id").alias("v")).collect()
        assert rows[0].v == 99  # noqa: PLR2004, S101
    finally:
        stop_spark(spark)


def case_copy_from_local_to_fs() -> None:
    spark = None
    try:
        spark = new_spark("copy")
        with tempfile.TemporaryDirectory(prefix=f"sail-k8s-copy-{RUN_ID}-") as raw_tmp:
            tmp = Path(raw_tmp)
            source = tmp / f"k8s_copy_source_{RUN_ID}.txt"
            payload = f"copy payload {RUN_ID}"
            source.write_text(payload, encoding="utf-8")
            disabled_dest = f"/tmp/sail/k8s_copy_disabled_{RUN_ID}.txt"  # noqa: S108
            expect_raises(
                "copyFromLocalToFs local destination disabled",
                lambda: spark.copyFromLocalToFs(str(source), disabled_dest),
                r"local file|copyFromLocalToFs|disabled|UNSUPPORTED",
            )

            spark.conf.set("spark.sql.artifact.copyFromLocalToFs.allowDestLocal", "true")
            dest = f"/tmp/sail/k8s_copy_enabled_{RUN_ID}.txt"  # noqa: S108
            expect_raises(
                "copyFromLocalToFs client-side enablement",
                lambda: spark.copyFromLocalToFs(str(source), dest),
                r"local file|copyFromLocalToFs|not supported|UNSUPPORTED",
            )
            assert kubectl_exec(f"test ! -e {dest}", check=False).returncode == 0  # noqa: S101
    finally:
        stop_spark(spark)


def main() -> int:
    log(f"remote={REMOTE} run_id={RUN_ID}")
    run_case("basic k8s distributed execution", case_basic_k8s_execution)
    run_case("cache artifacts and local relation cache plans", case_cache_artifacts_and_local_relations)
    run_case("pyfile/file/archive artifacts and JVM artifact rejection", case_pyfile_file_archive_and_jar_artifacts)
    run_case("AddArtifacts protocol errors and partial commit behavior", case_protocol_errors_and_transactionality)
    run_case("copyFromLocalToFs forward_to_fs artifact path", case_copy_from_local_to_fs)

    if failures:
        log("FAILED cases:")
        for failure in failures:
            log(f"  - {failure}")
        return 1
    log("all k8s artifact matrix cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
