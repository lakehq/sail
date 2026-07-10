#!/usr/bin/env python3
from __future__ import annotations

# ruff: noqa: BLE001, EM102, I001, PLR2004, S101, T201, TRY003, TRY300

import contextlib
import csv
import json
import os
import struct
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path


REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15051")
KUBECTL_CONTEXT = os.environ.get("SAIL_K8S_CONTEXT")
NAMESPACE = os.environ["SAIL_K8S_NAMESPACE"]
MINIO_PORT = int(os.environ["SAIL_K8S_MINIO_PORT"])
MINIO_BUCKET = os.environ.get("SAIL_K8S_MINIO_BUCKET", "sail-artifact-store")
MINIO_USER = os.environ.get("SAIL_K8S_MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("SAIL_K8S_MINIO_PASSWORD", "password")


def kubectl_capture(args: list[str], *, check: bool = True) -> str:
    command = ["kubectl"]
    if KUBECTL_CONTEXT:
        command.extend(["--context", KUBECTL_CONTEXT])
    command.extend(args)
    print("+", " ".join(command), flush=True)
    proc = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=check,
    )
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    return proc.stdout


def create_bucket():
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{MINIO_PORT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    deadline = time.monotonic() + 60
    while True:
        try:
            client.create_bucket(Bucket=MINIO_BUCKET)
            return client
        except client.exceptions.BucketAlreadyOwnedByYou:
            return client
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def s3_keys(client) -> list[str]:
    response = client.list_objects_v2(Bucket=MINIO_BUCKET)
    return sorted(item["Key"] for item in response.get("Contents", []))


def wait_for_empty_bucket(client) -> None:
    deadline = time.monotonic() + 60
    last_keys: list[str] = []
    while time.monotonic() < deadline:
        last_keys = s3_keys(client)
        if not last_keys:
            return
        time.sleep(1)
    raise AssertionError(f"artifact objects were not cleaned up: {last_keys}")


def run_spark_probe(client) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    spark = SparkSession.builder.appName("sail-k8s-artifact-s3-validation").remote(REMOTE).getOrCreate()
    try:
        with tempfile.TemporaryDirectory(prefix="sail-k8s-artifact-") as temp:
            temp_path = Path(temp)

            numbers = [3, 5, 7, 11, 13, 17]
            binary_artifact = temp_path / "k8s_numbers.bin"
            binary_artifact.write_bytes(struct.pack("<6i", *numbers))
            spark.addArtifact(str(binary_artifact), file=True)

            json_artifact = temp_path / "k8s_weights.json"
            json_payload = {"scale": 4, "offset": 9}
            json_artifact.write_text(json.dumps(json_payload), encoding="utf-8")
            spark.addArtifact(str(json_artifact), file=True)

            py_zip = temp_path / "k8s_artifact_math.zip"
            with zipfile.ZipFile(py_zip, "w") as zf:
                zf.writestr(
                    "k8s_artifact_math.py",
                    "def score(x):\n    value = int(x)\n    return value * value + 31\n",
                )
            spark.addArtifact(str(py_zip), pyfile=True)

            archive_zip = temp_path / "k8s_calc_bundle.zip"
            with zipfile.ZipFile(archive_zip, "w") as zf:
                zf.writestr("bundle/config.json", json.dumps({"multiplier": 5}))
                zf.writestr("bundle/lookup.csv", "value\n2\n4\n8\n")
            spark.addArtifact(f"{archive_zip}#calc_bundle", archive=True)

            keys_after_add = s3_keys(client)
            print(f"S3 keys after addArtifact: {keys_after_add}")
            assert any(key.startswith("artifacts/sail-artifacts/sessions/") for key in keys_after_add), keys_after_add
            assert len(keys_after_add) >= 4, keys_after_add

            @udf(IntegerType())
            def binary_sum(x: int) -> int:
                from pyspark.core.files import SparkFiles

                with open(SparkFiles.get("k8s_numbers.bin"), "rb") as file:
                    data = file.read()
                values = struct.unpack("<6i", data)
                return sum(values) + int(x)

            @udf(IntegerType())
            def json_score(x: int) -> int:
                from pyspark.core.files import SparkFiles

                with open(SparkFiles.get("k8s_weights.json"), encoding="utf-8") as file:
                    config = json.load(file)
                return int(config["offset"]) + int(config["scale"]) * int(x)

            @udf(IntegerType())
            def pyfile_score(x: int) -> int:
                import k8s_artifact_math

                return k8s_artifact_math.score(int(x))

            @udf(IntegerType())
            def archive_score(x: int) -> int:
                from pyspark.core.files import SparkFiles

                root = SparkFiles.getRootDirectory()
                bundle = os.path.join(root, "calc_bundle", "bundle")
                with open(os.path.join(bundle, "config.json"), encoding="utf-8") as file:
                    config = json.load(file)
                with open(os.path.join(bundle, "lookup.csv"), encoding="utf-8") as file:
                    total = sum(int(row["value"]) for row in csv.DictReader(file))
                return total + int(config["multiplier"]) * int(x)

            rows = (
                spark.range(6)
                .repartition(3)
                .select(
                    "id",
                    binary_sum("id").alias("binary_sum"),
                    json_score("id").alias("json_score"),
                    pyfile_score("id").alias("pyfile_score"),
                    archive_score("id").alias("archive_score"),
                )
                .sort("id")
                .collect()
            )
            actual = [
                (
                    row.id,
                    row.binary_sum,
                    row.json_score,
                    row.pyfile_score,
                    row.archive_score,
                )
                for row in rows
            ]
            expected = [
                (
                    i,
                    sum(numbers) + i,
                    json_payload["offset"] + json_payload["scale"] * i,
                    i * i + 31,
                    14 + 5 * i,
                )
                for i in range(6)
            ]
            print(f"Computed artifact UDF rows: {actual}")
            assert actual == expected
    finally:
        spark.stop()
    wait_for_empty_bucket(client)
    print("S3 keys after Spark session stop: []")


def dump_diagnostics() -> None:
    print("\n--- Kubernetes diagnostics ---", flush=True)
    for args in [
        ["-n", NAMESPACE, "get", "pods", "-o", "wide"],
        ["-n", NAMESPACE, "get", "events", "--sort-by=.lastTimestamp"],
        ["-n", NAMESPACE, "logs", "deployment/sail-spark-server", "--tail=200"],
        ["-n", NAMESPACE, "logs", "deployment/minio", "--tail=80"],
        [
            "-n",
            NAMESPACE,
            "logs",
            "-l",
            "app.kubernetes.io/component=worker",
            "--tail=200",
            "--all-containers=true",
        ],
    ]:
        with contextlib.suppress(Exception):
            kubectl_capture(args, check=False)


def main() -> int:
    try:
        client = create_bucket()
        print(f"Created/verified bucket {MINIO_BUCKET}")
        run_spark_probe(client)
    except Exception as error:
        print(f"VALIDATION FAILED: {error}", file=sys.stderr)
        dump_diagnostics()
        return 1
    print("Kubernetes + MinIO artifact validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
