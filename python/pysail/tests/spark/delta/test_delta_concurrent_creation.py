import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def test_delta_concurrent_initial_creation_consistent_metadata(spark, tmp_path):
    """Concurrent creation with consistent metadata should succeed without version-0 conflicts."""

    delta_path = tmp_path / "delta_concurrent_initial"
    table_uri = str(delta_path)
    num_writers = 5
    errors = []

    def write_partition(partition_value: int):
        df = spark.createDataFrame([(partition_value, f"value-{partition_value}")], ["p", "v"])
        # Writing in append mode should create the table when it does not exist yet.
        df.write.format("delta").mode("append").partitionBy("p").save(table_uri)

    with ThreadPoolExecutor(max_workers=num_writers) as pool:
        futures = {pool.submit(write_partition, i): i for i in range(num_writers)}
        for future, idx in futures.items():
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                errors.append((idx, exc))

    if errors:
        formatted = "\n".join(f"writer {idx} failed: {err}" for idx, err in errors)
        pytest.fail(f"Concurrent writers failed during initial creation:\n{formatted}")

    df = spark.read.format("delta").load(table_uri)
    assert df.count() == num_writers
    assert df.select("p").distinct().count() == num_writers

    log_dir = Path(delta_path) / "_delta_log"
    log_files = sorted(p.name for p in log_dir.glob("*.json"))
    # Ensure version 0 log exists.
    assert log_files, "Expected at least one log file"
    assert log_files[0] == "00000000000000000000.json"
    # All versions should be contiguous starting at 0.
    assert log_files == [f"{i:020}.json" for i in range(len(log_files))], f"Non-contiguous log files: {log_files}"
    # Protocol/Metadata should appear only once (in version 0) across all commits.
    protocol_count = 0
    metadata_count = 0
    for log_file in log_files:
        with open(log_dir / log_file, encoding="utf-8") as fh:
            for line in fh:
                action = json.loads(line)
                if "protocol" in action:
                    protocol_count += 1
                if "metaData" in action:
                    metadata_count += 1
    assert protocol_count == 1, f"Expected exactly one protocol action, got {protocol_count}"
    assert metadata_count == 1, f"Expected exactly one metadata action, got {metadata_count}"


def test_delta_concurrent_initial_creation_metadata_mismatch_errors(spark, tmp_path):
    """Ensure mismatched metadata during concurrent create surfaces as protocol/metadata conflict."""

    delta_path = tmp_path / "delta_concurrent_initial_mismatch"
    table_uri = str(delta_path)
    errors = []

    def write(*, use_extra_column: bool):
        if use_extra_column:
            df = spark.createDataFrame([(1, "a", "x")], ["p", "v", "w"])
        else:
            df = spark.createDataFrame([(2, "b")], ["p", "v"])
        df.write.format("delta").mode("append").partitionBy("p").save(table_uri)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(write, use_extra_column=flag): flag for flag in (False, True)}
        for future, flag in futures.items():
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                errors.append((flag, exc))

    # One writer should succeed, the mismatched writer should fail with protocol/metadata conflict.
    assert len(errors) == 1, f"Expected one failure due to metadata mismatch, got: {errors}"
    msg = str(errors[0][1])
    assert "Protocol changed" in msg or "Metadata changed" in msg

    df = spark.read.format("delta").load(table_uri)
    assert df.count() == 1
