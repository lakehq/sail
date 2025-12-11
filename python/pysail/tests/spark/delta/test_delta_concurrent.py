import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def test_delta_concurrent_initial_consistent_metadata(spark, tmp_path):
    """Concurrent creation with consistent metadata should succeed; retries may land on later versions."""

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

    # Some writers may conflict; measure success count.
    success = num_writers - len(errors)
    assert success >= 1, f"All writers failed: {errors}"

    df = spark.read.format("delta").load(table_uri)
    assert df.count() == success
    assert df.select("p").distinct().count() == success

    log_dir = Path(delta_path) / "_delta_log"
    log_files = sorted(p.name for p in log_dir.glob("*.json"))
    # Version 0 must exist; later versions are allowed due to retries.
    assert "00000000000000000000.json" in log_files
    # Protocol/Metadata may repeat across later versions; ensure at least once.
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
    assert protocol_count >= 1, f"Expected protocol actions, got {protocol_count}"
    assert metadata_count >= 1, f"Expected metadata actions, got {metadata_count}"


def test_delta_concurrent_initial_metadata_mismatch_errors(spark, tmp_path):
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

    # At least one writer should fail with protocol/metadata conflict.
    assert len(errors) >= 1, f"Expected failures due to metadata mismatch, got: {errors}"
    assert any("Protocol changed" in str(e) or "Metadata changed" in str(e) for _, e in errors)

    df = spark.read.format("delta").load(table_uri)
    assert df.count() == 1


def test_delta_concurrent_blind_append_succeeds(spark, tmp_path):
    """Blind append on an existing table should allow concurrent writers without conflicts."""

    delta_path = tmp_path / "delta_concurrent_blind_append"
    table_uri = str(delta_path)

    # Create the table first with a single write (version 0).
    spark.createDataFrame([(0, "init")], ["p", "v"]).write.format("delta").mode("append").partitionBy("p").save(
        table_uri
    )

    num_writers = 5
    errors = []

    def write_partition(partition_value: int):
        df = spark.createDataFrame([(partition_value, f"value-{partition_value}")], ["p", "v"])
        df.write.format("delta").mode("append").partitionBy("p").save(table_uri)

    with ThreadPoolExecutor(max_workers=num_writers) as pool:
        futures = {pool.submit(write_partition, i + 1): i for i in range(num_writers)}
        for future, idx in futures.items():
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001
                errors.append((idx, exc))

    # All writers should succeed because blind append retries are allowed.
    assert errors == [], f"Unexpected append failures: {errors}"

    df = spark.read.format("delta").load(table_uri)
    assert df.count() == num_writers + 1
    assert df.select("p").distinct().count() == num_writers + 1

    log_dir = Path(delta_path) / "_delta_log"
    log_files = sorted(p.name for p in log_dir.glob("*.json"))
    # Expect one log per successful commit (initial create + each writer).
    assert len(log_files) == num_writers + 1
