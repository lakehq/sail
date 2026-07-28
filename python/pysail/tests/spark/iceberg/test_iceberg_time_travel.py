import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row


def _table_path_and_uri(tmp_path: Path, name: str) -> tuple[Path, str]:
    table_path = tmp_path / name
    return table_path, table_path.absolute().as_uri()


def _latest_metadata_path(table_path: Path) -> Path:
    metadata_files = sorted((table_path / "metadata").glob("*.metadata.json"))
    assert metadata_files
    return metadata_files[-1]


def _read_latest_metadata(table_path: Path) -> dict:
    return json.loads(_latest_metadata_path(table_path).read_text(encoding="utf-8"))


def _write_latest_metadata(table_path: Path, metadata: dict) -> None:
    _latest_metadata_path(table_path).write_text(json.dumps(metadata, separators=(",", ":")), encoding="utf-8")


def _ordered_snapshots(metadata: dict) -> list[dict]:
    snapshots_by_id = {
        snapshot["snapshot-id"]: snapshot for snapshot in metadata.get("snapshots", []) if "snapshot-id" in snapshot
    }
    ordered = []
    for entry in metadata.get("snapshot-log", []):
        snapshot = snapshots_by_id.get(entry.get("snapshot-id"))
        if snapshot is not None:
            ordered.append(snapshot)
    return ordered or sorted(snapshots_by_id.values(), key=lambda snapshot: snapshot["timestamp-ms"])


def _current_snapshot_id(table_path: Path) -> int:
    metadata = _read_latest_metadata(table_path)
    snapshot_id = metadata.get("current-snapshot-id")
    assert snapshot_id is not None
    return snapshot_id


def _snapshot_ids(table_path: Path) -> list[int]:
    metadata = _read_latest_metadata(table_path)
    return [snapshot["snapshot-id"] for snapshot in _ordered_snapshots(metadata)]


def _rewrite_snapshot_timestamps(table_path: Path) -> list[str]:
    metadata = _read_latest_metadata(table_path)
    ordered = _ordered_snapshots(metadata)
    assert ordered

    start = datetime.now(timezone.utc) - timedelta(seconds=10)
    timestamp_by_id = {
        snapshot["snapshot-id"]: int((start + timedelta(seconds=index)).timestamp() * 1000)
        for index, snapshot in enumerate(ordered)
    }

    for snapshot in metadata.get("snapshots", []):
        snapshot_id = snapshot.get("snapshot-id")
        if snapshot_id in timestamp_by_id:
            snapshot["timestamp-ms"] = timestamp_by_id[snapshot_id]

    for entry in metadata.get("snapshot-log", []):
        snapshot_id = entry.get("snapshot-id")
        if snapshot_id in timestamp_by_id:
            entry["timestamp-ms"] = timestamp_by_id[snapshot_id]

    metadata["last-updated-ms"] = max(timestamp_by_id.values())
    _write_latest_metadata(table_path, metadata)

    return [
        datetime.fromtimestamp(timestamp_by_id[snapshot["snapshot-id"]] / 1000, timezone.utc).isoformat()
        for snapshot in ordered
    ]


def test_iceberg_time_travel_by_snapshot_id(spark, tmp_path):
    table_path, table_location = _table_path_and_uri(tmp_path, "tt_by_snapshot")

    spark.createDataFrame([Row(id=1, value="v0")]).write.format("iceberg").mode("overwrite").save(table_location)
    snapshot_id = _current_snapshot_id(table_path)

    spark.createDataFrame([Row(id=2, value="v1")]).write.format("iceberg").mode("append").save(table_location)

    latest = spark.read.format("iceberg").load(table_location).sort("id").toPandas()
    assert_frame_equal(latest, pd.DataFrame({"id": [1, 2], "value": ["v0", "v1"]}).astype(latest.dtypes))

    actual = spark.read.format("iceberg").option("snapshotId", str(snapshot_id)).load(table_location).sort("id")
    actual_pdf = actual.toPandas()
    assert_frame_equal(actual_pdf, pd.DataFrame({"id": [1], "value": ["v0"]}).astype(actual_pdf.dtypes))


def test_iceberg_time_travel_by_timestamp(spark, tmp_path):
    table_path, table_location = _table_path_and_uri(tmp_path, "tt_by_timestamp")

    spark.createDataFrame([Row(id=1, value="v0")]).write.format("iceberg").mode("overwrite").save(table_location)
    spark.createDataFrame([Row(id=2, value="v1")]).write.format("iceberg").mode("append").save(table_location)
    spark.createDataFrame([Row(id=3, value="v2")]).write.format("iceberg").mode("append").save(table_location)
    snapshot_times = _rewrite_snapshot_timestamps(table_path)

    latest = spark.read.format("iceberg").load(table_location).sort("id").collect()
    assert latest == [Row(id=1, value="v0"), Row(id=2, value="v1"), Row(id=3, value="v2")]

    rows = spark.read.format("iceberg").option("timestampAsOf", snapshot_times[0]).load(table_location).collect()
    assert rows == [Row(id=1, value="v0")]

    rows = (
        spark.read.format("iceberg")
        .option("timestampAsOf", snapshot_times[1])
        .load(table_location)
        .sort("id")
        .collect()
    )
    assert rows == [Row(id=1, value="v0"), Row(id=2, value="v1")]

    rows = (
        spark.read.format("iceberg")
        .option("timestampAsOf", snapshot_times[2])
        .load(table_location)
        .sort("id")
        .collect()
    )
    assert rows == [Row(id=1, value="v0"), Row(id=2, value="v1"), Row(id=3, value="v2")]


def test_iceberg_time_travel_precedence_snapshot_over_timestamp(spark, tmp_path):
    table_path, table_location = _table_path_and_uri(tmp_path, "tt_precedence")

    spark.createDataFrame([Row(id=1, value="old")]).write.format("iceberg").mode("overwrite").save(table_location)
    old_snapshot_id = _current_snapshot_id(table_path)
    spark.createDataFrame([Row(id=2, value="new")]).write.format("iceberg").mode("overwrite").save(table_location)
    snapshot_times = _rewrite_snapshot_timestamps(table_path)

    rows = (
        spark.read.format("iceberg")
        .option("snapshotId", str(old_snapshot_id))
        .option("timestampAsOf", snapshot_times[-1])
        .load(table_location)
        .collect()
    )
    assert rows == [Row(id=1, value="old")]


def test_iceberg_time_travel_by_ref_main(spark, tmp_path):
    _table_path, table_location = _table_path_and_uri(tmp_path, "tt_ref_main")

    spark.createDataFrame([Row(id=1, value="x")]).write.format("iceberg").mode("overwrite").save(table_location)

    rows = spark.read.format("iceberg").option("ref", "main").load(table_location).collect()
    assert rows == [Row(id=1, value="x")]


def test_iceberg_time_travel_across_merge_schema(spark, tmp_path):
    table_path, table_location = _table_path_and_uri(tmp_path, "tt_schema_evolution")

    spark.createDataFrame([Row(id=1, value="base")]).write.format("iceberg").mode("overwrite").save(table_location)
    snapshot_id = _snapshot_ids(table_path)[0]

    spark.createDataFrame([Row(id=2, value="new", extra=10)]).write.format("iceberg").mode("append").option(
        "mergeSchema",
        "true",
    ).save(table_location)

    latest_df = spark.read.format("iceberg").load(table_location).orderBy("id")
    assert latest_df.columns == ["id", "value", "extra"]
    latest_rows = latest_df.collect()
    assert latest_rows[0].id == 1
    assert latest_rows[0].extra is None
    assert latest_rows[1].id == 2  # noqa: PLR2004
    assert latest_rows[1].extra == 10  # noqa: PLR2004

    tt_df = spark.read.format("iceberg").option("snapshotId", str(snapshot_id)).load(table_location).orderBy("id")
    tt_rows = tt_df.collect()
    assert len(tt_rows) == 1
    assert tt_rows[0].id == 1
    assert tt_rows[0].value == "base"
    assert "extra" not in tt_df.columns
