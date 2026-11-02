# ruff: noqa: S608
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row

from pysail.testing.spark.utils.sql import escape_sql_string_literal


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


@pytest.mark.parametrize("snapshot_option", ["versionAsOf", "snapshotId", "snapshot-id"])
def test_iceberg_time_travel_rejects_snapshot_and_timestamp(spark, tmp_path, snapshot_option):
    table_path, table_location = _table_path_and_uri(tmp_path, "tt_conflict")

    spark.createDataFrame([Row(id=1, value="old")]).write.format("iceberg").mode("overwrite").save(table_location)
    old_snapshot_id = _current_snapshot_id(table_path)
    spark.createDataFrame([Row(id=2, value="new")]).write.format("iceberg").mode("overwrite").save(table_location)
    snapshot_times = _rewrite_snapshot_timestamps(table_path)

    # Selectors are now mutually exclusive, including retained Sail aliases;
    # silently preferring a snapshot over a conflicting timestamp hides mistakes.
    with pytest.raises(Exception, match="Conflicting Iceberg snapshot selectors"):
        (
            spark.read.format("iceberg")
            .option(snapshot_option, str(old_snapshot_id))
            .option("timestampAsOf", snapshot_times[-1])
            .load(table_location)
            .collect()
        )
    assert spark.read.format("iceberg").load(table_location).collect() == [Row(id=2, value="new")]


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


@pytest.fixture
def selector_table(spark, tmp_path):
    table = "iceberg_snapshot_selectors"
    table_path = tmp_path / "selectors"
    location = escape_sql_string_literal(str(table_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING iceberg LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
        first = _current_snapshot_id(table_path)
        spark.sql(f"INSERT INTO {table} VALUES (2, 'new')")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit AS OF VERSION {first}")
        spark.sql(f"ALTER TABLE {table} CREATE TAG frozen AS OF VERSION {first}")
        timestamp = datetime.fromisoformat(_rewrite_snapshot_timestamps(table_path)[0])
        formatted_timestamp = timestamp.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
        yield table, table_path, str(first), formatted_timestamp
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _read_with_selector(spark, table, table_path, entry, options):
    reader = spark.read.format("iceberg").options(**options)
    if entry == "table":
        return reader.table(table)
    if entry == "identifier":
        return reader.load(table)
    assert entry == "path"
    return reader.load(str(table_path))


@pytest.mark.parametrize("entry", ["path", "table", "identifier"])
@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("versionAsOf", "snapshot"),
        ("versionAsOf", "audit"),
        ("versionAsOf", "frozen"),
        ("timestampAsOf", "timestamp"),
        ("branch", "audit"),
    ],
)
def test_canonical_snapshot_selectors(spark, selector_table, entry, option, value):
    table, table_path, snapshot_id, timestamp = selector_table
    value = {"snapshot": snapshot_id, "timestamp": timestamp}.get(value, value)
    actual = _read_with_selector(spark, table, table_path, entry, {option: value})
    assert actual.schema.simpleString() == "struct<id:int,value:string>"
    assert actual.collect() == [Row(id=1, value="old")]


@pytest.mark.parametrize(
    "options",
    [
        {"snapshotId": "snapshot"},
        {"snapshot-id": "snapshot"},
        {"snapshot_id": "snapshot"},
        {"version_as_of": "audit"},
        {"timestamp_as_of": "timestamp"},
        {"ref": "audit"},
        {"use_ref": "frozen"},
        {"iceberg.ref": "frozen"},
        {"tag": "frozen"},
        {"SNAPSHOTID": "snapshot", "snapshot-id": "snapshot"},
        {"ref": "audit", "use_ref": "audit"},
    ],
)
def test_sail_snapshot_selector_aliases(spark, selector_table, options):
    table, table_path, snapshot_id, timestamp = selector_table
    values = {"snapshot": snapshot_id, "timestamp": timestamp}
    options = {key: values.get(value, value) for key, value in options.items()}
    assert _read_with_selector(spark, table, table_path, "path", options).collect() == [Row(id=1, value="old")]


@pytest.mark.parametrize("entry", ["path", "table", "identifier"])
def test_snapshot_reference_schema_evolution(spark, selector_table, entry):
    table, table_path, snapshot_id, timestamp = selector_table
    selectors = [
        ("branch", "audit", True),
        ("versionAsOf", "audit", True),
        ("versionAsOf", "frozen", False),
        ("versionAsOf", snapshot_id, False),
        ("timestampAsOf", timestamp, False),
        ("ref", "audit", True),
        ("use_ref", "frozen", False),
        ("tag", "frozen", False),
    ]
    for ddl, branch_schema, branch_rows in [
        ("DROP COLUMN value", "struct<id:int>", [Row(id=1)]),
        ("ADD COLUMN value STRING", "struct<id:int,value:string>", [Row(id=1, value=None)]),
    ]:
        spark.sql(f"ALTER TABLE {table} {ddl}")
        for option, value, is_branch in selectors:
            actual = _read_with_selector(spark, table, table_path, entry, {option: value})
            assert actual.schema.simpleString() == (branch_schema if is_branch else "struct<id:int,value:string>")
            assert actual.collect() == (branch_rows if is_branch else [Row(id=1, value="old")])


@pytest.mark.parametrize("entry", ["path", "table", "identifier"])
def test_conflicting_snapshot_selectors(spark, selector_table, entry):
    table, table_path, snapshot_id, timestamp = selector_table
    before = _read_latest_metadata(table_path)
    for options in [
        {"versionAsOf": snapshot_id, "timestampAsOf": timestamp},
        {"branch": "audit", "versionAsOf": "audit"},
        {"branch": "audit", "timestampAsOf": timestamp},
        {"branch": "audit", "tag": "frozen"},
        {"tag": "frozen", "timestampAsOf": timestamp},
        {"snapshotId": snapshot_id, "ref": "audit"},
        {"snapshotId": snapshot_id, "versionAsOf": snapshot_id},
        {"use_ref": "audit", "timestamp_as_of": timestamp},
        {"snapshotId": snapshot_id, "snapshot-id": "-10"},
        {"versionAsOf": "audit", "version_as_of": "frozen"},
        {"ref": "audit", "use_ref": "frozen"},
        {"timestampAsOf": timestamp, "timestamp_as_of": "1900-01-01T00:00:00Z"},
    ]:
        with pytest.raises(Exception, match="Conflicting Iceberg snapshot selectors"):
            _read_with_selector(spark, table, table_path, entry, options).collect()
    assert _read_latest_metadata(table_path) == before
    assert spark.table(table).orderBy("id").collect() == [Row(id=1, value="old"), Row(id=2, value="new")]


@pytest.mark.parametrize("entry", ["path", "table", "identifier"])
def test_invalid_snapshot_selectors_do_not_fall_back(spark, selector_table, entry):
    table, table_path, _snapshot_id, _timestamp = selector_table
    before = _read_latest_metadata(table_path)
    for option, value, error in [
        ("versionAsOf", "-10", "Snapshot with id -10 not found"),
        ("versionAsOf", "missing", "Unknown Iceberg ref"),
        ("versionAsOf", "9223372036854775808", "Unknown Iceberg ref"),
        ("branch", "missing", "Unknown Iceberg ref"),
        ("tag", "missing", "Unknown Iceberg ref"),
        ("ref", "missing", "Unknown Iceberg ref"),
        ("branch", "Audit", "Unknown Iceberg ref"),
        ("branch", "frozen", "not a branch"),
        ("tag", "audit", "not a tag"),
        ("tag", "main", "not a tag"),
        ("snapshotId", "not-an-id", "invalid option"),
        ("timestampAsOf", "not-a-timestamp", "Invalid timestamp"),
        ("timestampAsOf", "1900-01-01T00:00:00Z", "No Iceberg snapshot exists"),
        *[(option, "", "must not be empty") for option in ["versionAsOf", "snapshotId", "timestampAsOf", "ref", "tag"]],
        ("branch", " ", "must not be empty"),
    ]:
        with pytest.raises(Exception, match=error):
            _read_with_selector(spark, table, table_path, entry, {option: value}).collect()
    assert _read_latest_metadata(table_path) == before
    assert spark.table(table).orderBy("id").collect() == [Row(id=1, value="old"), Row(id=2, value="new")]


@pytest.mark.parametrize("entry", ["path", "table", "identifier"])
def test_snapshot_selectors_on_empty_table(spark, tmp_path, entry):
    table = "empty_snapshot_selectors"
    table_path = tmp_path / "empty"
    location = escape_sql_string_literal(str(table_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{location}'")
        for options in [{}, {"branch": "main"}]:
            actual = _read_with_selector(spark, table, table_path, entry, options)
            assert actual.schema.simpleString() == "struct<id:int>"
            assert actual.collect() == []
        for options, error in [
            ({"versionAsOf": "-10"}, "Snapshot with id -10 not found"),
            ({"versionAsOf": "missing"}, "Unknown Iceberg ref"),
            ({"branch": "missing"}, "Unknown Iceberg ref"),
            ({"tag": "main"}, "not a tag"),
            ({"timestampAsOf": "2000-01-01T00:00:00Z"}, "No Iceberg snapshot exists"),
            ({"branch": "main", "versionAsOf": "-10"}, "Conflicting Iceberg snapshot selectors"),
            ({"versionAsOf": ""}, "must not be empty"),
        ]:
            with pytest.raises(Exception, match=error):
                _read_with_selector(spark, table, table_path, entry, options).collect()
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
        for options in [{"branch": "audit"}, {"versionAsOf": "audit"}]:
            assert _read_with_selector(spark, table, table_path, entry, options).collect() == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
