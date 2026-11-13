from __future__ import annotations

import json
import os
import shutil
import time
from collections import Counter
from urllib.parse import quote, unquote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.tests._dev.spark.delta.utils import _active_files, _commits, write_delta_with_row_groups

"""Stored Delta URIs identify logical files; decoded paths identify physical objects."""

# ruff: noqa: S608, PLR2004


def _identity(action):
    dv = action.get("deletionVector")
    return action["path"], None if dv is None else (dv["storageType"], dv["pathOrInlineDv"], dv.get("offset"))


def _rows(session, path, **options):
    rows = [
        Counter(
            tuple(row)
            for row in session.read.format("delta")
            .options(**options)
            .option("metadataAsDataRead", metadata)
            .load(str(path))
            .collect()
        )
        for metadata in ("false", "true")
    ]
    assert rows[0] == rows[1]
    return rows[0]


def _assert_removes_match(actions, original):
    removed = [action["remove"] for action in actions if "remove" in action]
    assert Counter(map(_identity, removed)) == Counter(map(_identity, original))
    by_identity = {_identity(add): add for add in original}
    for remove in removed:
        assert remove.get("deletionVector") == by_identity[_identity(remove)].get("deletionVector")


@pytest.mark.integration
@pytest.mark.parametrize("partition", ["日本語", "café", "a%2Fb % #?"], ids=["unicode", "accent", "escaped"])
@pytest.mark.parametrize("dv", [False, True], ids=["no-dv", "dv"])
def test_optimize_preserves_spark_uri_and_dv_identity(spark, delta_jvm_spark, tmp_path, partition, dv):
    # Delta v4.0.1 AddFile.removeWithTimestamp preserves path and DV verbatim;
    # DeltaLogSuite's "do not relativize paths in RemoveFiles" asserts URI retention.
    jvm = delta_jvm_spark
    reference = tmp_path / "reference"
    path = tmp_path / "sail"
    for start in (0, 4):
        (
            jvm.createDataFrame([(i, partition) for i in range(start, start + 4)], "id LONG, p STRING")
            .coalesce(1)
            .write.format("delta")
            .partitionBy("p")
            .mode("append")
            .option("delta.enableDeletionVectors", str(dv).lower())
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.checkpointInterval", "1")
            .save(str(reference))
        )
    if dv:
        jvm.sql(f"DELETE FROM delta.`{reference}` WHERE id % 4 = 0").collect()
    shutil.copytree(reference, path)
    original = [row.asDict(recursive=True) for row in _active_files(path)]
    assert len(original) == 2
    assert all(bool(add.get("deletionVector")) == dv for add in original)
    if partition in ("日本語", "café"):
        assert all(partition in add["path"] for add in original)
    assert all((path / unquote(add["path"])).is_file() for add in original)
    expected = Counter((i, partition) for i in range(8) if not dv or i % 4 != 0)
    assert _rows(spark, path) == _rows(jvm, path) == expected
    physical_files = [file for file in path.rglob("*") if file.is_file() and "_delta_log" not in file.parts]
    old = time.time() - 8 * 24 * 3600
    for file in physical_files:
        os.utime(file, (old, old))
    assert spark.sql(f"VACUUM delta.`{path}` DRY RUN").collect() == []
    before = max(_commits(path))

    actual = spark.sql(f"OPTIMIZE delta.`{path}`").first().metrics
    oracle = jvm.sql(f"OPTIMIZE delta.`{reference}`").first().metrics
    assert actual.numFilesRemoved == oracle.numFilesRemoved == len(original)
    actions = _commits(path)[before + 1]
    _assert_removes_match(actions, original)
    assert all(not action[key]["dataChange"] for action in actions for key in ("add", "remove") if key in action)
    assert _rows(spark, path) == _rows(jvm, path) == _rows(jvm, reference) == expected
    assert _rows(spark, path, versionAsOf=before) == expected
    for session in (spark, jvm):
        assert (
            session.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", before + 1)
            .option("endingVersion", before + 1)
            .load(str(path))
            .collect()
            == []
        )

    checkpoint = path / "_delta_log" / f"{before + 1:020}.checkpoint.parquet"
    checkpoint_rows = pq.ParquetFile(checkpoint).read().to_pylist()
    checkpoint_removes = [row["remove"] for row in checkpoint_rows if row.get("remove")]
    checkpoint_ids = {_identity(remove) for remove in checkpoint_removes}
    assert {_identity(add) for add in original} <= checkpoint_ids
    reopened = tmp_path / "checkpoint_recovery"
    shutil.copytree(path, reopened)
    # Delta 4.0.1 SnapshotManagement requires the checkpoint's commit for its
    # timestamp/version anchor. Older commits are absent, so replay needs the checkpoint.
    for commit in (reopened / "_delta_log").glob("*.json"):
        if commit.stem.isdigit() and int(commit.stem) < before + 1:
            commit.unlink()
    assert set(_commits(reopened)) == {before + 1}
    assert _rows(spark, reopened) == _rows(jvm, reopened) == expected
    # Recently removed URIs and their DVs protect even physically old files.
    assert spark.sql(f"VACUUM delta.`{path}` DRY RUN").collect() == []
    spark.sql(f"VACUUM delta.`{path}`").collect()
    assert all(file.is_file() for file in physical_files)
    assert _rows(spark, path) == _rows(jvm, path) == expected

    # Alternate writers after the rewrite and read the new URI with both engines.
    spark.createDataFrame([(99, partition)], "id LONG, p STRING").write.format("delta").mode("append").save(str(path))
    expected[(99, partition)] += 1
    assert _rows(spark, path) == _rows(jvm, path) == expected
    assert all((path / unquote(row.path)).is_file() for row in _active_files(path))
    jvm.createDataFrame([(100, partition)], "id LONG, p STRING").write.format("delta").mode("append").save(str(path))
    expected[(100, partition)] += 1
    assert _rows(spark, path) == _rows(jvm, path) == expected


@pytest.mark.integration
@pytest.mark.parametrize("dv", [False, True], ids=["cow", "mor"])
@pytest.mark.parametrize("operation", ["update", "delete", "merge"])
def test_dml_routes_decoded_paths_but_removes_stored_uris(spark, delta_jvm_spark, tmp_path, dv, operation):
    jvm = delta_jvm_spark
    reference = tmp_path / "reference"
    path = tmp_path / "sail"
    partitions = ["日本語", "café", "a%2Fb % #?"]
    initial = [(i, p) for p in partitions for i in range(4)]
    (
        jvm.createDataFrame(initial, "id LONG, p STRING")
        .coalesce(1)
        .write.format("delta")
        .partitionBy("p")
        .option("delta.enableDeletionVectors", str(dv).lower())
        .option("delta.enableChangeDataFeed", "true")
        .save(str(reference))
    )
    if dv:
        jvm.sql(f"DELETE FROM delta.`{reference}` WHERE id = 0").collect()
        initial = [(i, p) for i, p in initial if i != 0]
    shutil.copytree(reference, path)
    original = [row.asDict(recursive=True) for row in _active_files(path)]
    assert all(bool(add.get("deletionVector")) == dv for add in original)
    version = max(_commits(path)) + 1
    statements = {
        "update": "UPDATE delta.`{path}` SET id = 10 WHERE id = 1",
        "delete": "DELETE FROM delta.`{path}` WHERE id = 1",
        "merge": (
            "MERGE INTO delta.`{path}` t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = 10"
        ),
    }
    for session, location in ((spark, path), (jvm, reference)):
        session.sql(statements[operation].format(path=location)).collect()
    expected = Counter((10 if i == 1 else i, p) for i, p in initial if operation != "delete" or i != 1)
    assert _rows(spark, path) == _rows(jvm, path) == _rows(jvm, reference) == expected
    actions = _commits(path)[version]
    _assert_removes_match(actions, original)
    adds = [action["add"] for action in actions if "add" in action]
    if dv:
        dv_adds = [add for add in adds if add.get("deletionVector")]
        assert {add["path"] for add in dv_adds} == {add["path"] for add in original}
        assert all(add["deletionVector"]["cardinality"] == 2 for add in dv_adds)
    else:
        assert not any(add.get("deletionVector") for add in adds)
    assert all(
        (path / unquote(action[key]["path"])).is_file() for action in actions for key in ("add", "cdc") if key in action
    )
    changes = []
    for session, location in ((spark, path), (jvm, path), (jvm, reference)):
        changes.append(
            Counter(
                tuple(row)
                for row in session.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", version)
                .option("endingVersion", version)
                .load(str(location))
                .select("id", "p", "_change_type")
                .collect()
            )
        )
    expected_changes = Counter((1, p, "delete" if operation == "delete" else "update_preimage") for p in partitions)
    if operation != "delete":
        expected_changes.update((10, p, "update_postimage") for p in partitions)
    assert changes == [expected_changes] * 3


@pytest.mark.parametrize("operation", ["update", "delete", "merge"])
def test_dv_writer_rejects_decoded_path_collision_without_commit(spark, tmp_path, operation):
    path = tmp_path / "collision"
    write_delta_with_row_groups(path, "none", include_stats=True)
    log = path / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    add = next(action["add"] for action in actions if "add" in action)
    stored = "café.parquet"
    (path / add["path"]).rename(path / stored)
    add["path"] = stored
    actions.append({"add": {**add, "path": quote(stored)}})
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))
    expected = Counter((i, f"value-{i}") for i in range(4352))
    expected.update((i, f"value-{i}") for i in range(4096))
    assert _rows(spark, path) == expected
    before = _commits(path)
    files = {file.relative_to(path) for file in path.rglob("*") if file.is_file()}
    statements = {
        "update": "UPDATE delta.`{path}` SET value = 'changed' WHERE id = 1",
        "delete": "DELETE FROM delta.`{path}` WHERE id = 1",
        "merge": ("MERGE INTO delta.`{path}` t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE"),
    }
    with pytest.raises(Exception, match="ambiguous Delta file identities"):
        spark.sql(statements[operation].format(path=path)).collect()
    assert _commits(path) == before
    assert all((path / file).is_file() for file in files)
    assert _rows(spark, path) == expected


@pytest.fixture
def uri_jvm_spark(delta_jvm_spark):
    # Keep Spark's snapshot repartitioning separate from Java URI equality.
    key = "spark.databricks.delta.snapshotPartitions"
    previous = delta_jvm_spark.conf.get(key)
    delta_jvm_spark.conf.set(key, "1")
    try:
        yield delta_jvm_spark
    finally:
        if previous is None:
            delta_jvm_spark.conf.unset(key)
        else:
            delta_jvm_spark.conf.set(key, previous)


def _write_commit(path, version, actions):
    (path / "_delta_log" / f"{version:020}.json").write_text("".join(json.dumps(action) + "\n" for action in actions))


def _cdf_rows(session, path):
    return Counter(
        tuple(row)
        for row in session.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .option("endingVersion", 1)
        .load(str(path))
        .select("id", "_change_type")
        .collect()
    )


def _uri_replay_table(jvm, root, dv, checkpoint):
    seed = root / "seed"
    (
        jvm.range(3)
        .coalesce(1)
        .write.format("delta")
        .option("delta.enableDeletionVectors", str(dv).lower())
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.checkpointInterval", "1")
        .save(str(seed))
    )
    if dv:
        jvm.sql(f"DELETE FROM delta.`{seed}` WHERE id = 2").collect()
    template = _active_files(seed)[0].asDict(recursive=True)
    assert bool(template.get("deletionVector")) == dv
    headers = [
        action
        for actions in _commits(seed).values()
        for action in actions
        if "protocol" in action or "metaData" in action
    ]
    path = root / "table"
    shutil.copytree(seed, path)
    shutil.rmtree(path / "_delta_log")
    (path / "_delta_log").mkdir()
    for physical in ("removedé.parquet", "café.parquet"):
        shutil.copyfile(seed / unquote(template["path"]), path / physical)
    (path / unquote(template["path"])).unlink()
    keep = path / "keep.parquet"
    pq.write_table(pa.table({"id": pa.array([10], type=pa.int64())}), keep)
    now = int(time.time() * 1000)
    literal = {**template, "path": "café.parquet", "dataChange": True}
    kept = {
        "path": keep.name,
        "partitionValues": {},
        "size": keep.stat().st_size,
        "modificationTime": now,
        "dataChange": True,
        "stats": json.dumps({"numRecords": 1}),
    }
    adds = [
        {**literal, "path": "removed%C3%A9.parquet"},
        {**literal, "path": "caf%C3%A9.parquet"},
        literal,
        kept,
    ]
    _write_commit(
        path, 0, [*headers, {"commitInfo": {"timestamp": now, "operation": "WRITE"}}, *({"add": add} for add in adds)]
    )
    if checkpoint:
        delta_log = jvm._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(jvm._jsparkSession, str(path))  # noqa: SLF001
        delta_log.checkpoint()
        checkpoint_rows = (
            pq.ParquetFile(path / "_delta_log" / "00000000000000000000.checkpoint.parquet").read().to_pylist()
        )
        assert {row["add"]["path"] for row in checkpoint_rows if row.get("add")} == {add["path"] for add in adds}
    # Escape hex case differs, but these Removes retain the extended metadata
    # required by Delta 4.0.1 TahoeRemoveFileIndex for cross-engine CDF reads.
    removes = [
        {
            "remove": {
                "path": stored,
                "dataChange": True,
                "deletionTimestamp": now + 1,
                "deletionVector": template.get("deletionVector"),
                "extendedFileMetadata": True,
                "size": template["size"],
                "partitionValues": template["partitionValues"],
            }
        }
        for stored in ("removed%c3%a9.parquet", "caf%c3%a9.parquet")
    ]
    _write_commit(path, 1, [{"commitInfo": {"timestamp": now + 1, "operation": "DELETE"}}, *removes])
    return path, [literal, kept]


@pytest.mark.integration
@pytest.mark.parametrize("checkpoint", [False, True], ids=["commits", "checkpoint-tail"])
@pytest.mark.parametrize("dv", [False, True], ids=["no-dv", "same-dv"])
@pytest.mark.parametrize(
    ("metadata", "strategy"),
    [("false", "sort"), ("true", "sort"), ("true", "hash")],
    ids=["typed", "arrow-sort", "arrow-hash"],
)
def test_uri_escape_case_replay_keeps_literal_unicode_distinct(
    spark, uri_jvm_spark, tmp_path, checkpoint, dv, metadata, strategy
):
    # Delta v4.0.1 InMemoryLogReplay keys on java.net.URI plus the DV unique ID.
    # URI.equals ignores hex case, but does not equate literal and escaped Unicode.
    jvm = uri_jvm_spark
    path, survivors = _uri_replay_table(jvm, tmp_path, dv, checkpoint)
    visible = range(2 if dv else 3)
    expected = Counter((i,) for i in [*visible, 10])
    frame = (
        spark.read.format("delta")
        .option("metadataAsDataRead", metadata)
        .option("deltaLogReplayStrategy", strategy)
        .load(str(path))
    )
    plan = frame._explain_string()  # noqa: SLF001
    assert ("DeltaLogReplayExec" in plan) == (metadata == "true"), plan
    if metadata == "true":
        mode = "hash-commits" if strategy == "hash" and not checkpoint else strategy
        assert f"mode={mode}" in plan, plan
        assert "delta_path_key" in plan, plan
    assert Counter(tuple(row) for row in frame.collect()) == expected
    assert _rows(spark, path) == _rows(jvm, path) == expected
    assert spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().numFiles == 2
    historical = Counter((i,) for i in visible for _ in range(3))
    historical[(10,)] += 1
    assert _rows(spark, path, versionAsOf=0) == _rows(jvm, path, versionAsOf=0) == historical
    for session in (spark, jvm):
        assert _cdf_rows(session, path) == Counter((i, "delete") for i in visible for _ in range(2))

    metrics = spark.sql(f"OPTIMIZE delta.`{path}`").first().metrics
    assert metrics.numFilesRemoved == 2
    _assert_removes_match(_commits(path)[2], survivors)
    assert _rows(spark, path) == _rows(jvm, path) == expected
    checkpoint_rows = pq.ParquetFile(path / "_delta_log" / "00000000000000000002.checkpoint.parquet").read().to_pylist()
    assert {row["add"]["path"] for row in checkpoint_rows if row.get("add")} == {
        action["add"]["path"] for action in _commits(path)[2] if "add" in action
    }
    reopened = tmp_path / "reopened"
    shutil.copytree(path, reopened)
    for version in (0, 1):
        (reopened / "_delta_log" / f"{version:020}.json").unlink()
    assert _rows(spark, reopened) == _rows(jvm, reopened) == expected
    # Spark writes after Sail's compaction, then alternate writers again on the
    # checkpoint-dependent copy to exercise reader invalidation and stored paths.
    for session, value in ((jvm, 20), (spark, 21), (jvm, 22)):
        session.range(value, value + 1).coalesce(1).write.format("delta").mode("append").save(str(reopened))
        expected[(value,)] += 1
        assert _rows(spark, reopened) == _rows(jvm, reopened) == expected


@pytest.mark.integration
@pytest.mark.parametrize("dv", [False, True], ids=["no-dv", "same-dv"])
def test_sail_cdf_resolves_minimal_removes_by_uri_and_dv(spark, uri_jvm_spark, tmp_path, dv):
    # Sail's historical Add fallback is an extension: Delta 4.0.1 rejects CDF
    # without extendedFileMetadata (DELTA_REMOVE_FILE_CDC_MISSING_EXTENDED_METADATA).
    # Spark supplies real DV bytes and a snapshot oracle, not a CDF success oracle.
    path, _ = _uri_replay_table(uri_jvm_spark, tmp_path, dv=dv, checkpoint=False)
    actions = _commits(path)[1]
    for action in actions:
        if "remove" in action:
            remove = action["remove"]
            for field in ("extendedFileMetadata", "size", "partitionValues"):
                del remove[field]
            assert set(remove) == {"path", "dataChange", "deletionTimestamp", "deletionVector"}
    _write_commit(path, 1, actions)
    visible = range(2 if dv else 3)
    expected = Counter((i,) for i in [*visible, 10])
    assert _rows(spark, path) == _rows(uri_jvm_spark, path) == expected
    assert _cdf_rows(spark, path) == Counter((i, "delete") for i in visible for _ in range(2))


def _dv_replacement_table(jvm, root, replacement_path):
    path, _ = _uri_replay_table(jvm, root, dv=True, checkpoint=False)
    seed = root / "seed"
    jvm.sql(f"DELETE FROM delta.`{seed}` WHERE id = 1").collect()
    replacement_dv = _active_files(seed)[0].deletionVector
    assert replacement_dv["cardinality"] == 2
    shutil.copytree(seed, path, dirs_exist_ok=True, ignore=shutil.ignore_patterns("_delta_log", "*.parquet"))
    old = next(
        action["add"] for action in _commits(path)[0] if action.get("add", {}).get("path") == "removed%C3%A9.parquet"
    )
    replacement = {**old, "path": replacement_path, "deletionVector": replacement_dv}
    remove = {
        "path": old["path"],
        "deletionVector": old["deletionVector"],
        "dataChange": True,
        "extendedFileMetadata": True,
        "size": old["size"],
        "partitionValues": old["partitionValues"],
    }
    _write_commit(path, 1, [_commits(path)[1][0], {"remove": remove}, {"add": replacement}])
    return path


@pytest.mark.integration
def test_cdf_pairs_dv_replacements_with_identical_uri_spelling(spark, uri_jvm_spark, tmp_path):
    # Delta 4.0.1 CDCReader pairs FilePathWithTableVersion by its raw path String.
    path = _dv_replacement_table(uri_jvm_spark, tmp_path, "removed%C3%A9.parquet")
    expected = Counter({(0,): 3, (1,): 2, (10,): 1})
    assert _rows(spark, path) == _rows(uri_jvm_spark, path) == expected
    for session in (spark, uri_jvm_spark):
        assert _cdf_rows(session, path) == Counter({(1, "delete"): 1})


@pytest.mark.integration
def test_sail_cdf_pairs_dv_replacements_across_uri_escape_case(spark, uri_jvm_spark, tmp_path):
    # URI-normalized CDF pairing is a Sail extension to Delta 4.0.1's raw-string
    # CDCReader pairing. Spark snapshot replay still supplies the row-state oracle.
    path = _dv_replacement_table(uri_jvm_spark, tmp_path, "removed%c3%a9.parquet")
    expected = Counter({(0,): 3, (1,): 2, (10,): 1})
    assert _rows(spark, path) == _rows(uri_jvm_spark, path) == expected
    assert _cdf_rows(spark, path) == Counter({(1, "delete"): 1})
