from __future__ import annotations

import json
import os
import shutil
import time
from urllib.parse import unquote

import pytest
from pyspark.sql import Row
from pyspark.sql.connect import proto

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import pyspark_version
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _commits(path):
    return {
        int(file.stem): [json.loads(line) for line in file.read_text().splitlines()]
        for file in (path / "_delta_log").glob("*.json")
        if file.stem.isdigit()
    }


def _active_files(path):
    # These bounded fixtures retain every JSON commit, so file state can be checked directly.
    def identity(action):
        dv = action.get("deletionVector")
        return action["path"], None if dv is None else (dv["storageType"], dv["pathOrInlineDv"], dv.get("offset"))

    files = {}
    for _, actions in sorted(_commits(path).items()):
        for action in actions:
            if "remove" in action:
                files.pop(identity(action["remove"]), None)
        for action in actions:
            if "add" in action:
                files[identity(action["add"])] = action["add"]
    return [Row(**{"deletionVector": None, **add}) for add in files.values()]


def _paths(rows):
    # Hadoop Path strings may contain a literal '#'.
    return {unquote(row.path.removeprefix("file:").removeprefix("//")) for row in rows}


@pytest.fixture
def maintenance_table(spark, tmp_path):
    name = "delta_maintenance"
    path = tmp_path / "table with % space"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, part STRING) USING delta PARTITIONED BY (part) "
        f"LOCATION '{escape_sql_string_literal(path.as_uri())}'"
    )
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("suffix", "default", "value"),
    [
        ("optimize.minFileSize", "1073741824", "1"),
        ("optimize.maxFileSize", "1073741824", "2048"),
        ("optimize.maxDeletedRowsRatio", "0.05", "0.25"),
        ("retentionDurationCheck.enabled", "true", "false"),
        ("vacuum.retentionWindowIgnore.enabled", "true", "false"),
    ],
)
def test_maintenance_config_registry(spark, delta_jvm_spark, tmp_path, suffix, default, value):
    delta_jvm_spark.sql(f"CREATE TABLE delta.`{tmp_path / 'config'}` (id BIGINT) USING delta")
    key = f"spark.databricks.delta.{suffix}"
    sessions = [spark]
    # The Delta releases used with Spark 3.5/4.0 do not register this vacuum setting.
    if suffix != "vacuum.retentionWindowIgnore.enabled" or pyspark_version() >= (4, 1):
        sessions.append(delta_jvm_spark)
    previous = [session.conf.get(key, None) for session in sessions]
    try:
        for session in sessions:
            session.conf.unset(key)
            assert session.conf.isModifiable(key)
            assert session.conf.get(key) == default
            assert session.conf.get(key, value) == value
            session.sql(f"SET {key}={value}").collect()
            assert session.conf.get(key) == value
        response = spark.client.config(proto.ConfigRequest.Operation(get_all=proto.ConfigRequest.GetAll()))
        assert dict(response.pairs)[key] == value
        for session in sessions:
            session.sql(f"RESET {key}").collect()
            assert session.conf.get(key) == default
    finally:
        for session, original in zip(sessions, previous, strict=True):
            if original is None:
                session.conf.unset(key)
            else:
                session.conf.set(key, original)


def test_native_inspection_sql(spark, maintenance_table):
    name, path = maintenance_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'x')")  # noqa: S608
    detail = spark.sql(f"DESCRIBE DETAIL sail.default.{name}").first()
    assert spark.sql(f"DESC DETAIL {name}").first() == detail
    expected = spark.sql(f"DESCRIBE HISTORY sail.default.{name}").collect()
    assert spark.sql(f"DESCRIBE HISTORY {name}").collect() == expected
    assert spark.sql(f"DESC HISTORY {name} LIMIT 1").collect() == expected[:1]
    assert spark.sql(f"DESCRIBE HISTORY {name} LIMIT 0").collect() == []
    for target in [f"delta.`{path}`", f"'{escape_sql_string_literal(str(path))}'"]:
        assert spark.sql(f"DESCRIBE HISTORY {target}").collect() == expected
        path_detail = spark.sql(f"DESCRIBE DETAIL {target}").first()
        assert path_detail.id == detail.id
        assert path_detail.numFiles == detail.numFiles
        assert path_detail.name is None


@pytest.mark.parametrize("mapped", [False, True])
def test_optimize_partition_compaction_and_idempotence(spark, maintenance_table, mapped):
    name, path = maintenance_table
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.appendOnly' = 'true')"
    )
    if mapped:
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    for i in range(3):
        spark.sql(f"INSERT INTO {name} VALUES ({i}, 'a b'), ({i}, 'other'), ({i}, NULL)")  # noqa: S608
    rows = spark.table(name).orderBy("id", "part").collect()
    before_version = max(_commits(path))
    key = "spark.databricks.delta.optimize.minFileSize"
    spark.conf.set(key, "1")
    try:
        assert spark.sql(f"OPTIMIZE {name}").first().metrics.numFilesRemoved == 0
        assert max(_commits(path)) == before_version
    finally:
        spark.conf.unset(key)
    files = _active_files(path)
    selected = {row.path for row in files if "a b" in row.partitionValues.values()}
    untouched = {row.path for row in files} - selected
    result = spark.sql(f"OPTIMIZE {name} WHERE part = 'a b'").first()
    assert result.metrics.numFilesRemoved == len(selected)
    assert result.metrics.numFilesAdded == 1
    assert result.metrics.partitionsOptimized == 1
    assert untouched <= {row.path for row in _active_files(path)}
    assert spark.table(name).orderBy("id", "part").collect() == rows
    commits = _commits(path)
    assert max(commits) == before_version + 1
    rewrite = commits[max(commits)]
    assert next(action["commitInfo"]["operation"] for action in rewrite if "commitInfo" in action) == "OPTIMIZE"
    assert all(not action[key]["dataChange"] for action in rewrite for key in ["add", "remove"] if key in action)
    assert spark.read.format("delta").option("versionAsOf", before_version).load(str(path)).count() == len(rows)
    assert (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", max(commits))
        .load(str(path))
        .count()
        == 0
    )
    assert spark.sql(f"OPTIMIZE {name} WHERE part = 'a b'").first().metrics.numFilesRemoved == 0
    assert max(_commits(path)) == max(commits)
    # All remaining partitions are compacted independently, including the null partition.
    spark.sql(f"OPTIMIZE delta.`{path}`").collect()
    assert len(_active_files(path)) == 3  # noqa: PLR2004
    assert spark.table(name).orderBy("id", "part").collect() == rows
    with pytest.raises(Exception, match="only partition columns"):
        spark.sql(f"OPTIMIZE {name} WHERE id = 0").collect()


def test_optimize_matches_jvm_schema_and_artifacts(spark, delta_jvm_spark, tmp_path):
    jvm = delta_jvm_spark
    reference = tmp_path / "reference"
    path = tmp_path / "sail"
    jvm.range(40).repartition(4).write.format("delta").save(str(reference))
    shutil.copytree(reference, path)
    spark.sql(f"CREATE TABLE optimize_interop USING delta LOCATION '{path.as_uri()}'")
    try:
        expected = jvm.sql(f"OPTIMIZE delta.`{reference}`")
        actual = spark.sql("OPTIMIZE optimize_interop")
        assert actual.schema == expected.schema
        metrics = actual.first().metrics
        assert metrics.numFilesRemoved == expected.first().metrics.numFilesRemoved
        assert metrics.numFilesAdded == expected.first().metrics.numFilesAdded == 1
        assert metrics.totalConsideredFiles == metrics.numFilesRemoved
        assert metrics.totalFilesSkipped == 0
        files = _active_files(path)
        assert metrics.filesAdded.totalSize == sum(row.size for row in files)
        assert metrics.filesAdded.totalFiles == len(files)
        assert sorted(jvm.read.format("delta").load(str(path)).collect()) == jvm.range(40).collect()
        history = jvm.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first()
        assert history.operation == "OPTIMIZE"
        assert history.operationMetrics["numAddedFiles"] == "1"
    finally:
        spark.sql("DROP TABLE optimize_interop")


def test_optimize_multiple_bins_commit_atomically(spark, maintenance_table):
    name, path = maintenance_table
    for i in range(3):
        spark.sql(f"INSERT INTO {name} VALUES ({i}, 'a'), ({i}, 'b'), ({i}, 'c'), ({i}, 'd')")  # noqa: S608
    expected = spark.table(name).orderBy("part", "id").collect()
    before = max(_commits(path))
    result = spark.sql(f"OPTIMIZE {name}").first().metrics
    assert result.numBins == 4  # noqa: PLR2004
    assert result.totalScheduledTasks == result.numBins
    assert result.totalClusterParallelism > 1
    assert result.numFilesAdded == result.numBins
    assert max(_commits(path)) == before + 1
    assert spark.table(name).orderBy("part", "id").collect() == expected
    actions = _commits(path)[before + 1]
    assert all(not action[key]["dataChange"] for action in actions for key in ["add", "remove"] if key in action)


def test_optimize_failed_bin_does_not_commit(spark, maintenance_table):
    name, path = maintenance_table
    for i in range(2):
        spark.sql(f"INSERT INTO {name} VALUES ({i}, 'a'), ({i}, 'b')")  # noqa: S608
    files = _active_files(path)
    missing = next(row for row in files if "b" in row.partitionValues.values())
    (path / unquote(missing.path)).unlink()
    before = _commits(path)
    with pytest.raises(Exception, match=r"(?i)(not found|no such file)"):
        spark.sql(f"OPTIMIZE {name}").collect()
    assert _commits(path) == before
    assert {row.path for row in _active_files(path)} == {row.path for row in files}


def test_optimize_materializes_dvs_and_vacuum_protects_them(spark, delta_jvm_spark, tmp_path):
    path = tmp_path / "mapped % # dv"
    jvm = delta_jvm_spark
    jvm.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, `_part` STRING) USING delta PARTITIONED BY (`_part`) "
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')"
    )
    jvm.sql(f"INSERT INTO delta.`{path}` VALUES (1, '%# space'), (2, '%# space'), (3, '%# space')")  # noqa: S608
    jvm.sql(f"DELETE FROM delta.`{path}` WHERE id = 1")  # noqa: S608
    spark.sql(f"CREATE TABLE optimize_dv USING delta LOCATION '{path.as_uri()}'")
    try:
        original = [file for file in path.rglob("*") if file.is_file() and "_delta_log" not in file.parts]
        old = time.time() - 8 * 24 * 3600
        for file in original:
            os.utime(file, (old, old))
        assert spark.sql("VACUUM optimize_dv DRY RUN").collect() == []
        result = spark.sql("OPTIMIZE optimize_dv").first()
        assert result.metrics.deletionVectorStats.numDeletionVectorsRemoved == 1
        assert result.metrics.deletionVectorStats.numDeletionVectorRowsRemoved == 1
        assert all(row.deletionVector is None for row in _active_files(path))
        assert sorted(row.id for row in jvm.read.format("delta").load(str(path)).collect()) == [2, 3]
        # Recent tombstones continue to protect the old data file and bitmap despite old mtimes.
        assert spark.sql("VACUUM optimize_dv DRY RUN").collect() == []
        assert all(file.exists() for file in original)
    finally:
        spark.sql("DROP TABLE optimize_dv")


def test_vacuum_retention_dry_run_and_audit(spark, delta_jvm_spark, maintenance_table):
    name, path = maintenance_table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 1 hours')")
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a b')")  # noqa: S608
    removed = path / unquote(_active_files(path)[0].path)
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2, 'new')")  # noqa: S608
    active = path / unquote(_active_files(path)[0].path)
    orphan = path / "orphan % #.parquet"
    orphan.write_bytes(b"orphan")
    cdf = path / "_change_data" / "expired.parquet"
    cdf.parent.mkdir(exist_ok=True)
    cdf.write_bytes(b"expired change data")
    recent = path / "recent.parquet"
    recent.write_bytes(b"recent")
    hidden = path / "_streaming" / "checkpoint"
    hidden.parent.mkdir()
    hidden.write_bytes(b"preserve")
    old = time.time() - 2 * 3600
    for file in [orphan, cdf, removed, active, hidden]:
        os.utime(file, (old, old))
    before = max(_commits(path))
    dry = spark.sql(f"VACUUM {name} FULL DRY RUN RETAIN 1 HOURS")
    # The table property takes precedence over nonzero RETAIN values by default.
    assert _paths(spark.sql(f"VACUUM {name} RETAIN 24 HOURS DRY RUN").collect()) == {str(orphan), str(cdf)}
    ignore_window = "spark.databricks.delta.vacuum.retentionWindowIgnore.enabled"
    spark.conf.set(ignore_window, "false")
    try:
        assert spark.sql(f"VACUUM {name} RETAIN 24 HOURS DRY RUN").collect() == []
    finally:
        spark.conf.unset(ignore_window)
    parallelism = "spark.sql.sources.parallelPartitionDiscovery.parallelism"
    previous = delta_jvm_spark.conf.get(parallelism)
    delta_jvm_spark.conf.set(parallelism, "2")
    try:
        reference = delta_jvm_spark.sql(f"VACUUM delta.`{path}` FULL DRY RUN RETAIN 1 HOURS")
        assert dry.schema == reference.schema
        assert _paths(dry.collect()) == _paths(reference.collect()) == {str(orphan), str(cdf)}
    finally:
        delta_jvm_spark.conf.set(parallelism, previous)
    assert orphan.exists()
    assert max(_commits(path)) == before
    with pytest.raises(Exception, match=r"retention.*shorter"):
        spark.sql(f"VACUUM {name} RETAIN 0 HOURS").collect()
    actual = spark.sql(f"VACUUM '{escape_sql_string_literal(str(path))}' RETAIN 1 HOURS")
    assert actual.schema == dry.schema
    assert _paths(actual.collect()) == {str(path)}
    assert not orphan.exists()
    assert not cdf.exists()
    assert all(file.exists() for file in [recent, hidden, removed, active])
    assert [row.operation for row in spark.sql(f"DESCRIBE HISTORY {name} LIMIT 2").collect()] == [
        "VACUUM END",
        "VACUUM START",
    ]
    assert spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first().operationMetrics["numDeletedFiles"] == "2"
    key = "spark.databricks.delta.retentionDurationCheck.enabled"
    spark.conf.set(key, "false")
    try:
        assert str(removed) in _paths(spark.sql(f"VACUUM {name} RETAIN 0 HOURS DRY RUN").collect())
        spark.sql(f"VACUUM {name} RETAIN 0 HOURS").collect()
    finally:
        spark.conf.unset(key)
    assert not removed.exists()
    assert active.exists()
    assert hidden.exists()
    assert spark.table(name).first().id == 2  # noqa: PLR2004


def test_maintenance_errors_and_explain_do_not_mutate(spark, maintenance_table):
    name, path = maintenance_table
    before = _commits(path)
    assert spark.sql(f"OPTIMIZE {name}").first().metrics.numFilesRemoved == 0
    assert spark.sql(f"VACUUM {name} DRY RUN").collect() == []
    assert _commits(path) == before
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")  # noqa: S608
    before = _commits(path)
    for sql in [f"OPTIMIZE {name}", f"VACUUM {name} RETAIN 0 HOURS"]:
        spark.sql(f"EXPLAIN EXTENDED {sql}").collect()
    assert _commits(path) == before
    for sql, message in [
        (f"OPTIMIZE {name} FULL", "not implemented"),
        (f"OPTIMIZE {name} ZORDER BY (id)", "not implemented"),
        (f"VACUUM {name} LITE", "FULL mode only"),
        (f"VACUUM {name} RETAIN 1 HOURS RETAIN 2 HOURS", "Duplicate argument"),
        ("VACUUM missing_table", "(?i)(not found|does not exist)"),
    ]:
        with pytest.raises(Exception, match=message):
            spark.sql(sql).collect()
    assert _commits(path) == before


@pytest.mark.parametrize(("partitions", "parallelism"), [(1, 1), (4, 2), (4, 8)])
@pytest.mark.yamlsnapshot(group="plan")
def test_maintenance_distributed_plan(tmp_path, snapshot, partitions, parallelism):
    with (
        spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
                "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(parallelism),
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        path = tmp_path / "distributed"
        spark.sql(
            f"CREATE TABLE maintenance_plan (id BIGINT, part INT) USING delta "
            f"PARTITIONED BY (part) LOCATION '{path.as_uri()}'"
        )
        try:
            for value in range(2):
                spark.sql(f"INSERT INTO maintenance_plan SELECT {value}, CAST(id AS INT) FROM range({partitions})")  # noqa: S608
            plans = {}
            for statement in ["OPTIMIZE maintenance_plan", "VACUUM maintenance_plan DRY RUN"]:
                plan = spark.sql(f"EXPLAIN CODEGEN {statement}").first().plan
                plans[statement] = normalize_plan_text(plan[plan.index("== Distributed Plan ==") :])
            snapshot.assert_match(plans)
            metrics = spark.sql("OPTIMIZE maintenance_plan").first().metrics
            assert metrics.numFilesRemoved == 2 * partitions
            assert metrics.numFilesAdded == partitions
            assert metrics.totalClusterParallelism == min(partitions, parallelism)
            assert metrics.totalScheduledTasks == min(partitions, parallelism)
            assert spark.table("maintenance_plan").count() == 2 * partitions
            assert spark.sql("VACUUM maintenance_plan DRY RUN").collect() == []
        finally:
            spark.sql("DROP TABLE maintenance_plan")
