"""MERGE consumers must agree on one source evaluation and its file effects.

Related upstream contracts: delta-io/delta v3.3.2,
MergeIntoMaterializeSourceSuite (rand/UDF sources, arbitrary source errors).
The audited UDF detects repeated evaluation without a probabilistic row oracle.
"""

import json
import os
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as sf

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request):
    # The audit directory is shared by the local-cluster worker processes.
    with spark_connect_server(
        {
            "SAIL_MODE": request.param,
            "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
            "SAIL_CLUSTER__WORKER_TASK_SLOTS": "16",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
        }
    ) as server:
        yield server.remote


@pytest.fixture
def merge_target(spark, tmp_path):
    name = "merge_source_" + uuid.uuid4().hex
    path = tmp_path / "target"
    spark.sql(f"""
        CREATE TABLE {name} (id INT, msg STRING) USING DELTA
        PARTITIONED BY (id) LOCATION '{escape_sql_string_literal(str(path))}'
    """).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'old'), (2, 'old')").collect()  # noqa: S608
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def _log(path):
    return {file.name: file.read_bytes() for file in sorted((path / "_delta_log").glob("*.json"))}


def _actions(log):
    return [json.loads(line) for contents in log.values() for line in contents.splitlines()]


def _active_files(log):
    active = {}
    for action in _actions(log):
        if "remove" in action:
            active.pop(action["remove"]["path"], None)
        if "add" in action:
            active[action["add"]["path"]] = action["add"]
    return active


def _merge(spark, name, source):
    spark.sql(f"""
        MERGE INTO {name} t USING ({source}) s ON t.id=s.id
        WHEN MATCHED THEN UPDATE SET msg=s.msg
        WHEN NOT MATCHED THEN INSERT (id,msg) VALUES(s.id,s.msg)
    """).collect()  # noqa: S608


def _assert_update(spark, name, path, before, key):
    expected = [(i, "new" if i == key else "old") for i in (1, 2)]
    assert spark.table(name).orderBy("id").collect() == expected
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == expected
    after = _log(path)
    assert all(after[name] == contents for name, contents in before.items())
    commits = {name: contents for name, contents in after.items() if name not in before}
    assert len(commits) == 1
    actions = _actions(commits)
    removed = {action["remove"]["path"] for action in actions if "remove" in action}
    old_files = _active_files(before)
    assert removed == {name for name, add in old_files.items() if int(add["partitionValues"]["id"]) == key}
    active = _active_files(after)
    assert set(old_files) - removed <= set(active)
    added = [action["add"] for action in actions if "add" in action]
    assert len(added) == 1
    assert int(added[0]["partitionValues"]["id"]) == key
    metrics = next(action["commitInfo"]["operationMetrics"] for action in actions if "commitInfo" in action)
    assert int(metrics["numSourceRows"]) == 1
    assert int(metrics["numTargetRowsUpdated"]) == 1
    assert int(metrics["numTargetRowsInserted"]) == 0
    assert int(metrics["numTargetRowsDeleted"]) == 0
    disk_rows = sorted(
        (int(add["partitionValues"]["id"]), row["msg"])
        for name, add in active.items()
        for row in pq.ParquetFile(path / name).read().to_pylist()
    )
    assert disk_rows == expected


@pytest.mark.parametrize("source_kind", ["fixed", "random", "audited"])
def test_merge_source_is_evaluated_once(spark, merge_target, tmp_path, source_kind):
    name, path = merge_target
    before = _log(path)
    source_name = name + "_source"
    audit = tmp_path / "evaluations"
    audit.mkdir()
    if source_kind == "fixed":
        source = spark.createDataFrame([(1, "new")], "id INT, msg STRING")
    elif source_kind == "random":
        source = spark.sql("SELECT 1 + CAST(rand() * 2 AS INT) id, 'new' msg")
    else:
        audit_path = str(audit)

        @sf.udf("int")
        def audited_key(value):
            token = uuid.uuid4()
            key = 1 + (token.int % 2) + int(value)
            event = {"key": key, "pid": os.getpid()}
            (Path(audit_path) / f"{token.hex}.json").write_text(json.dumps(event))
            return key

        source = spark.range(1).select(audited_key.asNondeterministic()("id").alias("id"), sf.lit("new").alias("msg"))
    source.createOrReplaceTempView(source_name)
    try:
        _merge(spark, name, f"SELECT * FROM {source_name}")
        if source_kind == "audited":
            events = [json.loads(file.read_text()) for file in audit.glob("*.json")]
            assert len(events) == 1, events
            key = events[0]["key"]
        elif source_kind == "fixed":
            key = 1
        else:
            updated = spark.table(name).where("msg = 'new'").collect()
            assert len(updated) == 1
            key = updated[0].id
        _assert_update(spark, name, path, before, key)
    finally:
        spark.catalog.dropTempView(source_name)


def test_merge_source_failure_does_not_commit(spark, merge_target):
    name, path = merge_target
    before = _log(path)
    schema = spark.table(name).schema
    files = {file: (path / file).read_bytes() for file in _active_files(before)}
    with pytest.raises(Exception, match="merge source failed"):
        _merge(spark, name, "SELECT CAST(raise_error('merge source failed') AS INT) id, 'new' msg")
    assert _log(path) == before
    assert spark.table(name).schema == schema
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [(1, "old"), (2, "old")]
    assert all((path / file).read_bytes() == contents for file, contents in files.items())
    # A subsequent successful statement must not inherit failed materialization state.
    _merge(spark, name, "SELECT 1 id, 'new' msg")
    _assert_update(spark, name, path, before, 1)


@pytest.mark.parametrize("deletion_vectors", [False, True])
def test_merge_selected_rhs_and_struct_alignment_are_evaluated_once(spark, tmp_path, deletion_vectors, request):
    path = tmp_path / "rhs"
    audit = tmp_path / "rhs_events"
    audit.mkdir()
    function = "merge_rhs_" + uuid.uuid4().hex
    audit_path = str(audit)

    @sf.udf("STRUCT<b: STRING, a: STRING>")
    def value_once(key):
        token = uuid.uuid4().hex
        (Path(audit_path) / f"{token}.json").write_text(json.dumps({"id": key, "token": token}))
        return token, token

    spark.udf.register(function, value_once.asNondeterministic())
    target = "merge_rhs_target_" + uuid.uuid4().hex
    request.addfinalizer(lambda: spark.sql(f"DROP TABLE IF EXISTS {target}").collect())
    spark.sql(f"""
        CREATE TABLE {target} (id INT, payload STRUCT<a: STRING, b: STRING>, token STRING)
        USING DELTA LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES (
          'delta.enableChangeDataFeed'='true',
          'delta.enableDeletionVectors'='{str(deletion_vectors).lower()}')
    """).collect()
    spark.sql(f"INSERT INTO {target} VALUES (1, named_struct('a','old','b','old'), 'old'), (2, NULL, 'old')").collect()
    spark.sql(f"""
        MERGE INTO {target} t USING (SELECT * FROM VALUES (1), (2), (3) AS source(id)) s ON t.id=s.id
        WHEN MATCHED AND CAST(NULL AS BOOLEAN) THEN UPDATE SET token=raise_error('null clause selected')
        WHEN MATCHED AND s.id=1 THEN UPDATE SET payload={function}(s.id), token=uuid()
        WHEN MATCHED THEN UPDATE SET token='second'
        WHEN NOT MATCHED AND s.id=99 THEN INSERT (id,token) VALUES(s.id,raise_error('unselected insert'))
        WHEN NOT MATCHED THEN INSERT (id,payload,token) VALUES(s.id,{function}(s.id),uuid())
    """).collect()
    events = [json.loads(file.read_text()) for file in audit.glob("*.json")]
    assert sorted(event["id"] for event in events) == [1, 3], events
    expected = {event["id"]: event["token"] for event in events}
    rows = spark.read.format("delta").load(str(path)).orderBy("id").collect()
    assert [row.id for row in rows] == [1, 2, 3]
    assert rows[1].payload is None and rows[1].token == "second"
    for row in (rows[0], rows[2]):
        assert tuple(row.payload) == (expected[row.id], expected[row.id])
        assert row.token != expected[row.id]
    postimages = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 2)
        .load(str(path))
        .where("_change_type IN ('insert','update_postimage')")
        .select("id", "payload", "token")
        .orderBy("id")
        .collect()
    )
    assert postimages == rows
    assert spark.read.format("delta").load(str(path)).columns == ["id", "payload", "token"]


@pytest.mark.parametrize(
    "source,condition,count",
    [
        ("SELECT 1 id WHERE false", "true", 0),
        ("SELECT 1 id UNION ALL SELECT 2 id", "true", 2),
        ("SELECT 3 id UNION ALL SELECT 3 id", "false", 2),
    ],
)
def test_fast_append_counts_entire_source_without_rewrites(spark, merge_target, source, condition, count):
    name, path = merge_target
    # An empty CDF action preserves the no-op commit's metrics; without CDF,
    # DeltaCommit intentionally skips transactions with no payload actions.
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')").collect()
    before = _log(path)
    files = _active_files(before)
    spark.sql(f"""
        MERGE INTO {name} t USING ({source}) s ON t.id=s.id
        WHEN NOT MATCHED AND {condition} THEN INSERT (id,msg) VALUES(s.id,'new')
    """).collect()
    after = _log(path)
    assert _active_files(after) == files
    actions = _actions({name: data for name, data in after.items() if name not in before})
    assert not any("remove" in action or "add" in action for action in actions)
    metrics = next(action["commitInfo"]["operationMetrics"] for action in actions if "commitInfo" in action)
    assert int(metrics["numSourceRows"]) == count
    assert int(metrics["numTargetRowsInserted"]) == 0
    assert int(metrics["numTargetRowsUpdated"]) == 0


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "dv"])
def test_target_only_merge_preserves_untouched_files_and_artifact_metrics(spark, tmp_path, deletion_vectors, request):
    path = tmp_path / "target_only"
    target = "merge_target_only_" + uuid.uuid4().hex
    request.addfinalizer(lambda: spark.sql(f"DROP TABLE IF EXISTS {target}").collect())
    spark.sql(f"""
        CREATE TABLE {target} (id INT, msg STRING) USING DELTA
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES (
          'delta.enableChangeDataFeed'='true',
          'delta.enableDeletionVectors'='{str(deletion_vectors).lower()}')
    """).collect()
    spark.createDataFrame([(1, "old"), (2, "old"), (3, "old"), (5, "old"), (6, "old")], "id INT, msg STRING").coalesce(
        1
    ).write.format("delta").mode("append").save(str(path))
    touched = _active_files(_log(path))
    assert len(touched) == 1
    spark.createDataFrame([(99, "untouched")], "id INT, msg STRING").coalesce(1).write.format("delta").mode(
        "append"
    ).save(str(path))
    before = _log(path)
    old_files = _active_files(before)
    untouched = set(old_files) - set(touched)
    assert len(untouched) == 1
    contents = {name: (path / name).read_bytes() for name in old_files}
    spark.sql(f"""
        MERGE INTO {target} t USING (SELECT 0 id WHERE false) s ON t.id=s.id
        WHEN NOT MATCHED BY SOURCE AND t.id=5 THEN UPDATE SET msg='new'
        WHEN NOT MATCHED BY SOURCE AND t.id=6 THEN DELETE
    """).collect()
    after = _log(path)
    assert all(after[name] == data for name, data in before.items())
    commits = {name: data for name, data in after.items() if name not in before}
    assert len(commits) == 1
    actions = _actions(commits)
    added = [action["add"] for action in actions if "add" in action]
    removed = [action["remove"] for action in actions if "remove" in action]
    assert {add["path"] for add in removed} == set(touched)
    assert untouched.isdisjoint(add["path"] for add in added)
    active = _active_files(after)
    assert all(active[name] == old_files[name] and (path / name).read_bytes() == contents[name] for name in untouched)
    expected = [(1, "old"), (2, "old"), (3, "old"), (5, "new"), (99, "untouched")]
    assert spark.table(target).orderBy("id").collect() == expected
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == expected
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", int(next(iter(commits)).split(".")[0]))
        .load(str(path))
        .select("id", "msg", "_change_type")
        .orderBy("id", "_change_type")
        .collect()
    )
    assert changes == [(5, "new", "update_postimage"), (5, "old", "update_preimage"), (6, "old", "delete")]
    metrics = next(action["commitInfo"]["operationMetrics"] for action in actions if "commitInfo" in action)
    expected_metrics = {
        "numSourceRows": 0,
        "numTargetRowsUpdated": 1,
        "numTargetRowsDeleted": 1,
        "numTargetRowsInserted": 0,
        "numTargetRowsNotMatchedBySourceUpdated": 1,
        "numTargetRowsNotMatchedBySourceDeleted": 1,
        "numTargetFilesRemoved": 1,
        "numTargetFilesAdded": len(added),
        "numTargetRowsCopied": 0 if deletion_vectors else 3,
    }
    assert {key: int(metrics[key]) for key in expected_metrics} == expected_metrics
    new_files = [add for add in added if add["path"] not in old_files]
    assert int(metrics["numOutputFiles"]) == len(new_files)
    assert int(metrics["numTargetBytesAdded"]) == sum(add["size"] for add in new_files)
    assert int(metrics["numOutputRows"]) == sum(json.loads(add["stats"])["numRecords"] for add in new_files)
    for add in new_files:
        assert (path / add["path"]).stat().st_size == add["size"]
        assert pq.ParquetFile(path / add["path"]).schema_arrow.names == ["id", "msg"]
    if deletion_vectors:
        assert all((path / name).read_bytes() == contents[name] for name in touched)
        assert sum(active[name]["deletionVector"]["cardinality"] for name in touched) == 2
        assert int(metrics["numTargetDeletionVectorsAdded"]) == 1
    else:
        assert set(touched).isdisjoint(active)
        assert (
            sorted((row["id"], row["msg"]) for name in active for row in pq.ParquetFile(path / name).read().to_pylist())
            == expected
        )


def test_merge_assignment_scalar_subquery(spark, merge_target):
    name, path = merge_target
    before = _log(path)
    spark.sql(f"""
        MERGE INTO {name} t USING (SELECT 1 id) s ON t.id=s.id
        WHEN MATCHED THEN UPDATE SET msg=(SELECT 'new')
    """).collect()
    _assert_update(spark, name, path, before, 1)
