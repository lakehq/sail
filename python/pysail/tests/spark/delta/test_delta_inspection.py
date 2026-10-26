from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture
def inspection_table(spark, tmp_path):
    name = "delta_inspection"
    path = tmp_path / name
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, part STRING) USING delta "
        f"PARTITIONED BY (part) LOCATION '{escape_sql_string_literal(path.as_uri())}'"
    )
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _actions(path, version):
    return [json.loads(line) for line in (path / "_delta_log" / f"{version:020d}.json").read_text().splitlines()]


def _write_log(path, commits, *, configuration=None, protocol=None):
    log = path / "_delta_log"
    log.mkdir(parents=True)
    metadata = {
        "id": "delta-inspection-fixture",
        "name": "log-name",
        "description": "inspection fixture",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps(
            {"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": True, "metadata": {}}]}
        ),
        "partitionColumns": [],
        "createdTime": 1700000000000,
        "configuration": configuration or {},
    }
    for version, commit in enumerate(commits):
        actions = commit
        if version == 0:
            actions = [
                {"protocol": protocol or {"minReaderVersion": 1, "minWriterVersion": 2}},
                {"metaData": metadata},
                *actions,
            ]
        file = log / f"{version:020d}.json"
        file.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
        # Equal file times exercise Spark's monotonic history adjustment.
        os.utime(file, ns=(1700000000000000000, 1700000000000000000))


def test_native_detail_and_history(spark, inspection_table):
    name, path = inspection_table
    metadata = next(action["metaData"] for action in _actions(path, 0) if "metaData" in action)
    empty = spark.sql(f"DESCRIBE DETAIL {name}").first()
    assert empty.id == metadata["id"]
    assert empty.numFiles == 0
    assert empty.sizeInBytes == 0

    spark.sql(f"INSERT INTO {name} VALUES (1, 'a b'), (2, NULL)")  # noqa: S608
    adds = [action["add"] for action in _actions(path, 1) if "add" in action]
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
    assert detail.numFiles == len(adds)
    assert detail.sizeInBytes == sum(add["size"] for add in adds)
    assert detail.partitionColumns == ["part"]
    assert detail.clusteringColumns == []
    assert detail.tableFeatures == ["appendOnly", "invariants"]
    assert spark.sql(f"DESC DETAIL sail.default.{name}").collect() == [detail]
    history = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 2").collect()
    assert [row.version for row in history] == [1, 0]
    assert spark.sql(f"DESCRIBE HISTORY {name} LIMIT 0").collect() == []

    spark.sql(f"INSERT OVERWRITE {name} VALUES (3, 'new')")  # noqa: S608
    current = [action["add"] for action in _actions(path, 2) if "add" in action]
    assert spark.sql(f"DESCRIBE DETAIL {name}").first().numFiles == len(current)
    assert spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first().version == 2  # noqa: PLR2004


@pytest.mark.parametrize(
    "options",
    [
        "metadataAsDataRead 'true'",
        "readChangeFeed 'true'",
        "metadataAsDataRead 'true', readChangeFeed 'true'",
    ],
    ids=["metadata-read", "change-feed-read", "both-read-modes"],
)
def test_inspection_ignores_data_read_modes(spark, tmp_path, options):
    name = "delta_inspection_read_mode"
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' OPTIONS ({options})"
    ).collect()
    try:
        spark.range(3).coalesce(1).write.format("delta").mode("append").save(str(path))
        adds = [action["add"] for action in _actions(path, 1) if "add" in action]
        assert adds
        detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
        assert detail.numFiles == len(adds)
        assert detail.sizeInBytes == sum(add["size"] for add in adds)
        assert [row.version for row in spark.sql(f"DESCRIBE HISTORY {name}").collect()] == [1, 0]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("checkpoint_policy", ["classic", "v2"])
def test_checkpoint_state_and_retained_history(spark, inspection_table, checkpoint_policy):
    name, path = inspection_table
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES "
        f"('delta.checkpointInterval' = '1', 'delta.checkpointPolicy' = '{checkpoint_policy}')"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")  # noqa: S608
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2, 'b')")  # noqa: S608
    assert (path / "_delta_log" / "_last_checkpoint").exists()
    before = spark.sql(f"DESCRIBE DETAIL {name}").collect()
    latest = max(int(file.stem) for file in (path / "_delta_log").glob("*.json") if file.stem.isdigit())
    for file in (path / "_delta_log").glob("*.json"):
        if file.stem.isdigit() and int(file.stem) < latest:
            file.unlink()
    assert spark.sql(f"DESCRIBE DETAIL {name}").collect() == before
    # Checkpoints preserve state, not commitInfo; history must not synthesize earlier commits.
    assert [row.version for row in spark.sql(f"DESCRIBE HISTORY {name}").collect()] == [latest]


@pytest.mark.parametrize("local_timezone", ["UTC", "Asia/Shanghai"], indirect=True)
@pytest.mark.usefixtures("local_timezone")
def test_history_optional_fields(spark, tmp_path):
    path = tmp_path / "raw"
    _write_log(
        path,
        [
            [
                {"txn": {"appId": "job", "version": 1, "lastUpdated": 1700000000000}},
                {"domainMetadata": {"domain": "retained", "configuration": '{"value":0}', "removed": False}},
            ],
            [
                {"txn": {"appId": "job", "version": 2}},
                {"domainMetadata": {"domain": "custom", "configuration": '{"value":1}', "removed": False}},
                {"domainMetadata": {"domain": "retained", "configuration": '{"value":2}', "removed": False}},
                {
                    "commitInfo": {
                        "timestamp": 1,
                        "operation": "WRITE",
                        "operationParameters": {"mode": "Append", "partitionBy": ["x"], "count": 1, "empty": None},
                        "job": {"jobId": "j", "runId": "r"},
                        "notebook": {"notebookId": "n"},
                        "clusterId": "c",
                        "operationMetrics": {"numOutputRows": "1"},
                        "userMetadata": "audit",
                        "engineInfo": "fixture",
                    }
                },
            ],
            [{"domainMetadata": {"domain": "custom", "configuration": "{}", "removed": True}}],
        ],
        protocol={"minReaderVersion": 1, "minWriterVersion": 7, "writerFeatures": ["domainMetadata"]},
    )
    spark.sql(f"CREATE TABLE raw_inspection USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        history = spark.sql("DESCRIBE HISTORY raw_inspection").collect()
        assert [row.version for row in history] == [2, 1, 0]
        assert [row.timestamp for row in reversed(history)] == [
            datetime.fromtimestamp(1700000000 + offset / 1000, tz=timezone.utc).astimezone().replace(tzinfo=None)
            for offset in range(3)
        ]
        assert history[0].operation is None
        assert history[1].job.jobId == "j"
        assert history[1].job.jobName is None
        assert history[1].notebook.notebookId == "n"
        assert history[1].operationParameters == {"mode": "Append", "partitionBy": '["x"]', "count": "1", "empty": None}
        assert history[1].operationMetrics == {"numOutputRows": "1"}
        assert history[1].userMetadata == "audit"
        limited = spark.sql("DESCRIBE HISTORY raw_inspection LIMIT 1").first()
        assert limited.version == 2  # noqa: PLR2004
        assert limited.timestamp == history[-1].timestamp
        assert spark.sql("DESCRIBE DETAIL raw_inspection").first().tableFeatures == ["domainMetadata"]
    finally:
        spark.sql("DROP TABLE raw_inspection")


@pytest.mark.parametrize(
    "relation",
    [
        "detail",
        "history",
        "files",
        "tombstones",
        "metadata",
        "protocol",
        "transactions",
        "domain_metadata",
        "partitions",
        "changes",
        "log_entries",
        "checkpoints",
        "sidecars",
        "checksums",
        "log_compactions",
    ],
)
def test_custom_metadata_relations_are_not_registered(spark, inspection_table, relation):
    name, _ = inspection_table
    with pytest.raises(Exception, match=r"(?i)(not found|does not exist|table_or_view_not_found)"):
        spark.table(f"{name}.{relation}").collect()


@pytest.mark.parametrize("namespace", ["delta", "sail.delta"])
@pytest.mark.parametrize(
    "procedure",
    [
        "describe_detail",
        "describe_history",
        "optimize",
        "vacuum",
        "restore",
        "convert_to_delta",
        "generate",
        "clone",
        "reorg",
        "upgrade_table_protocol",
        "add_feature_support",
        "drop_feature",
        "checkpoint",
        "compact_log",
    ],
)
def test_custom_procedures_are_not_registered(spark, inspection_table, namespace, procedure):
    name, path = inspection_table
    before = sorted(file.name for file in (path / "_delta_log").iterdir())
    with pytest.raises(Exception, match="Procedure not found"):
        spark.sql(f"CALL {namespace}.{procedure}('{name}')").collect()
    assert sorted(file.name for file in (path / "_delta_log").iterdir()) == before


@pytest.mark.parametrize("command", ["DESCRIBE DETAIL", "DESCRIBE HISTORY", "OPTIMIZE", "VACUUM"])
@pytest.mark.parametrize("prefix", ["", "./"], ids=["relative", "dot-relative"])
def test_native_delta_command_keeps_relative_string_target_as_path(spark, tmp_path, command, prefix):
    # Spark 4.2.0 + Delta 4.4.0 treat string targets as paths, not catalog identifiers.
    path = tmp_path / "relative_delta_target"
    spark.range(2).coalesce(1).write.format("delta").save(str(path))
    relative = prefix + os.path.relpath(path)
    absolute = escape_sql_string_literal(str(path))
    suffix = " DRY RUN" if command == "VACUUM" else ""
    actual = spark.sql(f"{command} '{escape_sql_string_literal(relative)}'{suffix}").collect()
    if command == "OPTIMIZE":
        assert len(actual) == 1
        assert actual[0].metrics.numFilesRemoved == 0
    else:
        expected = spark.sql(f"{command} '{absolute}'{suffix}").collect()
        assert actual == expected
    # The same relative text in a delta identifier remains a catalog lookup.
    identifier = relative.replace("`", "``")
    with pytest.raises(Exception, match=r"(?i)(not found|does not exist)"):
        spark.sql(f"{command} delta.`{identifier}`{suffix}").collect()
    assert [row.id for row in spark.read.format("delta").load(str(path)).orderBy("id").collect()] == [0, 1]


def test_native_delta_commands_resolve_delta_namespace_table(spark, tmp_path):
    target = "delta.`relative-table`"
    path = tmp_path / "catalog_target"
    spark.sql("CREATE DATABASE delta")
    try:
        spark.sql(f"CREATE TABLE {target} (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(str(path))}'")
        spark.range(2).coalesce(1).write.format("delta").mode("append").save(str(path))
        spark.range(2, 4).coalesce(1).write.format("delta").mode("append").save(str(path))
        detail = spark.sql(f"DESCRIBE DETAIL {target}").first()
        assert detail.name == "sail.delta.relative-table"
        assert detail.numFiles == 2  # noqa: PLR2004
        assert [row.version for row in spark.sql(f"DESCRIBE HISTORY {target}").collect()] == [2, 1, 0]
        metrics = spark.sql(f"OPTIMIZE {target}").first().metrics
        assert metrics.numFilesRemoved == 2  # noqa: PLR2004
        assert metrics.numFilesAdded == 1
        assert spark.sql(f"VACUUM {target} DRY RUN").collect() == []
        assert [row.id for row in spark.table(f"sail.{target}").orderBy("id").collect()] == [0, 1, 2, 3]
    finally:
        spark.sql("DROP DATABASE delta CASCADE")


def test_native_target_resolution(spark, inspection_table):
    name, _ = inspection_table
    spark.sql("CREATE TEMPORARY VIEW inspection_view AS SELECT 1 AS id")
    try:
        with pytest.raises(Exception, match=r"(not a table|not found|does not exist)"):
            spark.sql("DESCRIBE DETAIL inspection_view").collect()
    finally:
        spark.sql("DROP VIEW inspection_view")
    spark.sql("CREATE TABLE inspection_parquet (id INT) USING parquet")
    try:
        with pytest.raises(Exception, match=r"cannot target table.*format 'parquet'"):
            spark.sql("DESCRIBE DETAIL inspection_parquet").collect()
    finally:
        spark.sql("DROP TABLE inspection_parquet")
    # A real table named history remains an ordinary catalog lookup.
    spark.sql(f"CREATE DATABASE {name}")
    try:
        spark.sql(f"CREATE TABLE {name}.history (marker INT) USING parquet")
        spark.sql(f"INSERT INTO {name}.history VALUES (42)")  # noqa: S608
        assert spark.table(f"{name}.history").first().marker == 42  # noqa: PLR2004
    finally:
        spark.sql(f"DROP DATABASE {name} CASCADE")


def test_inspection_does_not_read_data_files(spark, inspection_table):
    name, path = inspection_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")  # noqa: S608
    before = spark.sql(f"DESCRIBE DETAIL {name}").collect()
    for file in path.rglob("*.parquet"):
        if "_delta_log" not in file.parts:
            file.unlink()
    assert spark.sql(f"DESCRIBE DETAIL {name}").collect() == before
    assert [row.version for row in spark.sql(f"DESCRIBE HISTORY {name}").collect()] == [1, 0]


@pytest.mark.yamlsnapshot(group="plan")
def test_inspection_plans(spark, inspection_table, snapshot):
    name, _ = inspection_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")  # noqa: S608
    statements = [
        f"DESCRIBE HISTORY {name} LIMIT 1",
        f"DESCRIBE DETAIL {name}",
    ]
    plans = {}
    for statement in statements:
        plan = "\n".join(row[0] for row in spark.sql(f"EXPLAIN EXTENDED {statement}").collect())
        plans[statement] = normalize_plan_text(plan)
    snapshot.assert_match(plans)


@pytest.mark.yamlsnapshot(group="plan")
def test_inspection_distributed_plan(tmp_path, snapshot):
    with (
        spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        path = tmp_path / "distributed"
        spark.sql(
            f"CREATE TABLE delta_distributed (id BIGINT) USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'"
        )
        try:
            statement = "DESCRIBE HISTORY delta_distributed"
            plan = spark.sql(f"EXPLAIN CODEGEN {statement}").first().plan
            distributed = plan[plan.index("== Distributed Plan ==") :]
            assert normalize_plan_text(distributed) == snapshot
            assert spark.sql(statement).first().version == 0
        finally:
            spark.sql("DROP TABLE delta_distributed")


@pytest.mark.parametrize("ict", [False, True])
def test_inspection_matches_delta_spark(spark, delta_jvm_spark, tmp_path, ict):
    path = tmp_path / "interop"
    jvm = delta_jvm_spark
    # Both engines inspect exactly the same log, including JVM-written metrics and checkpoints.
    writer = jvm.range(4).write.format("delta").option("delta.checkpointInterval", "1")
    if ict:
        writer = writer.option("delta.enableInCommitTimestamps", "true")
    writer.save(str(path))
    jvm.range(4, 6).write.format("delta").mode("append").option("userMetadata", "interop").save(str(path))
    spark.sql(f"CREATE TABLE inspection_interop USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        expected_history = jvm.sql(f"DESCRIBE HISTORY delta.`{path}`")
        actual_history = spark.sql("DESCRIBE HISTORY inspection_interop")
        assert actual_history.schema == expected_history.schema
        assert actual_history.collect() == expected_history.collect()
        expected_detail = jvm.sql(f"DESCRIBE DETAIL delta.`{path}`")
        actual_detail = spark.sql("DESCRIBE DETAIL inspection_interop")
        assert actual_detail.schema == expected_detail.schema
        expected = expected_detail.first().asDict(recursive=True)
        actual = actual_detail.first().asDict(recursive=True)
        assert actual.pop("name") == "sail.default.inspection_interop"
        expected.pop("name")
        assert unquote(urlparse(actual.pop("location")).path) == unquote(urlparse(expected.pop("location")).path)
        assert actual == expected
        # A Sail append remains visible to both inspection implementations and the JVM reader.
        spark.sql("INSERT INTO inspection_interop VALUES (6)")
        assert (
            spark.sql("DESCRIBE HISTORY inspection_interop").collect()
            == jvm.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
        )
        assert jvm.read.format("delta").load(str(path)).count() == 7  # noqa: PLR2004
    finally:
        spark.sql("DROP TABLE inspection_interop")


def test_detail_preserves_mapping_and_deletion_vectors(spark, delta_jvm_spark, tmp_path):
    path = tmp_path / "mapped_dv"
    jvm = delta_jvm_spark
    jvm.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, part STRING) USING delta PARTITIONED BY (part) "
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')"
    )
    jvm.sql(f"INSERT INTO delta.`{path}` VALUES (1, 'a'), (2, 'a'), (3, 'a')")  # noqa: S608
    jvm.sql(f"DELETE FROM delta.`{path}` WHERE id = 1")  # noqa: S608
    spark.sql(f"CREATE TABLE inspection_mapped USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        current = [action["add"] for action in _actions(path, 2) if "add" in action]
        assert current
        assert any(add.get("deletionVector") for add in current)
        expected = jvm.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        actual = spark.sql("DESCRIBE DETAIL inspection_mapped").first()
        assert actual.numFiles == expected.numFiles == len(current)
        assert actual.sizeInBytes == expected.sizeInBytes == sum(add["size"] for add in current)
        assert actual.partitionColumns == expected.partitionColumns == ["part"]
        assert actual.tableFeatures == expected.tableFeatures
        assert spark.table("inspection_mapped").count() == 2  # noqa: PLR2004
    finally:
        spark.sql("DROP TABLE inspection_mapped")


def test_detail_resolves_logical_clustering_columns(spark, delta_jvm_spark, tmp_path):
    path = tmp_path / "clustered"
    jvm = delta_jvm_spark
    jvm.sql(
        f"CREATE TABLE delta.`{path}` (id BIGINT, `sp ace` STRING) USING delta CLUSTER BY (`sp ace`) "
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    spark.sql(f"CREATE TABLE inspection_clustered USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        expected = jvm.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
        actual = spark.sql("DESCRIBE DETAIL inspection_clustered").first()
        assert actual.clusteringColumns == expected.clusteringColumns == ["`sp ace`"]
        assert actual.tableFeatures == expected.tableFeatures
    finally:
        spark.sql("DROP TABLE inspection_clustered")
