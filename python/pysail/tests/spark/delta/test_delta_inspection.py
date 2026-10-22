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


def test_state_relations_and_procedures(spark, inspection_table):
    name, path = inspection_table
    initial = _actions(path, 0)
    protocol = next(action["protocol"] for action in initial if "protocol" in action)
    metadata = next(action["metaData"] for action in initial if "metaData" in action)
    assert spark.table(f"{name}.files").collect() == []
    assert spark.table(f"{name}.tombstones").collect() == []
    assert spark.table(f"{name}.transactions").collect() == []
    assert spark.table(f"{name}.domain_metadata").collect() == []
    assert spark.table(f"{name}.MeTaDaTa").first().asDict(recursive=True) == metadata
    actual_protocol = spark.table(f"{name}.PROTOCOL").first().asDict()
    assert {key: value for key, value in actual_protocol.items() if value is not None} == protocol

    spark.sql(f"INSERT INTO {name} VALUES (1, 'a b'), (2, NULL)")  # noqa: S608
    files = spark.table(f"{name}.files").collect()
    adds = [action["add"] for action in _actions(path, 1) if "add" in action]
    assert sorted(row.path for row in files) == sorted(add["path"] for add in adds)
    assert {row.partitionValues["part"] for row in files} == {"a b", None}
    assert sum(row.size for row in files) == sum(add["size"] for add in adds)
    assert all(json.loads(row.stats)["numRecords"] == 1 for row in files)
    assert spark.sql(f"SELECT count(*) FROM {name}.files").first()[0] == len(files)  # noqa: S608
    filtered = spark.sql(f"SELECT path FROM {name}.files WHERE partitionValues['part'] IS NULL LIMIT 1")  # noqa: S608
    assert filtered.first().path in {row.path for row in files if row.partitionValues["part"] is None}

    detail = spark.table(f"{name}.detail").first()
    assert detail.numFiles == len(files)
    assert detail.sizeInBytes == sum(row.size for row in files)
    assert detail.partitionColumns == ["part"]
    assert detail.clusteringColumns == []
    assert detail.tableFeatures == ["appendOnly", "invariants"]
    assert spark.sql(f"CALL delta.describe_detail('{name}')").collect() == [detail]
    history = spark.sql(f"CALL sail.delta.describe_history(table => '{name}', limit => 1 + 1)").collect()
    assert [row.version for row in history] == [1, 0]
    assert spark.sql(f"CALL delta.describe_history('{name}', 0)").collect() == []
    assert history == spark.table(f"sail.default.{name}.HiStOrY").orderBy("version", ascending=False).collect()
    assert spark.sql(
        f"SELECT d.numFiles, count(f.path) AS actual FROM {name}.detail d CROSS JOIN {name}.files f GROUP BY d.numFiles"  # noqa: S608
    ).first().asDict() == {"numFiles": len(files), "actual": len(files)}

    spark.sql(f"INSERT OVERWRITE {name} VALUES (3, 'new')")  # noqa: S608
    assert {row.path for row in spark.table(f"{name}.tombstones").collect()} == {row.path for row in files}
    assert {row.partitionValues["part"] for row in spark.table(f"{name}.files").collect()} == {"new"}
    assert spark.sql(f"CALL delta.describe_history('{name}', 1)").first().version == 2  # noqa: PLR2004


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
    before_files = spark.table(f"{name}.files").orderBy("path").collect()
    before_tombstones = spark.table(f"{name}.tombstones").select("path", "deletionVector").orderBy("path").collect()
    latest = max(int(file.stem) for file in (path / "_delta_log").glob("*.json") if file.stem.isdigit())
    for file in (path / "_delta_log").glob("*.json"):
        if file.stem.isdigit() and int(file.stem) < latest:
            file.unlink()
    assert spark.table(f"{name}.files").orderBy("path").collect() == before_files
    assert (
        spark.table(f"{name}.tombstones").select("path", "deletionVector").orderBy("path").collect()
        == before_tombstones
    )
    # Checkpoints preserve state, not commitInfo; history must not synthesize earlier commits.
    assert [row.version for row in spark.table(f"{name}.history").collect()] == [latest]


@pytest.mark.parametrize("local_timezone", ["UTC", "Asia/Shanghai"], indirect=True)
@pytest.mark.usefixtures("local_timezone")
def test_history_optional_fields_and_reconciled_actions(spark, tmp_path):
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
        history = spark.sql("CALL delta.describe_history('raw_inspection')").collect()
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
        assert spark.table("raw_inspection.transactions").first().asDict() == {
            "appId": "job",
            "version": 2,
            "lastUpdated": None,
        }
        assert [row.asDict() for row in spark.table("raw_inspection.domain_metadata").collect()] == [
            {"domain": "retained", "configuration": '{"value":2}', "removed": False},
        ]
        limited = spark.sql("CALL delta.describe_history('raw_inspection', 1)").first()
        assert limited.version == 2  # noqa: PLR2004
        assert limited.timestamp == history[-1].timestamp
        assert spark.table("raw_inspection.detail").first().tableFeatures == ["domainMetadata"]
    finally:
        spark.sql("DROP TABLE raw_inspection")


@pytest.mark.parametrize(
    ("relation", "action", "checksum_field", "value"),
    [
        ("transactions", "txn", "setTransactions", {"appId": "writer", "version": 7, "lastUpdated": 1700000000000}),
        (
            "domain_metadata",
            "domainMetadata",
            "domainMetadata",
            {"domain": "custom", "configuration": '{"value":1}', "removed": False},
        ),
    ],
)
@pytest.mark.parametrize("include_optional_state", [False, True], ids=["omitted", "included"])
@pytest.mark.parametrize("newer_commit", [False, True], ids=["current-crc", "older-crc"])
def test_state_relations_replay_optional_checksum_fields(
    spark, tmp_path, relation, action, checksum_field, value, include_optional_state, newer_commit
):
    path = tmp_path / "optional_checksum_state"
    commits = [[{action: value}]]
    if newer_commit:
        commits.append([{"commitInfo": {"operation": "SET TBLPROPERTIES"}}])
    _write_log(
        path,
        commits,
        protocol={"minReaderVersion": 1, "minWriterVersion": 7, "writerFeatures": ["domainMetadata"]},
    )
    actions = _actions(path, 0)
    checksum = {
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": next(item["metaData"] for item in actions if "metaData" in item),
        "protocol": next(item["protocol"] for item in actions if "protocol" in item),
    }
    if include_optional_state:
        checksum[checksum_field] = [value]
    # Delta 4.4 permits these CRC fields to be omitted even when the log has live actions.
    (path / "_delta_log" / f"{0:020d}.crc").write_text(json.dumps(checksum))
    spark.sql(f"CREATE TABLE inspection_crc_state USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        actual = [row.asDict(recursive=True) for row in spark.table(f"inspection_crc_state.{relation}").collect()]
        assert actual == [value], f"{relation} must retain log state regardless of optional CRC fields"
    finally:
        spark.sql("DROP TABLE inspection_crc_state")


@pytest.mark.parametrize(
    "relation", ["partitions", "changes", "log_entries", "checkpoints", "sidecars", "checksums", "log_compactions"]
)
def test_recognized_relations_report_unsupported(spark, inspection_table, relation):
    name, _ = inspection_table
    with pytest.raises(Exception, match=f"Delta metadata table '{relation}' is recognized but not implemented"):
        spark.table(f"{name}.{relation}").collect()


@pytest.mark.parametrize(
    "procedure",
    [
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
def test_recognized_procedures_report_unsupported(spark, procedure):
    with pytest.raises(Exception, match=f"Delta procedure '{procedure}' is recognized but not implemented"):
        spark.sql(f"CALL delta.{procedure}()").collect()


def test_resolution_and_argument_validation(spark, inspection_table):
    name, _ = inspection_table
    for call, message in [
        ("CALL delta.unknown()", "Procedure not found"),
        ("CALL missing.delta.describe_detail()", "Catalog not found"),
        ("CALL delta.describe_detail()", "Missing required argument"),
        (f"CALL delta.describe_history('{name}', -1)", "non-negative integer"),
        (f"CALL delta.describe_detail(table => '{name}', typo => 1)", "Unknown argument"),
        (
            f"CALL delta.describe_detail('{name}', table => '{name}')",
            "(Duplicate|multiple|already|provided more than once)",
        ),
    ]:
        with pytest.raises(Exception, match=message):
            spark.sql(call).collect()
    spark.sql("CREATE TEMPORARY VIEW inspection_view AS SELECT 1 AS id")
    with pytest.raises(Exception, match=r"(not a table|not found|does not exist)"):
        spark.sql("CALL delta.describe_detail('inspection_view')").collect()
    spark.sql("DROP VIEW inspection_view")
    with pytest.raises(Exception, match=r"(?i)(not found|table_or_view_not_found)"):
        spark.table(f"{name}.unknown").collect()
    with pytest.raises(Exception, match="time travel is not supported"):
        spark.sql(f"SELECT * FROM {name}.history VERSION AS OF 0").collect()  # noqa: S608
    spark.sql("CREATE TABLE inspection_parquet (id INT) USING parquet")
    try:
        with pytest.raises(Exception, match=r"cannot target table.*format 'parquet'"):
            spark.sql("CALL delta.describe_detail('inspection_parquet')").collect()
    finally:
        spark.sql("DROP TABLE inspection_parquet")
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
    files = spark.table(f"{name}.files").orderBy("path").collect()
    for file in path.rglob("*.parquet"):
        if "_delta_log" not in file.parts:
            file.unlink()
    assert spark.table(f"{name}.files").orderBy("path").collect() == files
    assert spark.sql(f"CALL delta.describe_detail('{name}')").first().numFiles == len(files)
    assert [row.version for row in spark.sql(f"CALL delta.describe_history('{name}')").collect()] == [1, 0]


@pytest.mark.yamlsnapshot(group="plan")
def test_inspection_plans(spark, inspection_table, snapshot):
    name, _ = inspection_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')")  # noqa: S608
    statements = [
        f"CALL delta.describe_history('{name}', 1)",
        f"SELECT count(*) FROM {name}.files WHERE size > 0",  # noqa: S608
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
            statement = "CALL delta.describe_history('delta_distributed')"
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


def test_file_metadata_preserves_mapping_and_deletion_vectors(spark, delta_jvm_spark, tmp_path):
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
        files = spark.table("inspection_mapped.files").collect()
        assert len(files) == len(current)
        for row, add in zip(
            sorted(files, key=lambda row: row.path), sorted(current, key=lambda add: add["path"]), strict=True
        ):
            assert row.path == add["path"]
            assert row.partitionValues == add["partitionValues"]
            assert "part" not in row.partitionValues
            assert row.deletionVector.asDict() == add["deletionVector"]
            assert json.loads(row.stats) == json.loads(add["stats"])
        assert spark.table("inspection_mapped.detail").first().partitionColumns == ["part"]
        assert spark.table("inspection_mapped.tombstones").count() == 1
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
        actual = spark.table("inspection_clustered.detail").first()
        assert actual.clusteringColumns == expected.clusteringColumns == ["`sp ace`"]
        assert actual.tableFeatures == expected.tableFeatures
        domain = spark.table("inspection_clustered.domain_metadata").where("domain = 'delta.clustering'").first()
        assert json.loads(domain.configuration)["clusteringColumns"] != [["sp ace"]]
    finally:
        spark.sql("DROP TABLE inspection_clustered")
