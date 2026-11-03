"""Tests for the OpenLineage events emitted by the Spark Connect server.

Each test starts its own server with the object store transport writing to a temporary
directory, so that the events of the test are isolated, and all events are delivered
when the server is stopped.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import pyspark_version

if TYPE_CHECKING:
    from pathlib import Path

PARENT_RUN_ID = str(uuid.uuid5(uuid.NAMESPACE_URL, "dag/task/run/1"))


def lineage_server(path: Path):
    return spark_connect_server(
        {
            "SAIL_LINEAGE__ENABLED": "true",
            "SAIL_LINEAGE__TRANSPORT": "object_store",
            "SAIL_LINEAGE__OBJECT_STORE__URL": f"{path.as_uri()}/",
        }
    )


def read_events(path: Path) -> list[dict]:
    # The file names are `<millis>_<sequence>_<uuid>.json`, where the sequence is the emission order.
    files = sorted(path.iterdir(), key=lambda x: int(x.name.split("_")[1]))
    return [json.loads(x.read_text()) for x in files]


def job_type(event: dict) -> str:
    return event["job"]["facets"]["jobType"]["jobType"]


def summary(events: list[dict]) -> list[tuple[str, str]]:
    return [(x["eventType"], job_type(x)) for x in events]


def assert_runs_closed(events: list[dict]):
    started = {x["run"]["runId"] for x in events if x["eventType"] == "START"}
    finished = {x["run"]["runId"] for x in events if x["eventType"] in ("COMPLETE", "FAIL", "ABORT")}
    assert started == finished


def test_query_runs_are_children_of_the_application_run(tmp_path):
    data = tmp_path / "data"
    with lineage_server(tmp_path / "events") as server:
        spark = (
            SparkSession.builder.remote(server.remote)
            .appName("lineage_test")
            .config("spark.openlineage.namespace", "test_namespace")
            .config("spark.openlineage.parentJobNamespace", "airflow")
            .config("spark.openlineage.parentJobName", "dag.task")
            .config("spark.openlineage.parentRunId", PARENT_RUN_ID)
            .config("spark.openlineage.job.tags", "dag_id:dag;task_id:task")
            .config("spark.openlineage.run.tags", "try_number:1")
            .create()
        )
        spark.range(3).write.parquet(str(data))
        assert spark.read.parquet(str(data)).count() == 3  # noqa: PLR2004
        spark.stop()

    events = read_events(tmp_path / "events")
    assert summary(events) == [
        ("START", "APPLICATION"),
        ("START", "WRITE"),
        ("COMPLETE", "WRITE"),
        ("START", "QUERY"),
        ("COMPLETE", "QUERY"),
        ("COMPLETE", "APPLICATION"),
    ]
    assert_runs_closed(events)

    application = events[0]
    assert application["job"]["namespace"] == "test_namespace"
    assert application["job"]["name"] == "lineage_test"
    parent = application["run"]["facets"]["parent"]
    assert parent["job"] == {"namespace": "airflow", "name": "dag.task"}
    assert parent["run"]["runId"] == PARENT_RUN_ID

    for event in events[1:5]:
        parent = event["run"]["facets"]["parent"]
        assert parent["run"]["runId"] == application["run"]["runId"]
        assert parent["root"]["run"]["runId"] == PARENT_RUN_ID
        assert event["job"]["namespace"] == "test_namespace"

    for event in events:
        tags = {(x["key"], x["value"]) for x in event["job"]["facets"]["tags"]["tags"]}
        assert tags == {("dag_id", "dag"), ("task_id", "task")}
        tags = {(x["key"], x["value"]) for x in event["run"]["facets"]["tags"]["tags"]}
        assert tags == {("try_number", "1")}

    [output] = events[1]["outputs"]
    assert (output["namespace"], output["name"]) == ("file", str(data))
    assert output["facets"]["lifecycleStateChange"]["lifecycleStateChange"] == "CREATE"
    [read] = events[3]["inputs"]
    assert (read["namespace"], read["name"]) == ("file", str(data))
    assert [x["name"] for x in read["facets"]["schema"]["fields"]] == ["id"]


def test_failed_query_emits_fail_event(tmp_path):
    data = tmp_path / "data"
    with lineage_server(tmp_path / "events") as server:
        spark = SparkSession.builder.remote(server.remote).create()
        spark.range(3).write.parquet(str(data))
        with pytest.raises(Exception, match="is not true"):
            spark.read.parquet(str(data)).selectExpr("assert_true(id < 0)").collect()
        spark.stop()

    events = read_events(tmp_path / "events")
    assert summary(events)[-3:] == [("START", "QUERY"), ("FAIL", "QUERY"), ("COMPLETE", "APPLICATION")]
    assert "is not true" in events[-2]["run"]["facets"]["errorMessage"]["message"]
    assert_runs_closed(events)


def test_open_sessions_are_completed_on_shutdown(tmp_path):
    with lineage_server(tmp_path / "events") as server:
        spark = SparkSession.builder.remote(server.remote).create()
        spark.range(3).write.parquet(str(tmp_path / "data"))
        # The session is not stopped before the server.

    events = read_events(tmp_path / "events")
    assert summary(events) == [
        ("START", "APPLICATION"),
        ("START", "WRITE"),
        ("COMPLETE", "WRITE"),
        ("COMPLETE", "APPLICATION"),
    ]
    assert_runs_closed(events)


def test_queries_without_datasets_do_not_emit_events(tmp_path):
    with lineage_server(tmp_path / "events") as server:
        spark = SparkSession.builder.remote(server.remote).create()
        assert spark.sql("SELECT 1").collect()[0][0] == 1
        spark.stop()

    assert not (tmp_path / "events").exists() or read_events(tmp_path / "events") == []


@pytest.mark.skipif(pyspark_version() < (4, 1), reason="Python data source requires Spark 4.1+")
def test_jdbc_inputs_are_tables_without_credentials(tmp_path):
    pytest.importorskip("connectorx")
    from pysail.spark.datasource.jdbc import JdbcDataSource

    secret = "s3cret-value"  # noqa: S105
    # The invalid `sslmode` makes the read fail immediately, after the lineage run is started.
    url = f"jdbc:postgresql://alice:{secret}@127.0.0.1:5432/sales?password={secret}&sslmode=invalid"
    dbtable = (
        "(SELECT o.id FROM orders o JOIN crm.customers c ON o.customer_id = c.id "
        "WHERE o.id NOT IN (SELECT order_id FROM refunds)) AS t"
    )
    with lineage_server(tmp_path / "events") as server:
        spark = SparkSession.builder.remote(server.remote).create()
        spark.dataSource.register(JdbcDataSource)
        df = (
            spark.read.format("jdbc")
            # The schema is specified so that the data source does not connect during planning.
            .schema("id INT")
            .option("url", url)
            .option("user", "alice")
            .option("password", secret)
            .option("dbtable", dbtable)
            .load()
        )
        with pytest.raises(Exception, match="sslmode"):
            df.collect()
        spark.stop()

    events = read_events(tmp_path / "events")
    assert summary(events) == [
        ("START", "APPLICATION"),
        ("START", "QUERY"),
        ("FAIL", "QUERY"),
        ("COMPLETE", "APPLICATION"),
    ]
    for event in events[1:3]:
        inputs = [(x["namespace"], x["name"]) for x in event["inputs"]]
        assert inputs == [
            ("postgres://127.0.0.1:5432", "sales.public.orders"),
            ("postgres://127.0.0.1:5432", "sales.crm.customers"),
            ("postgres://127.0.0.1:5432", "sales.public.refunds"),
        ]
        for dataset in event["inputs"]:
            assert dataset["facets"]["dataSource"]["uri"] == "postgres://127.0.0.1:5432"
    for path in (tmp_path / "events").iterdir():
        text = path.read_text()
        assert secret not in text
        assert "alice" not in text
