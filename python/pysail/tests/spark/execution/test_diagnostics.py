import json
import time

from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

from pysail.spark import diagnostics


def test_distributed_explain_returns_a_versioned_typed_report(spark):
    dataframe = spark.range(16, numPartitions=4).repartition(2)
    spark_explain = ConnectDataFrame.explain

    report = diagnostics.explain(dataframe, format="json", verbose=True)

    assert ConnectDataFrame.explain is spark_explain
    assert report.schema_version == 1
    assert report.execution_mode == "local_cluster"
    assert not report.executed
    assert report.job_id is None
    assert report.metrics == {}
    assert report.stages
    assert report.edges
    assert all(stage.partitions > 0 for stage in report.stages)
    assert any(stage.placement == "worker" for stage in report.stages)
    assert any(stage.operator_tree for stage in report.stages)
    assert json.loads(report.text)["schema_version"] == 1


def test_distributed_explain_renders_text_and_graphviz(spark):
    dataframe = spark.range(8, numPartitions=2).repartition(2)

    text = diagnostics.explain(dataframe, verbose=True).text
    graphviz = diagnostics.explain(dataframe, format="graphviz").text

    assert text.startswith("Distributed Plan V1\n")
    assert "=== exchanges ===" in text
    assert "RangeExec" in text
    assert graphviz.startswith("digraph distributed_plan {")
    assert "stage_0 -> stage_1" in graphviz
    assert "RangeExec" not in graphviz


def test_distributed_explain_sql_supports_json_graphviz_and_analyze(spark):
    json_plan = (
        spark.sql(
            """
        EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON, VERBOSE TRUE)
        SELECT id, id * 2 AS value FROM range(0, 8, 1, 2)
        """
        )
        .first()
        .plan
    )
    graphviz = (
        spark.sql(
            """
        EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ)
        SELECT id FROM range(0, 8, 1, 2)
        """
        )
        .first()
        .plan
    )
    analyzed_plan = (
        spark.sql(
            """
        EXPLAIN (TYPE DISTRIBUTED, ANALYZE TRUE, FORMAT JSON)
        SELECT id FROM range(0, 8, 1, 2)
        """
        )
        .first()
        .plan
    )

    model = json.loads(json_plan)
    analyzed_model = json.loads(analyzed_plan)
    assert model["schema_version"] == 1
    assert any(stage["operator_tree"] for stage in model["stages"])
    assert graphviz.startswith("digraph distributed_plan {")
    assert analyzed_model["executed"]
    assert analyzed_model["execution"]["job_id"] >= 0


def test_distributed_explain_analyze_uses_a_system_table_job_id(spark):
    report = diagnostics.explain(
        spark.range(32, numPartitions=4).repartition(2).selectExpr("id * 2 AS value"),
        format="json",
        analyze=True,
    )

    assert report.executed
    assert report.job_id is not None

    statuses = []
    for _ in range(20):
        statuses = [
            row.status
            for row in spark.sql(
                f"SELECT status FROM system.execution.jobs WHERE job_id = {report.job_id}"  # noqa: S608
            ).collect()
        ]
        if "SUCCEEDED" in statuses:
            break
        time.sleep(0.05)
    assert "SUCCEEDED" in statuses

    stage_rows = spark.sql(
        "SELECT CAST(stage AS BIGINT) AS stage, CAST(partitions AS BIGINT) AS partitions "  # noqa: S608
        f"FROM system.execution.stages WHERE job_id = {report.job_id}"
    ).collect()
    task_rows = spark.sql(
        "SELECT CAST(stage AS BIGINT) AS stage, CAST(partition AS BIGINT) AS partition "  # noqa: S608
        f"FROM system.execution.tasks WHERE job_id = {report.job_id}"
    ).collect()
    expected_stages = {stage.id: stage.partitions for stage in report.stages}
    expected_tasks = {(stage.id, partition) for stage in report.stages for partition in range(stage.partitions)}

    assert {row.stage: row.partitions for row in stage_rows} == expected_stages
    assert expected_tasks <= {(row.stage, row.partition) for row in task_rows}
