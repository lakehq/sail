import uuid

import grpc
import pyspark.sql.connect.functions as F  # noqa: N812
import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql.connect import proto as pb
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.fixture(scope="module")
def fail():
    @F.udf("long")
    def _fail(_value):
        msg = "expected failure"
        raise RuntimeError(msg)

    return _fail


@pytest.mark.skipif(is_jvm_spark(), reason="Sail only")
def test_execution_error_without_reattachment(remote, fail):
    """Error handling for reattachable execution (the default) is exercised frequently in other tests.
    This test ensures that the error can still be propagated for (non-default) non-reattachable execution.
    """
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        spark._client.disable_reattachable_execute()  # noqa: SLF001
        with pytest.raises(SparkConnectGrpcException, match="expected failure"):
            spark.range(1).select(fail("id")).write.format("noop").mode("overwrite").save()
        with pytest.raises(SparkConnectGrpcException, match="expected failure"):
            spark.range(1).select(fail("id")).collect()


@pytest.fixture
def raw_execution(spark):
    client = spark.client
    stub = client._stub  # noqa: SLF001
    request = client._execute_plan_request_with_metadata()  # noqa: SLF001
    request.operation_id = str(uuid.uuid4())
    request.request_options.add().reattach_options.reattachable = True
    metadata = client._builder.metadata()  # noqa: SLF001
    try:
        yield stub, request, metadata
    finally:
        stub.ReleaseExecute(
            pb.ReleaseExecuteRequest(
                session_id=request.session_id,
                user_context=request.user_context,
                operation_id=request.operation_id,
                release_all=pb.ReleaseExecuteRequest.ReleaseAll(),
            ),
            metadata=metadata,
        )


@pytest.mark.parametrize("plan_kind", ["sql_query", "sql_command", "relation"])
def test_completed_execution_responses_can_be_replayed(spark, raw_execution, plan_kind):
    stub, request, metadata = raw_execution
    view = f"replay_{uuid.uuid4().hex}"
    if plan_kind == "sql_query":
        request.plan.command.sql_command.sql = "SELECT 1 AS value"
    elif plan_kind == "sql_command":
        request.plan.command.sql_command.sql = f"CREATE TEMPORARY VIEW {view} AS SELECT 1 AS value"
    else:
        request.plan.root.range.CopyFrom(pb.Range(start=0, end=3, step=1, num_partitions=1))
    reattach = pb.ReattachExecuteRequest(
        session_id=request.session_id,
        user_context=request.user_context,
        operation_id=request.operation_id,
    )
    try:
        responses = list(stub.ExecutePlan(request, metadata=metadata))
        assert responses[-1].HasField("result_complete")
        assert len({response.response_id for response in responses}) == len(responses)
        assert all(response.session_id == request.session_id for response in responses)
        assert all(response.operation_id == request.operation_id for response in responses)
        if plan_kind.startswith("sql_"):
            assert any(response.HasField("sql_command_result") for response in responses)
        else:
            assert any(response.HasField("arrow_batch") for response in responses)

        # A completed operation retains both the payloads and their response IDs.
        assert list(stub.ReattachExecute(reattach, metadata=metadata)) == responses
        assert list(stub.ReattachExecute(reattach, metadata=metadata)) == responses

        stub.ReleaseExecute(
            pb.ReleaseExecuteRequest(
                session_id=request.session_id,
                user_context=request.user_context,
                operation_id=request.operation_id,
                release_until=pb.ReleaseExecuteRequest.ReleaseUntil(response_id=responses[0].response_id),
            ),
            metadata=metadata,
        )
        reattach.last_response_id = responses[0].response_id
        assert list(stub.ReattachExecute(reattach, metadata=metadata)) == responses[1:]
        assert list(stub.ReattachExecute(reattach, metadata=metadata)) == responses[1:]
        reattach.last_response_id = responses[-1].response_id
        assert list(stub.ReattachExecute(reattach, metadata=metadata)) == []

        # Replaying a command must not execute its side effects again.
        if plan_kind == "sql_command":
            assert spark.table(view).collect() == [Row(value=1)]
        stub.ReleaseExecute(
            pb.ReleaseExecuteRequest(
                session_id=request.session_id,
                user_context=request.user_context,
                operation_id=request.operation_id,
                release_all=pb.ReleaseExecuteRequest.ReleaseAll(),
            ),
            metadata=metadata,
        )
        with pytest.raises(grpc.RpcError):
            list(stub.ReattachExecute(reattach, metadata=metadata))
    finally:
        if plan_kind == "sql_command":
            spark.sql(f"DROP VIEW IF EXISTS {view}")


def test_sql_query_recovers_when_completion_response_is_lost(spark, monkeypatch):
    stub = spark.client._stub  # noqa: SLF001
    execute_plan = stub.ExecutePlan
    dropped = []

    def execute_without_sql_completion(request, **kwargs):
        responses = execute_plan(request, **kwargs)
        if not (request.plan.HasField("command") and request.plan.command.HasField("sql_command")):
            return responses

        def without_completion():
            try:
                for response in responses:
                    if response.HasField("result_complete"):
                        dropped.append(response.response_id)
                        return
                    yield response
            finally:
                responses.cancel()

        return without_completion()

    monkeypatch.setattr(stub, "ExecutePlan", execute_without_sql_completion)
    rows = spark.sql(
        "SELECT COUNT(DISTINCT *) AS n FROM VALUES (1, 1.0, 'a'), (2, 2.0, 'b'), (1, 1.0, 'a') AS t(a, b, c)"
    ).collect()
    assert rows == [Row(n=2)]
    assert len(dropped) == 1


def test_sql_query_without_reattachment_has_no_completion_response(raw_execution):
    stub, request, metadata = raw_execution
    request.request_options.clear()
    request.plan.command.sql_command.sql = "SELECT 1 AS value"
    responses = list(stub.ExecutePlan(request, metadata=metadata))
    assert any(response.HasField("sql_command_result") for response in responses)
    assert not any(response.HasField("result_complete") for response in responses)
