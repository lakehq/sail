import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path

import grpc
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pyspark.errors import PySparkException

from pysail.testing.spark.session import spark_connect_server, spark_session_factory


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(
        envs={
            "SAIL_MODE": "local-cluster",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "4",
        }
    ) as server:
        yield server.remote


@pytest.mark.timeout(60)
def test_scalar_cardinality_failure_preserves_original_error(spark):
    # Nonfailing keys produce rows in other output partitions. Retrying the
    # failed region must not mask the semantic error with an attempt conflict.
    with pytest.raises(
        PySparkException,
        match="SCALAR_SUBQUERY_TOO_MANY_ROWS: scalar subquery returned more than one row",
    ):
        spark.sql(
            """
            SELECT o.id, (SELECT i.v FROM VALUES (1L, 10), (2L, 20), (2L, 21) i(k, v)
                          WHERE i.k = o.id) AS v
            FROM range(0, 64, 1, 4) o
            """
        ).collect()
    assert sorted(row.id for row in spark.range(64, numPartitions=4).collect()) == list(range(64))


@pytest.mark.timeout(60)
@pytest.mark.parametrize("shuffle", [False, True], ids=["output-stage", "shuffle-stage"])
def test_transient_failure_retries_without_duplicate_rows(spark, tmp_path, shuffle):
    # A shared, atomic fault budget survives Python worker recreation. The only
    # input partition fails before emitting, and its next attempt must succeed.
    failure = str(tmp_path / "failed")
    success = str(tmp_path / "succeeded")

    def fail_once(batches):
        try:
            with Path(failure).open("x"):
                pass
        except FileExistsError:
            with Path(success).open("x"):
                pass
        else:
            msg = "injected transient task failure"
            raise OSError(msg)
        yield from batches

    query = spark.range(64, numPartitions=1).mapInPandas(fail_once, "id long")
    if shuffle:
        query = query.repartition(4, "id")
    rows = query.collect()
    assert Path(failure).is_file(), "the transient failure must actually fire"
    assert Path(success).is_file(), "a later task attempt must execute successfully"
    assert sorted(row.id for row in rows) == list(range(64))


@pytest.fixture
def flight_request_failure():
    # A partial descriptor preserves all unknown fields while replacing only the
    # worker's advertised endpoint (sail/driver/service.proto). Both control-plane
    # RPCs and the worker itself stay live when the first Flight request is refused.
    descriptor = descriptor_pb2.FileDescriptorProto(name="fault_proxy.proto", syntax="proto3")
    message = descriptor.message_type.add(name="RegisterWorkerRequest")
    message.field.add(name="host", number=3, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    message.field.add(name="port", number=4, type=descriptor_pb2.FieldDescriptorProto.TYPE_UINT32)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(descriptor)
    registration = message_factory.GetMessageClass(pool.FindMessageTypeByName("RegisterWorkerRequest"))
    failed = threading.Event()
    cleaned = threading.Event()
    forwarded = threading.Event()

    with ExitStack() as stack:
        executor = stack.enter_context(ThreadPoolExecutor(max_workers=12))

        def start_proxy(target, service, methods, overrides):
            channel = stack.enter_context(grpc.insecure_channel(target))
            server = grpc.server(executor)

            def forward(method):
                def call(request, context):
                    try:
                        return channel.unary_unary(f"/{service}/{method}")(request, timeout=10)
                    except grpc.RpcError as error:
                        context.abort(error.code(), error.details())

                return call

            handlers = {
                method: grpc.unary_unary_rpc_method_handler(overrides.get(method, forward(method)))
                for method in methods
            }
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service, handlers),))
            port = server.add_insecure_port("127.0.0.1:0")
            return server, channel, port

        def start_worker_proxy(host, port):
            service = "sail.worker.WorkerService"
            methods = ["RunTaskBatch", "StopTask", "CleanUpJob", "StopWorker"]

            def clean_up(request, context):
                try:
                    response = channel.unary_unary(f"/{service}/CleanUpJob")(request, timeout=10)
                except grpc.RpcError as error:
                    context.abort(error.code(), error.details())
                cleaned.set()
                return response

            server, channel, port = start_proxy(f"{host}:{port}", service, methods, {"CleanUpJob": clean_up})

            def do_get(request, context):
                if not failed.is_set():
                    failed.set()
                    context.abort(grpc.StatusCode.UNAVAILABLE, "injected final-output DoGet failure")
                forwarded.set()
                try:
                    yield from channel.unary_stream("/arrow.flight.protocol.FlightService/DoGet")(request, timeout=10)
                except grpc.RpcError as error:
                    context.abort(error.code(), error.details())

            server.add_generic_rpc_handlers(
                (
                    grpc.method_handlers_generic_handler(
                        "arrow.flight.protocol.FlightService", {"DoGet": grpc.unary_stream_rpc_method_handler(do_get)}
                    ),
                )
            )
            server.start()
            stack.callback(lambda: server.stop(None).wait())
            return port

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            driver_port = sock.getsockname()[1]

        def register(request, context):
            location = registration.FromString(request)
            location.port = start_worker_proxy(location.host, location.port)
            location.host = "127.0.0.1"
            try:
                return driver_channel.unary_unary("/sail.driver.DriverService/RegisterWorker")(
                    location.SerializeToString(), timeout=10
                )
            except grpc.RpcError as error:
                context.abort(error.code(), error.details())

        driver, driver_channel, proxy_port = start_proxy(
            f"127.0.0.1:{driver_port}",
            "sail.driver.DriverService",
            ["RegisterWorker", "ReportWorkerHeartbeat", "ReportWorkerKnownPeers", "ReportTaskStatus", "ReportMetrics"],
            {"RegisterWorker": register},
        )
        driver.start()
        stack.callback(lambda: driver.stop(None).wait())
        with spark_connect_server(
            envs={
                "SAIL_MODE": "local-cluster",
                "SAIL_CLUSTER__DRIVER_LISTEN_PORT": str(driver_port),
                "SAIL_CLUSTER__DRIVER_EXTERNAL_PORT": str(proxy_port),
                "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
                "SAIL_CLUSTER__WORKER_MAX_COUNT": "1",
                "SAIL_CLUSTER__WORKER_TASK_SLOTS": "1",
                "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1",
                "SAIL_EXECUTION__BATCH_SIZE": "256",
            }
        ) as server:
            yield server.remote, failed, cleaned, forwarded


@pytest.mark.timeout(30)
def test_output_fetch_failure_cleans_up_and_allows_later_queries(flight_request_failure):
    remote, failed, cleaned, forwarded = flight_request_failure
    with spark_session_factory(remote) as registry:
        spark = registry.create()
        with pytest.raises(PySparkException, match="injected final-output DoGet failure"):
            spark.range(10_000, numPartitions=1).collect()
        assert failed.is_set(), "the first Flight request must fail before subscribing"
        assert cleaned.wait(5), "output failure must clean up the blocked producer"
        assert not forwarded.is_set(), "the failed output must not be silently replayed"
        assert [row.id for row in spark.range(10_000, numPartitions=1).collect()] == list(range(10_000))
        assert forwarded.is_set(), "a later query must consume the worker's real Flight stream"
