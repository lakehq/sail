"""Flight protocol regressions that ADBC's fallback behavior can hide."""

import socket
import struct
import time

import grpc
import pyarrow as pa
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.any_pb2 import Any
from pyarrow import flight

from pysail.flight import FlightSqlServer
from pysail.tests.flight.conftest import local_flight_uri


def message_type(name, fields, *, optional_fields=()):
    """Define the wire fields used here from Flight.proto and FlightSql.proto.

    PyArrow exposes the Flight client but does not ship Flight SQL protobufs.
    Keeping these descriptors local avoids a protoc or generated-code dependency.
    """
    file = descriptor_pb2.FileDescriptorProto(name=f"{name}.proto", syntax="proto3")
    message = file.message_type.add(name=name)
    for number, field_name, field_type in fields:
        field = message.field.add(name=field_name, number=number, type=field_type, label=1)
        if field_name in optional_fields:
            field.proto3_optional = True
            field.oneof_index = len(message.oneof_decl)
            message.oneof_decl.add(name=f"_{field_name}")
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(name))


_STRING = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
_BYTES = descriptor_pb2.FieldDescriptorProto.TYPE_BYTES
_INT32 = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
Action = message_type("Action", [(1, "type", _STRING), (2, "body", _BYTES)])
Result = message_type("Result", [(1, "body", _BYTES)])
StatementQuery = message_type("CommandStatementQuery", [(1, "query", _STRING)])
PreparedQuery = message_type("CommandPreparedStatementQuery", [(1, "prepared_statement_handle", _BYTES)])
GetDbSchemas = message_type("CommandGetDbSchemas", [(1, "catalog", _STRING)], optional_fields=("catalog",))
GetTables = message_type("CommandGetTables", [(1, "catalog", _STRING)], optional_fields=("catalog",))
PrepareRequest = message_type("ActionCreatePreparedStatementRequest", [(1, "query", _STRING)])
PrepareResult = message_type(
    "ActionCreatePreparedStatementResult",
    [(1, "prepared_statement_handle", _BYTES), (2, "dataset_schema", _BYTES), (3, "parameter_schema", _BYTES)],
)
CloseResult = message_type("CloseSessionResult", [(1, "status", _INT32)])
Descriptor = message_type("FlightDescriptor", [(1, "type", _INT32), (2, "cmd", _BYTES)])


def pack(message):
    return Any(
        type_url=f"type.googleapis.com/arrow.flight.protocol.sql.{message.DESCRIPTOR.name}",
        value=message.SerializeToString(),
    ).SerializeToString()


def descriptor(message):
    return flight.FlightDescriptor.for_command(pack(message))


def action(client, name, body=b"", options=None):
    return [bytes(result.body) for result in client.do_action(flight.Action(name, body), options)]


def prepare(client, sql, options=None):
    result = action(client, "CreatePreparedStatement", pack(PrepareRequest(query=sql)), options)[0]
    return PrepareResult.FromString(Any.FromString(result).value).prepared_statement_handle


def query(client, sql, options=None):
    info = client.get_flight_info(descriptor(StatementQuery(query=sql)), options)
    return client.do_get(info.endpoints[0].ticket, options).read_all()


def rpc_action(channel, name, metadata=()):
    call = channel.unary_stream(
        "/arrow.flight.protocol.FlightService/DoAction",
        request_serializer=Action.SerializeToString,
        response_deserializer=Result.FromString,
    )
    return call(Action(type=name), metadata=metadata, timeout=5)


@pytest.mark.parametrize("command_type", [GetDbSchemas, GetTables])
@pytest.mark.parametrize("catalog", ["sail", "", "missing"])
def test_metadata_catalog_filter_skips_unrelated_providers(monkeypatch, command_type, catalog):
    monkeypatch.setenv(
        "SAIL_CATALOG__LIST",
        '[{type="memory", name="sail", initial_database=["default"]}, '
        '{type="iceberg-rest", name="offline", uri="http://127.0.0.1:1"}]',
    )
    monkeypatch.setenv("SAIL_CATALOG__DEFAULT_CATALOG", "sail")
    with local_flight_uri() as uri, flight.connect(uri) as client:
        query(client, "CREATE TEMP VIEW filtered_view AS SELECT 1 AS value")
        command = descriptor(command_type(catalog=catalog))
        schema = client.get_schema(command).schema
        info = client.get_flight_info(command)
        table = client.do_get(info.endpoints[0].ticket).read_all()
        assert table.schema == schema
        if catalog == "sail":
            assert table.column("catalog_name").to_pylist() == ["sail"]
            column = "db_schema_name" if command_type is GetDbSchemas else "table_name"
            assert table.column(column).to_pylist() == (
                ["default"] if command_type is GetDbSchemas else ["filtered_view"]
            )
        else:
            assert table.num_rows == 0


@pytest.mark.parametrize("prepared", [False, True])
def test_mutating_procedure_finishes_before_fetch(flight_uri, tmp_path, prepared):
    with flight.connect(flight_uri) as client:
        options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
        query(
            client, f"CREATE TABLE eager_procedure (value INT) USING iceberg LOCATION '{tmp_path.as_posix()}'", options
        )
        query(client, "INSERT INTO eager_procedure VALUES (1)", options)
        snapshot = query(client, "SELECT snapshot_id FROM eager_procedure.snapshots", options).column(0)[0].as_py()
        query(client, "INSERT INTO eager_procedure VALUES (2)", options)
        sql = f"CALL system.rollback_to_snapshot('eager_procedure', {snapshot})"
        command = descriptor(
            PreparedQuery(prepared_statement_handle=prepare(client, sql, options))
            if prepared
            else StatementQuery(query=sql)
        )
        schema = client.get_schema(command, options).schema
        assert query(client, "SELECT value FROM eager_procedure ORDER BY value", options).to_pylist() == [
            {"value": 1},
            {"value": 2},
        ]
        info = client.get_flight_info(command, options)
        assert query(client, "SELECT value FROM eager_procedure", options).to_pylist() == [{"value": 1}]
        result = client.do_get(info.endpoints[0].ticket, options).read_all()
        assert result.schema == schema
        assert result.column("current_snapshot_id").to_pylist() == [snapshot]
        assert query(client, "SELECT value FROM eager_procedure", options).to_pylist() == [{"value": 1}]


@pytest.mark.parametrize("name", ["GetSessionOptions", "CloseSession"])
def test_session_actions_require_an_existing_session(flight_uri, name):
    with grpc.insecure_channel(flight_uri.removeprefix("grpc://")) as channel:
        with pytest.raises(grpc.RpcError) as error:
            list(rpc_action(channel, name))
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        # A failed lookup must not implicitly create a session on this transport.
        with pytest.raises(grpc.RpcError) as error:
            list(rpc_action(channel, "GetSessionOptions"))
        assert error.value.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.parametrize("header", ["cookie", "authorization"])
def test_unknown_session_reference_is_not_found(flight_uri, header):
    value = "arrow_flight_session_id=unknown" if header == "cookie" else "Bearer unknown"
    with grpc.insecure_channel(flight_uri.removeprefix("grpc://")) as channel:
        for name in ("GetSessionOptions", "CloseSession", "SetSessionOptions"):
            with pytest.raises(grpc.RpcError) as error:
                list(rpc_action(channel, name, [(header, value)]))
            assert error.value.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.parametrize("use_cookie", [False, True])
def test_close_invalidates_session_and_handles(flight_uri, use_cookie):
    with flight.connect(flight_uri) as client:
        authorization = client.authenticate_basic_token("", "")
        session_id = authorization[1].decode().removeprefix("Bearer ")
        header = (b"cookie", f"arrow_flight_session_id={session_id}".encode()) if use_cookie else authorization
        options = flight.FlightCallOptions(headers=[header])
        handle = prepare(client, "SELECT 42", options)
        info = client.get_flight_info(descriptor(StatementQuery(query="SELECT 53")), options)
        result = action(client, "CloseSession", options=options)
        assert CloseResult.FromString(result[0]).status == 1
        with grpc.insecure_channel(flight_uri.removeprefix("grpc://")) as channel:
            metadata = [(header[0].decode(), header[1].decode())]
            for name in ("GetSessionOptions", "CloseSession", "SetSessionOptions"):
                with pytest.raises(grpc.RpcError) as error:
                    list(rpc_action(channel, name, metadata))
                assert error.value.code() == grpc.StatusCode.NOT_FOUND
        # The same connection can start a fresh session, but cannot reuse handles.
        new_options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
        with pytest.raises(pa.ArrowKeyError):
            client.get_flight_info(descriptor(PreparedQuery(prepared_statement_handle=handle)), new_options)
        with pytest.raises(pa.ArrowKeyError):
            client.do_get(info.endpoints[0].ticket, new_options).read_all()
        assert query(client, "SELECT 7 AS value", new_options).to_pylist() == [{"value": 7}]


def test_idle_expiry_invalidates_session_and_handles(monkeypatch):
    monkeypatch.setenv("SAIL_FLIGHT__SESSION_TIMEOUT_SECS", "1")
    server = FlightSqlServer("127.0.0.1", 0)
    server.start(background=True)
    try:
        host, port = server.listening_address
        uri = f"grpc://{host}:{port}"
        with flight.connect(uri) as client:
            header = client.authenticate_basic_token("", "")
            options = flight.FlightCallOptions(headers=[header])
            handle = prepare(client, "SELECT 42", options)
            info = client.get_flight_info(descriptor(StatementQuery(query="SELECT 53")), options)
            # Do not poll the session itself: successful lookups renew its lease.
            time.sleep(1.5)
            with grpc.insecure_channel(f"{host}:{port}") as channel:
                metadata = [(header[0].decode(), header[1].decode())]
                for name in ("GetSessionOptions", "CloseSession", "SetSessionOptions"):
                    with pytest.raises(grpc.RpcError) as error:
                        list(rpc_action(channel, name, metadata))
                    assert error.value.code() == grpc.StatusCode.NOT_FOUND
            new_options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
            with pytest.raises(pa.ArrowKeyError):
                client.get_flight_info(descriptor(PreparedQuery(prepared_statement_handle=handle)), new_options)
            with pytest.raises(pa.ArrowKeyError):
                client.do_get(info.endpoints[0].ticket, new_options).read_all()
    finally:
        server.stop()


@pytest.mark.parametrize("values", [["1", "bad"], ["1", "2"]])
def test_command_query_batch_is_rejected_before_side_effects(flight_uri, tmp_path, values):
    with flight.connect(flight_uri) as client:
        options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
        query(client, f"CREATE TABLE batch_query (value INT) USING parquet LOCATION '{tmp_path.as_posix()}'", options)
        handle = prepare(client, "INSERT INTO batch_query VALUES (CAST(? AS INT))", options)
        batch = pa.record_batch([pa.array(values)], names=["value"])
        writer, reader = client.do_put(
            descriptor(PreparedQuery(prepared_statement_handle=handle)), batch.schema, options
        )
        with writer:
            writer.write_batch(batch)
            writer.done_writing()
            reader.read()
        with pytest.raises(pa.ArrowNotImplementedError, match="multiple parameter sets"):
            client.get_flight_info(descriptor(PreparedQuery(prepared_statement_handle=handle)), options)
        assert query(client, "SELECT count(*) AS count FROM batch_query", options).to_pylist() == [{"count": 0}]
        # Rejecting a batch leaves the handle usable for a single parameter set.
        single = batch.slice(0, 1)
        writer, reader = client.do_put(
            descriptor(PreparedQuery(prepared_statement_handle=handle)), single.schema, options
        )
        with writer:
            writer.write_batch(single)
            writer.done_writing()
            reader.read()
        info = client.get_flight_info(descriptor(PreparedQuery(prepared_statement_handle=handle)), options)
        client.do_get(info.endpoints[0].ticket, options).read_all()
        assert query(client, "SELECT value FROM batch_query", options).to_pylist() == [{"value": 1}]


@pytest.mark.parametrize("binding", ["rows", "empty_batch", "schema_only"])
def test_bound_schema_and_result_are_consistent(flight_uri, binding):
    with flight.connect(flight_uri) as client:
        options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
        handle = prepare(client, "SELECT ? AS value", options)
        values = [11, 22] if binding == "rows" else []
        batch = pa.record_batch([pa.array(values, type=pa.int64())], names=["value"])
        command = descriptor(PreparedQuery(prepared_statement_handle=handle))
        writer, reader = client.do_put(command, batch.schema, options)
        with writer:
            if binding != "schema_only":
                writer.write_batch(batch)
            writer.done_writing()
            reader.read()
        schema = client.get_schema(command, options).schema
        info = client.get_flight_info(command, options)
        table = client.do_get(info.endpoints[0].ticket, options).read_all()
        assert schema == info.schema == table.schema
        assert schema.field("value").type == pa.int64()
        assert sorted(table.column("value").to_pylist()) == values


@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        "EXPLAIN INSERT INTO explain_analyze_target VALUES (1)",
        "EXPLAIN ANALYZE INSERT INTO explain_analyze_target VALUES (1)",
        "EXPLAIN EXPLAIN ANALYZE INSERT INTO explain_analyze_target VALUES (1)",
    ],
)
def test_explain_preparation_and_schema_do_not_execute(flight_uri, tmp_path, prepared, sql):
    with flight.connect(flight_uri) as client:
        options = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "")])
        query(
            client,
            f"CREATE TABLE explain_analyze_target (value INT) USING parquet LOCATION '{tmp_path.as_posix()}'",
            options,
        )
        command = descriptor(
            PreparedQuery(prepared_statement_handle=prepare(client, sql, options))
            if prepared
            else StatementQuery(query=sql)
        )
        assert query(client, "SELECT count(*) AS n FROM explain_analyze_target", options).to_pylist() == [{"n": 0}]
        schema = client.get_schema(command, options).schema
        assert schema.names == ["plan"]
        assert query(client, "SELECT count(*) AS n FROM explain_analyze_target", options).to_pylist() == [{"n": 0}]
        for count in (1, 2):
            expected_count = count if "ANALYZE" in sql else 0
            info = client.get_flight_info(command, options)
            assert query(client, "SELECT count(*) AS n FROM explain_analyze_target", options).to_pylist() == [
                {"n": expected_count}
            ]
            table = client.do_get(info.endpoints[0].ticket, options).read_all()
            assert table.schema == schema == info.schema
            assert table.num_rows == 1
            assert "Physical Plan" in table.column("plan")[0].as_py()
            assert query(client, "SELECT count(*) AS n FROM explain_analyze_target", options).to_pylist() == [
                {"n": expected_count}
            ]


def http2_query(host, port, sql, source_port=0):
    """Issue one unary RPC with a chosen source port, without cookies.

    Standard Flight clients cannot choose a source port. This minimal HTTP/2
    exchange lets the regression reuse a peer address deterministically without
    depending on OS ephemeral-port allocation or a third-party HTTP/2 library.
    """

    def frame(kind, flags, stream, body=b""):
        return len(body).to_bytes(3, "big") + bytes([kind, flags]) + stream.to_bytes(4, "big") + body

    def header(name, value):
        name, value = name.encode(), value.encode()
        # All strings below fit HPACK's seven-bit length prefix. Literal fields
        # without indexing keep the exchange independent of a dynamic table.
        assert len(name) < 127  # noqa: PLR2004
        assert len(value) < 127  # noqa: PLR2004
        return b"\0" + bytes([len(name)]) + name + bytes([len(value)]) + value

    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Close with RST so the test can immediately reuse its source port.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        sock.settimeout(5)
        sock.bind((host, source_port))
        source_port = sock.getsockname()[1]
        sock.connect((host, port))
        headers = [
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/arrow.flight.protocol.FlightService/GetFlightInfo"),
            (":authority", f"{host}:{port}"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
        ]
        encoded_headers = b"".join(header(name, value) for name, value in headers)
        message = Descriptor(type=2, cmd=pack(StatementQuery(query=sql))).SerializeToString()
        sock.sendall(
            b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
            + frame(4, 0, 0)  # SETTINGS
            + frame(1, 4, 1, encoded_headers)  # HEADERS, END_HEADERS
            + frame(0, 1, 1, b"\0" + len(message).to_bytes(4, "big") + message)  # DATA, END_STREAM
        )

        def read(size):
            data = bytearray()
            while len(data) < size:
                chunk = sock.recv(size - len(data))
                assert chunk, "unexpected end of HTTP/2 connection"
                data.extend(chunk)
            return data

        result = bytearray()
        while True:
            frame_header = read(9)
            size = int.from_bytes(frame_header[:3], "big")
            kind, flags = frame_header[3:5]
            stream = int.from_bytes(frame_header[5:], "big")
            payload = read(size)
            if kind == 4 and not flags & 1:  # noqa: PLR2004  # SETTINGS acknowledgement
                sock.sendall(frame(4, 1, 0))
            if kind == 0 and stream == 1:
                result.extend(payload)
            if kind in (0, 1) and stream == 1 and flags & 1:
                return source_port, bool(result)


def test_new_transport_cannot_inherit_session_from_reused_peer(flight_uri):
    host, port = flight_uri.removeprefix("grpc://").rsplit(":", 1)
    port = int(port)
    source_port, success = http2_query(host, port, "CREATE TEMP VIEW transport_secret AS SELECT 1")
    assert success
    reused_port, success = http2_query(host, port, "SELECT * FROM transport_secret", source_port)
    assert reused_port == source_port
    assert not success
